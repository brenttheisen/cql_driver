module CqlDriver
  class Cluster
    def initialize(contact_points, configuration)
      @manager = Manager.new(self, contact_points, configuration)
      @manager.init()
    end

    def connect(keyspace=nil)
      session = @manager.new_session
      session.manager.keyspace = keyspace
      session
    end

    def metadata
      @manager.metadata
    end

    def configuration
      @manager.configuration
    end

    def metrics
      @manager.metrics
    end

    def shutdown(timeout=nil, unit=nil)
      @manager.shutdown timeout, unit
    end

    class Manager
      attr_reader :cluster

      def initialize(cluster, contact_points, configuration)
        @cluster = cluster
        @configuration = configuration
        @metadata = Metadata.new(self)
        @contact_points = contact_points
        @connection_factory = Connection::Factory.new(self, @configuration.auth_info_provider)

        @control_connection = ControlConnection.new(self)

        @metrics = Metrics.new(self) if @configuration.metrics_options
        @configuration.register(self)

        @sessions = []
        @prepared_queries = {}
      end

      def init
        @contact_points.each { add_host(address, false) }
        @configuration.policies.load_balancing_policy.init(@cluster, @metadata.all_hosts)

        @control_connection.connect
      end

      def new_session
        session = Session.new(@cluster, metadata.all_hosts)
        @sessions << session
        session
      end

      def shutdown(timeout, unit)
        return true if @is_shutdown

        start = Time.now.to_i
        success = true
        timeout_diff = timeout - time_since(start, unit)
        success &= @control_connection.shutdown(timeout_diff, unit)

        metrics.shutdown if metrics

        @reconnection_executor.await_termination(timeout_diff, unit) &&
          @scheduled_tasks_executor.await_termination(timeout_diff, unit) &&
          @execute.await_termination(timeout_diff, unit)
      end

      def on_up(host)
        scheduled_attempt = host.reconnection_attempt.get_and_set
        scheduled_attempt.cancel(false) if scheduled_attempt

        prepare_all_queries(host)

        @control_connection.on_up(host)
        @sessions.each { |s| s.manager.on_up(host) }
      end

      def on_down(host)
        @control_connection.on_down(host)
        @sessions.each { |s| s.manager.on_down(host) }

        ReconnectionHandler.new(@reconnection_executor, @configuration.policies.reconnection_policy.new_schedule, host.reconnection_attempt).start
      end

      def on_add(host)
        prepare_all_queries(host)

        @control_connection.on_add(host)
        @sessions.each { |s| s.manager.on_add(host) }
      end

      def on_remove(host)
        @control_connection.on_remove(host)
        @sessions.each { |s| s.manager.on_remove(host) }
      end

      def add_host(address, signal)
        new_host = metadata.add(address)
        on_add(new_host) if new_host && @signal
        new_host
      end

      def remove_host(host)
        return unless host

        on_remove(host) if metadata.remove(host)
      end

      def ensure_pools_sizing
        @sessions.each do |s|
          s.manager.pools.values.each { |p| p.ensure_core_connections }
        end
      end

      def prepare(digest, stmt, to_exclude)
        @prepared_queries[digest] = stmt
        @sessions.each { |s| s.manager.prepare(stmt.query_string, to_exclude) }
      end

      def prepare_all_queries(host)
        return if @prepared_queries.empty?

        connection = @connection_factory.open(host)
        begin
          ControlConnection.wait_for_schema_agreement(connection, @metadata) rescue nil

          per_keyspace = @prepared_queries.values.inject do |memo, ps|
            keyspace = ps.query_keyspace || ''
            memo[keyspace] ||= []
            memo[keyspace] << ps.query_string unless memo[keyspace].include?(ps.query_string)
            memo
          end

          futures = per_keyspace[keyspace].collect { |query| connection.write(PreparedMessage.new(query) }

          # TODO This is messed up (noop)? Need to look at the futures stuff in Googel Commons
          futures.each { |f| f.get }
        ensure
          connection.close(0, TimeUnit::MILLISECONDS)
        end
      end

      def submit_schema_refresh(keyspace, table)
        @execute.submit do
          @control_connection.refresh_schema(keyspace, table)
        end
      end

      def refresh_schema(connection, future, rs, keyspace, table)
        @executor.submit do
          begin
            ControlConnection.wait_for_schema_agreement(connection, @metadata)
            ControlConnection.refresh_schema(connection, table, @manager)
          rescue
            submit_schema_refresh(keyspace, table)
          ensure
            future.set(rs)
          end
        end
      end

      def handle(response)
        return if !response.instance_of?(EventMessage)

        event = response.event
        @scheduled_tasks_executor.schedule(delay_for_event(event), TimeUnit::SECONDS) do
          case event.type
          when Event::TOPOLOGY_CHANGE:
            case event.change
            when TopologyChange::NEW_NODE:
              add_host(event.node.address, true)
            when TopologyChange::REMOVED_NODE:
              remove_host(@metadata.host(event.node.address)
            when TopologyChange::MOVED_NODE:
              @control_connection.refresh_node_list_and_token_map
            end
          when Event::STATUS_CHANGE:
            when event.status
            case UP:
              host_up = @metadata.host(event.node.address)
              host_up ? host_up.monitor.set_down : add_host(event.node.address, true)
            case DOWN:
              host_down = @metadata.host(event.node.address)
              host_down.monitor.set_down if host_down
            end
          when Event::SCHEMA_CHANGE:
            case event.change
            when CREATED, DROPPED:
              submit_schema_refresh(event.table.empty? ? null : event.keyspace, null)
            when UPDATED:
              submit_schema_refresh(event.keyspace, event.table.empty? ? nil : event.table)
            end
        end
      end

      def delay_for_event(event)
        delay = case event.type
        when TOPOLOGY_CHANGE then 1
        when STATUS_CHANGE:
          1 if event.status == Event::StatusChange::Status::UP
        end

        delay || 0
      end

      class ReconnectionHandler < AbstractReconnectionHandler
        def try_reconnect
          @connection_factory.open(@host)
        end

        def on_reconnection(connection)
          @host.monitor.set_up
        end
      end
    end
  end
end
