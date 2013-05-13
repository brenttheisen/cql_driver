module CqlDriver
  def initialize(cluster, hosts)
    @manager = Manager.new(cluster, hosts)
  end

  def execute(query)
    query = SimpleStatement.new(query) if query.instance_of?(String)

    execute_async(query).get_uninterruptibly
  end

  def execute_async(query)
    query = SimpleStatement.new(query) if query.instance_of?(String)

    if query.instance_of?(Statement)
      @manager.executeQuery(QueryMessage.new(query.query_string, ConsistencyLevel.to_cassandra_cl(query.consistency_level)), query)
    else
      raise "Query is not a BoundStatement" unless query.instance_of?(BoundStatement)

      @manager.execute_query(ExecuteMessage.new(query.statement.id, query.values, ConsistencyLevel.to_cassandra_cl(query.consistency_level)), query)
    end
  end

  def prepare(query)
    future = Connection::Future.new(PrepareMessage.net(query))
    @manager.execute(future, Query::DEFAULT)
    to_prepared_statement(query, future)
  end

  def shutdown(timeout=nil, unit=nil)
    @manager.shutdown(timeout, unit)
  end

  def cluster
    @manager.cluster
  end

  def to_prepared_statement(query, future)
    response = nil # TODO This is gotten from a Google Commons collection class
    case response.type
    when RESULT
      case response.kind
      when PREPARED
        stmt = PreparedStatement.from_message(response, @manager.cluster.metadata, query, @manager.pool_state.keyspace)
        @manager.cluster.manager.prepare(response.statement_id, stmt, future.address)
      else
        raise DriverInternalError.new("#{statement.kind} response received when prepared statement was expected")
      end
    when ERROR
      ResultSetFuture.extractCause(ResultSetFuture.convert_exception(response.error))
    else
      raise DriverInternalError(String.format("#{response.type} response received when prepared statement was expected"))
    end
  end

  class Manager
    def initialize(cluster, hosts)
      @cluster = cluster
      @pools = {}
      @load_balancer = @cluster.manager.configuration.policies.load_balancing_priority
      @pools_state = HostConnectionPool::PoolState.new

      hosts.each { |h| add_or_renew_pool(host) }
      @is_shutdown = false
    end
  end
end

