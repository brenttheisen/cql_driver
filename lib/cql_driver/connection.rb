module CqlDriver
  class Connection
    MAX_STREAM_PER_CONNECTION = 128
    CQL_VERSION = '3.0.0'

    def initialize(name, address, factory)
      @name = name
      @address = address
      @factory = factory
    end

    # TODO Finish this considering we don't have access to Netty or Google Commons Java libs
  end
end
