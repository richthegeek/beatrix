_ = require 'lodash'
Rabbot = require('rabbot')
Job = require('./job')

module.exports = class Queue

  constructor: (@name, options, @connection) ->
    @log = @connection.log
    @stats = @connection.stats
    @options = _.defaultsDeep options, {
      name: @name
      type: @connection.exchange.name + '.' + @name
      timeout: null
      concurrency: 1
      id: 0
    }

    @handle = false
    @connected = false
    @stack = @connection.queues?[@name]?.stack ? []
    @lastPublish = 0
    @lastComplete = 0

  createHandle: ->
    {name, type, concurrency, noBatch} = @options
    
    @handle?.remove?()
    @handle = Rabbot.handle({
      queue: type,
      type: type,
      autoNack: true,
      handler: @processJob,
      context: @
    })
    
    return Rabbot.addQueue(type, {
      subscribe: true,
      autoDelete: false,
      durable: true,
      noBatch: (noBatch isnt false),
      limit: concurrency
    }).then =>
      Rabbot.bindQueue @connection.exchange.name, type, [type] 

  connect: (cb) ->
    {name, type, concurrency} = @options
    @createHandle().then =>
      @log.info {type, concurrency}, "RabbitMQ Queue Started"

      # experimental: log the difference between the last publish on this queue and the last completion
      setInterval (=>
        if @lastPublish
          # rebind the handler if the delay between publishing and completing
          # is double the timeout for completing any job in this queue
          lag = @lastPublish - @lastComplete
          timeout = (@options.timeout | 0) or 60 * 1000
          @stats 'timing', type, 'lag', Math.abs lag
      ), 10 * 1000

      # enqueue any jobs that were added while not connected
      # short delay required to function correctly
      @connected = true
      setTimeout (=>
        for item in @stack
          @[item.type](item.body, item.options, item.cb)
      ), 100

      return cb? null, @
    .catch (err) =>
      @log.error {type}, "Could not initialise queue", err
      @connected = false
      cb "Could not initialise queue: #{err.stack}", @

  publish: (body, options, cb) ->
    unless @connected
      return @stack.push {type: 'publish', body, options, cb}

    job = new Job @options.type, @
    job.publish body, options, cb

  request: (body, options, cb) ->
    unless @connected
      return @stack.push {type: 'request', body, options, cb}

    job = new Job @options.type, @
    job.request body, options, cb

  processJob: (message) ->
    job = new Job @options.type, @
    job.process message
  
  partFailure: (message) ->
    @options.partFailure? message
    @connection.partFailure? message

  fullFailure: (message) ->
    @options.fullFailure? message
    @connection.fullFailure? message    