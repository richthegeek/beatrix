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
      timeout: 60 * 1000
      concurrency: 1
      id: 0
    }

    @lastPublish = 0
    @lastComplete = 0

  connect: (cb) ->
    {name, type, concurrency} = @options

    console.log name, type, @connection.exchange.name

    Rabbot.handle({
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
      limit: concurrency
    }).then =>
      Rabbot.bindQueue @connection.exchange.name, type, [type]
      @log.info {type, concurrency}, "RabbitMQ Queue Started"

      # experimental: log the difference between the last publish on this queue and the last completion
      setInterval (=>
        if @lastPublish
          @stats 'timing', type, 'lag', Math.abs @lastPublish - @lastComplete
          @lastPublish = 0
      ), 60 * 1000

      return cb null, @
    .catch (err) ->
      cb "Could not initialise queue: #{err.stack}", @

  createJob: (body, options) ->
    return new Job @options.type, body, options, @

  publish: (body, options, cb) ->
    job = new Job @options.type, @
    job.publish body, options, cb

  request: (body, options, cb) ->
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