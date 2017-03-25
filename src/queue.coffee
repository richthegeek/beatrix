_ = require 'lodash'
Rabbit = require('amqplib')
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
      id: 0,
      durable: true,
      autoDelete: false
    }

    @connected = false
    @stack = @connection.queues?[@name]?.stack ? []
    @lastPublish = 0
    @lastComplete = 0
    @pending = 0

  connect: (cb) ->
    {name, type, concurrency} = @options
    
    @channel?.close?()
    
    return @connection.connection.createChannel()
      .then (@channel) =>
        @channel.assertQueue type, _.omit @options, ['name', 'type', 'concurrency']
      .then =>
        @channel.bindQueue(type, @connection.exchange.name, type)
      .then =>
        @channel.recover()
      .then =>
        @channel.prefetch(concurrency)
      .then =>
        @channel.consume type, @processJob.bind(@)
      .then =>
        
        offset = 0
        timer = setInterval (=>
          @channel.checkQueue(type).then (ok) =>
            # stats the number of messages and consumers every 30 seconds
            offset++
            if offset is 30
              offset = 0
              @stats 'increment', type, 'consumers', ok.consumerCount
              @stats 'increment', type, 'messages', ok.messageCount

            # manually jog the queue every second, perhaps
            lag = Date.now() - @lastComplete
            timeout = @options.timeout or 60 * 1000
            if @pending is 0 and ok.messageCount > 0
              @channel.get(type).then (message) =>
                if message 
                  @log.info {type}, 'Manually retrieved message, consuming'
                  @processJob message
                else
                  @log.info {type}, 'No message retrieved despite count=' + ok.messageCount + '. Investigate.'

            if @pending > 0
              @log.error {type}, 'PENDINGCOUNT=' + @pending
              if lag > timeout
                @log.error {type}, 'CHANNELRECOVER'
                @channel.recover()
        ), 1000

        # reconnect on close
        @channel.on 'close', =>
          @log.error {type}, 'Channel closed. Reconnecting.'
          clearTimeout(timer)
          @connected = false
          @connect()

        # Log on error
        @channel.on 'error', (err) =>
          @log.error {type}, 'Channel errored.', err

        # Log on returned/unroutable message
        @channel.on 'returned', (msg) =>
          @log.error {type}, 'Unroutable message returned!'

        # after connection, publish any held messages
        @connected = true
        setTimeout (=>
          for item in @stack
            @[item.type](item.body, item.options, item.cb)
        ), 100

        cb? null, @

      .catch (err) =>
        @log.error {type}, "Could not connect queue", err.stack
        setTimeout (=> @connect(cb)), 1000

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
    @pending++
    job = new Job @options.type, @
    job.process message

  jobSuccess: (message) ->
    @pending = Math.max(0, @pending - 1)
    @lastComplete = Date.now()
    @lastSuccess = Date.now()
    @stats 'increment', @type, 'ok', 1
    @options.jobSuccess? message
    @connection.jobSuccess? message
  
  jobPartFailure: (message) ->
    @pending = Math.max(0, @pending - 1)
    @lastComplete = Date.now()
    @stats 'increment', @type, 'part_fail', 1
    @options.jobPartFailure? message
    @connection.jobPartFailure? message

  jobFullFailure: (message) ->
    @pending = Math.max(0, @pending - 1)
    @lastComplete = Date.now()
    @stats 'increment', @type, 'full_fail', 1
    @options.jobFullFailure? message
    @connection.jobFullFailure? message