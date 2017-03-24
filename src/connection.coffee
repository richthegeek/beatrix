_ = require('lodash')
os = require('os')
Rabbit = require('amqplib')
Queue = require('./queue')
Timeout = require('./callbackTimeout')

module.exports = class Connection

  constructor: (options) ->
    @options = _.defaultsDeep options, {
      log: new console.Console(process.stdout, process.stderr)
      stats: (type, queue, stat, value) -> null
      connection: {
        uri: 'amqp://guest:guest@localhost/'
      },
      exchange: {
        name: 'beatrix'
        autoDelete: false
        durable: true
        type: 'x-delayed-message'
        arguments: {
          'x-delayed-type': 'direct'
        }
      }
      onUnhandled: (message) ->
        @log.error 'Unhandled message', message
        message.nack()
    }

    # for children to link to
    @exchange = @options.exchange
    @log = @options.log
    @stats = @options.stats
    @onUnhandled = @options.onUnhandled
    @queues = {}
    @pendingResponses = {}

    _.each options.queues, (opts, name) =>
      @queues[name] = {
        publish: (body, options, cb) => @queues[name].stack.push {type: 'publish', body, options, cb}
        request: (body, options, cb) => @queues[name].stack.push {type: 'publish', body, options, cb}
        stack: []
      }

  reply: (id, err, res) ->


  connect: (cb) ->
    return Rabbit.connect(@options.connection.uri, {heartbeat: 5})
      .then (@connection) =>
        return @connection.createChannel()
      .then (@channel) =>
        @channel.assertExchange(@exchange.name, @exchange.type, _.omit(@exchange, 'name', 'type'))
      .then =>
        @createResponseQueue()
        @log.info 'RabbitMQ Connected!'
        cb null, @
        return @
      .catch (err) =>
        @log.error 'Could not connect', err
        cb err, @
        return @

  createQueue: (name, options, cb) ->
    @log.info 'Create Queue: ' + name
    @queues[name] = new Queue name, options, @
    return @queues[name].connect (err) =>
      if err
        @log.error 'Could not create queue', err
        return cb? err

      cb? null, @queues[name]

  partFailure: (message) ->
    @options.partFailure? message

  fullFailure: (message) ->
    @options.fullFailure? message

  createResponseQueue: ->
    @responseQueue = [os.hostname(), process.title, process.pid, 'response', 'queue'].join('.')
    
    queue = new Queue @responseQueue, {
      name: @responseQueue,
      type: @responseQueue,
      autoDelete: true,
      exclusive: true,
      messageTtl: 600 * 1000, # clear messages out after 10 minutes
    }, @
    
    queue.processJob = (message) =>
      queue.channel.ack message
      
      body = JSON.parse message.content
      
      cb = @pendingResponses[message.properties.correlationId]
      cb? body.err, body.result

    queue.connect()

  addRequestCallback: (options, cb) ->
    fn = _.once (err, res) =>
      delete @pendingResponses[options.correlationId]
      cb err, res

    @pendingResponses[options.correlationId] = Timeout options.replyTimeout, fn
