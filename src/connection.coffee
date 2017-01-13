_ = require('lodash')
Rabbot = require('rabbot')
Queue = require('./queue')

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
        message.ack()
    }

    # for children to link to
    @exchange = @options.exchange
    @log = @options.log
    @stats = @options.stats
    @onUnhandled = @options.onUnhandled
    @queues = {}

    _.each options.queues, (opts, name) =>
      @queues[name] = {
        publish: (body, options, cb) => @queues[name].stack.push {type: 'publish', body, options, cb}
        request: (body, options, cb) => @queues[name].stack.push {type: 'publish', body, options, cb}
        stack: []
      }

  connect: (cb) ->
    Rabbot.onUnhandled @onUnhandled.bind @
    return Rabbot.configure({
      connection: @options.connection,
      exchanges: [@options.exchange]
    }).then () =>
      @log.error 'RabbitMQ Connected!'
      cb null, @
      return @
    .catch (err) =>
      @log.error 'Could not connect', err
      cb err, @
      return @

  createQueue: (name, options, cb) ->
    @log.info 'Create Queue: ' + name
    @queues[name] = queue = new Queue name, options, @
    return @queues[name].connect (err) =>
      if err
        @log.error 'Could not create queue', err
        return cb? err

      cb? null, queue

  partFailure: (message) ->
    @options.partFailure? message

  fullFailure: (message) ->
    @options.fullFailure? message
