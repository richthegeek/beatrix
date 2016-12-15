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
        arguments: {}
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

    # unchangeable
    @options.exchange.type = 'x-delayed-message'
    @options.exchange.arguments['x-delayed-type'] = 'direct'

  connect: (cb) ->
    Rabbot.onUnhandled @onUnhandled.bind @
    return Rabbot.configure({
      connection: @options.connection,
      exchanges: [@options.exchange]
    }).then () =>
      cb null, @
      return @
    .catch (err) =>
      cb err, @
      return @

  createQueue: (name, options, cb) ->
    @queues[name] = new Queue name, options, @
    return @queues[name].connect cb

  partFailure: (message) ->
    @options.partFailure? message

  fullFailure: (message) ->
    @options.fullFailure? message
