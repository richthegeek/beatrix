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
        publish: => @queues[name].stack.push(['publish', arguments])
        request: => @queues[name].stack.push(['request', arguments])
        stack: []
      }

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
    stack = @queues[name]?.stack

    @queues[name] = queue = new Queue name, options, @
    return @queues[name].connect (err) ->
      if err
        return cb? err

      for item in stack
        queue[item[0]].apply queue, item[1]

      cb? null, queue

  partFailure: (message) ->
    @options.partFailure? message

  fullFailure: (message) ->
    @options.fullFailure? message
