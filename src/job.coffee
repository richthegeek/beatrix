_ = require 'lodash'
Rabbot = require('rabbot')
Timeout = require('callback-timeout')

module.exports = class Job

  constructor: (@type, @queue) ->
    @connection = @queue.connection
    @stats = @connection.stats
    @log = @connection.log

  mergePublishOptions: (body, options) ->
    @queue.lastPublish = Date.now()

    options = _.defaultsDeep {}, options, @queue.options, {
      type: @type
      publishedAt: Date.now(),
      headers: {},
      attempts: 0
      maxAttempts: 1,
      initialDelay: 0,
      delay: 1000,
      id: @queue.options.id + 1,
      body: body
    }

    unless options.messageId?
      options.messageId = @queue.options.name + '.' + (++@queue.options.id)

    options.routingKey = options.type = @type
    delete options.timeout # this is an option for Rabbot, delete it to prevent issues

    # copy things over to the headers
    _.defaults options.headers, _.pick options, ['attempts', 'maxAttempts', 'delay', 'publishedAt']

    # set the delay
    if options.headers.attempts > 0 and options.headers.delay
      options.headers['x-delay'] = options.headers.delay * Math.pow(2, options.headers.attempts - 1)
    else if options.initialDelay
      options.headers['x-delay'] = options.initialDelay

    return _.pick options, ['headers', 'messageId', 'routingKey', 'type', 'body']

  hasAttemptsRemaining: ({headers}) ->
    return headers.attempts < headers.maxAttempts

  publish: (body, options, cb) ->
    options = @mergePublishOptions body, options

    unless @hasAttemptsRemaining options
      @log.info {type: @type}, "Rejecting publish due to too many attempts: #{options.headers.attempts} >= #{options.headers.maxAttempts}"
      return false

    @log.info {type: @type, id: options.messageId}, 'Publishing job to queue', options
    return Rabbot.publish(@connection.exchange.name, options).then((res) -> cb? null, res).catch(cb)

  request: (body, options, cb) ->
    options = @mergePublishOptions body, options
    options.replyTimeout ?= 5000
    options.headers.reply = true

    unless @hasAttemptsRemaining options
      @log.info {type: @type}, "Rejecting publish due to too many attempts: #{options.headers.attempts} >= #{options.headers.maxAttempts}"
      return false

    @log.info {type: @type, id: options.messageId}, 'Requesting job in queue', options
    Rabbot.request(@connection.exchange.name, options).then((res) ->
      try res.ack()
      cb? null, res
    ).catch(cb)

  partFailure: (message) ->
    @queue.partFailure? message

  fullFailure: (message) ->
    @queue.fullFailure? message

  process: (message) ->
    headers = message.properties.headers
    headers.attempts += 1
    headers.startedAt = Date.now()

    @stats 'timing', @type, 'startDelay', Date.now() - message.properties.timestamp

    message.attempt = headers.attempts
    message.firstAttempt = message.attempt is 1
    message.lastAttempt = (headers.attempts >= headers.maxAttempts)
    message.retry = (val = true) -> message.shouldRetry = val
    
    # message.reply = _.attempt.bind(_, message.reply.bind(message))
    # message.ack = _.attempt.bind(_, message.ack.bind(message))
    message.finish = =>
      if headers.reply
        @log.info 'REPLYING', arguments
        message.reply arguments...
      else
        @log.info 'ACKING', arguments
        message.ack()

    @log.info @processLogMeta(message, {timeout: @queue.options.timeout}), 'Starting'
    try
      @queue.options.process message, Timeout(
        @processCallback.bind(@, message),
        @queue.options.timeout
      )
    catch err
      @processCallback message, err, err

  processLogMeta: (message, extra) =>
     return _.extend extra, {
      type: @type,
      id: message.properties.messageId,
      attempt: message.attempt,
      delaySinceStarted: Date.now() - message.properties.headers.startedAt,
      delaySincePublished: Date.now() - message.properties.headers.publishedAt
    }

  processCallback: (message, err, result) =>
    try
      headers = message.properties.headers

      @stats 'timing', @type, 'e2e', Date.now() - headers.publishedAt
      @stats 'timing', @type, 'run', Date.now() - headers.startedAt

      if err and result?.retry isnt false and message.shouldRetry isnt false and not message.lastAttempt
        message.finish({ok: false, final: false, result: err})
        @stats 'increment', @type, 'part_fail', 1
        @partFailure? message
        @queue.publish message.body, message.properties
        @log.error @processLogMeta(message), 'Failed and retrying', err
        return false
      
      if err
        message.finish({ok: false, final: true, result: err})
        @stats 'increment', @type, 'full_fail', 1
        @fullFailure? message
        @log.error @processLogMeta(message, {retry: result?.retry, lastAttempt: message.lastAttempt}), "Failed completely", err
        return false 

      message.finish({ok: true, final: true, result: result})

      @queue.lastComplete = Date.now()
      @stats 'increment', @type, 'ok', 1
      @log.info @processLogMeta(message), 'Completed without error', result
      return true

    catch err
      message.finish({ok: false, final: false, result: err})
      @log.error 'processCallback error', err
