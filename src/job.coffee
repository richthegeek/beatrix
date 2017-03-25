_ = require 'lodash'
Rabbit = require('amqplib')
Timeout = require('./callbackTimeout')

module.exports = class Job

  constructor: (@type, @queue) ->
    @connection = @queue.connection
    @stats = @connection.stats
    @log = @connection.log
    @channel = @queue.channel

  mergePublishOptions: (options) ->
    @queue.lastPublish = Date.now()

    options = _.defaultsDeep {}, options, @queue.options, {
      type: @type
      contentType: 'application/json'
      publishedAt: Date.now(),
      headers: {},
      attempts: 0
      maxAttempts: 1,
      initialDelay: 0,
      delay: 1000,
    }

    unless options.messageId?
      options.messageId = @queue.options.name + '.' + (++@queue.options.id)

    options.timestamp = Date.now()

    options.routingKey = options.type = @type
    delete options.timeout # this is an option for Rabbot, delete it to prevent issues

    # copy things over to the headers
    _.defaults options.headers, _.pick options, ['attempts', 'maxAttempts', 'delay']

    # set the delay
    if options.headers.attempts > 0 and options.headers.delay
      options.headers['x-delay'] = options.headers.delay * Math.pow(2, options.headers.attempts - 1)
    else if options.initialDelay
      options.headers['x-delay'] = options.initialDelay

    return options

  publish: (body, options, cb) ->
    options = @mergePublishOptions options

    unless options.headers.attempts < options.headers.maxAttempts
      @log.info {type: @type}, "Rejecting publish due to too many attempts: #{options.headers.attempts} >= #{options.headers.maxAttempts}"
      return false

    @log.info {type: @type, id: options.messageId, request: options.replyTo?}, 'Publishing job to queue', body

    body = new Buffer JSON.stringify body
    result = @channel.publish(@connection.exchange.name, @type, body, options)

    if result
      return cb? null, 'OK'
    else
      return cb? 'Queue full.'

  request: (body, options, cb) ->
    options = _.defaults {}, options, {
      replyTimeout: 5000,
      replyTo: @connection.responseQueue,
      correlationId: Date.now().toString()
    }

    @connection.addRequestCallback options, cb

    return @publish body, options

  jobPartFailure: (message) ->
    @queue.jobPartFailure? message

  jobFullFailure: (message) ->
    @queue.jobFullFailure? message

  jobSuccess: (message) ->
    @queue.jobSuccess? message

  process: (message) ->
    props = message.properties
    headers = props.headers
    headers.attempts += 1
    headers.startedAt = Date.now()

    @stats 'timing', @type, 'startDelay', Date.now() - props.timestamp

    message.ack = _.once => try @channel.ack message
    message.nack = _.once => try @channel.nack message
    message.body = JSON.parse message.content

    message.attempt = headers.attempts
    message.firstAttempt = message.attempt is 1
    message.lastAttempt = (headers.attempts >= headers.maxAttempts)

    message.finish = (err, result, final) =>
      message.ack()
      
      body = {err, result, final}
      if props.correlationId and props.replyTo and (final or not err)
        @log.info @processLogMeta(message), 'Replying', body
        body = new Buffer JSON.stringify body
        @channel.sendToQueue props.replyTo, body, {correlationId: props.correlationId}
      else
        @log.info @processLogMeta(message), 'Acking', body

    @log.info @processLogMeta(message, {timeout: @queue.options.timeout}), 'Starting'
    callback = Timeout @queue.options.timeout, @processCallback.bind(@, message)
    try
      @queue.options.process message, callback
    catch err
      callback err, {retry: false}

  processLogMeta: (message, extra) =>
     return _.extend extra, {
      type: @type,
      id: message.properties.messageId,
      attempt: message.attempt,
      delaySinceStarted: Date.now() - message.properties.headers.startedAt,
      delaySincePublished: Date.now() - message.properties.timestamp
    }

  processCallback: (message, err, result) =>
    headers = message.properties.headers

    try
      @stats 'timing', @type, 'e2e', Date.now() - message.properties.timestamp
      @stats 'timing', @type, 'run', Date.now() - headers.startedAt

      if err and result?.retry isnt false and message.shouldRetry isnt false and not message.lastAttempt
        message.finish err, null, false
        @queue.publish message.body, message.properties
        @jobPartFailure? message
        return false
      
      if err
        message.finish err, null, true
        @jobFullFailure? message
        return false 

      else
        message.finish null, result, true
        @jobSuccess? message
        return true

    catch err
      message.finish err, null, true
      @jobFullFailure? message
      @log.error 'processCallback error', err
