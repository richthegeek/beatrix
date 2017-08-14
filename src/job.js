var _ = require('lodash');
var uuid = require('uuid');
var Backoff = require('backoff-strategies');

var TimeoutPromise = require('./promiseTimeout');

module.exports = class Job {

  constructor (type, queue) {
    this.type = type;
    this.queue = queue;
    this.connection = queue.connection;
    this.channel = queue.channel;
    this.stats = queue.connection.stats;
    this.log = queue.connection.log;
  }

  mergePublishOptions (options) {
    options = _.defaultsDeep({}, options, this.queue.options, {
      contentType: 'application/json',
      publishedAt: Date.now(),
      headers: {},
      attempts: 0,
      maxAttempts: 1,
      initialDelay: 0,
      delay: 1000,
      maxDelay: 86400 * 1000,
      delayStrategy: 'Exponential'
    });

    // default here to prevent missing ids
    options.messageId = options.messageId || this.queue.options.name + '.' + (++this.queue.options.id);

    options.timestamp = Date.now()

    // double-default
    options.routingKey = options.routingKey || this.queue.options.routingKey

    // copy things over to the headers
    _.defaults(options.headers, _.pick(options, ['attempts', 'maxAttempts', 'delay', 'timeout']));

    // set the delay
    let delay = this.getDelay(options.headers.attempts, options);
    if (delay) {
      options.headers['x-delay'] = delay;
    }

    return options
  }

  getDelay (attempt, options) {
    if (attempt === 0) {
      return options.initialDelay
    }

    if (!options.delay) {
      return undefined;
    }

    var delayProps = {
      minValue: 0,
      maxValue: options.maxDelay,
      multiplier: options.delay
    }

    var strategy = _.upperFirst(options.delayStrategy)
    if (strategy === 'Defined') {
      strategy = Backoff.Defined;
      delayProps.values = _.castArray(options.delay);
      delayProps.multiplier = 1;
    } else if (strategy === 'Linear') {
      strategy = Backoff.Linear
    } else if (strategy === 'Polynomial') {
      strategy = Backoff.Polynomial;
      delayProps.factor = options.delayFactor || 2;
    } else {
      strategy = Backoff.Exponential;
    }

    return new strategy(delayProps).get(attempt);
  }

  publish (body, options) {
    options = this.mergePublishOptions(options);

    if (options.headers.attempts > options.headers.maxAttempts) {
      return Promise.reject("Rejecting publish due to too many attempts: ${options.headers.attempts} >= ${options.headers.maxAttempts}");
    }


    // bodyBuffer = new Buffer JSON.stringify body
    var promise = this.channel.publish(this.connection.exchange.name, options.routingKey, body, options);

    promise.then((a,b) => {
      this.queue.emit('publish', _.extend({}, options, {body})); // should set lastPublish time
      this.log.info({queue: this.type, id: options.messageId, request: !! options.replyTo}, 'Published job to queue', body);
    }).catch((err) => {
      this.log.error({queue: this.type, id: options.messageId, request: !! options.replyTo, err: err}, 'Could not publish job!', body);
    });

    return promise;
  }

  request (body, options, cb) {
    options = _.defaults({}, options, {
      replyTimeout: 5000,
      replyTo: this.connection.options.responseQueue.name,
      correlationId: uuid.v4()
    });
    return Promise.all([
      this.publish(body, options),
      this.connection.addRequestCallback(options)
    ]).then((results) => {
      return results[1]
    });
  }

  process (message) {
    var props = message.properties;
    var headers = props.headers;
    headers.attempts += 1;
    headers.startedAt = Date.now();

    this.stats('timing', this.type, 'startDelay', Date.now() - props.timestamp);

    message.finished = false
    message.finish = (err, result, final) => {
      if (message.finished) {
        return this.log.fatal(this.processLogMeta(message), 'Job was already acked/nacked');
      }

      message.finished = true;
      this.channel.ack(message);

      var body = {err, result, final};
      if (props.correlationId && props.replyTo && (final || !err)) {
        this.log.info(this.processLogMeta(message), 'Replying', body);
        this.channel.sendToQueue(props.replyTo, body, {correlationId: props.correlationId})
      } else {
        this.log.info(this.processLogMeta(message), 'Acking', body);
      }
    }

    message.retry = (should) => {
      message.shouldRetry = should;
    }
    message.retry(true);

    try {
      message.body = JSON.parse(message.content)
    } catch (err) {
      return message.finish(err, err, true)
    }

    message.attempt = headers.attempts;
    message.firstAttempt = message.attempt === 1;
    message.lastAttempt = (headers.attempts >= headers.maxAttempts);

    this.log.info(this.processLogMeta(message, {timeout: headers.timeout}), 'Starting');

    new TimeoutPromise(headers.timeout, (resolve, reject) => {
      message.reject = reject;
      message.resolve = resolve;
      this.queue.options.process(message);
    }).then((result) => {
      this.processSuccess(message, result);
      this.processFinish(message);
    }).catch((err, a, b) => {
      if (err instanceof Error) {
        err = _.extend({}, {
          isError: true,
          message: err.message,
          stack: err.stack
        }, err);
      }
      this.processFailure(message, err);
      this.processFinish(message);
    });
  }

  processLogMeta (message, extra) {
     return _.extend(_.pickBy(extra, Boolean), {
      queue: this.type,
      id: message.properties.messageId,
      attempt: message.attempt,
      delaySinceStarted: Date.now() - message.properties.headers.startedAt,
      delaySincePublished: Date.now() - message.properties.timestamp
    });
  }

  processSuccess (message, result) {
    message.finish(null, result, true);
    this.queue.emit('success', {message, result});
  }

  processFinish (message) {
    this.stats('timing', this.type, 'e2e', Date.now() - message.properties.timestamp);
    this.stats('timing', this.type, 'run', Date.now() - message.properties.headers.startedAt);
  }

  processFailure (message, err) {
    if (message.shouldRetry !== false && !message.lastAttempt) {
      message.finish(err, null, false);
      this.queue.publish(message.body, message.properties);
      this.queue.emit('partFailure', {message, err});
    } else {
      message.finish(err, null, true);
      this.queue.emit('fullFailure', {message, err});
    }
  }
}
