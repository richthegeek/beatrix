'use strict';

const _ = require('lodash');
const uuid = require('uuid');
const debug = require('debug')('beatrix');
const Backoff = require('backoff-strategies');

const TimeoutPromise = require('./promiseTimeout');

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
      exchange: this.connection.exchange.name,
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
    options.messageId = options.messageId || this.queue.name + '.' + (++this.queue.options.id);

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

    // copy over bunyan fields into headers
    if (_.has(options, 'bunyan')) {
      options.headers.bunyan = _.omit(options.bunyan.fields, ['name', 'hostname', 'pid'])
    }

    return _.omit(options, ['bunyan', 'timeout', 'delay', 'durable', 'autoDelete', 'process', 'concurrency'])
  }

  getDelay (attempt, options) {
    if (attempt === 0) {
      return options.initialDelay
    }

    if (!options.delay) {
      return undefined;
    }

    const delayProps = {
      minValue: 0,
      maxValue: options.maxDelay,
      multiplier: options.delay
    }

    let strategy = Backoff.Exponential;

    switch (_.upperFirst(options.delayStrategy)) {
      case 'Defined':
        strategy = Backoff.Defined;
        delayProps.values = _.castArray(options.delay);
        delayProps.multiplier = 1;
        break;
      case 'Linear':
        strategy = Backoff.Linear;
        break;
      case 'Polynomial':
        strategy = Backoff.Polynomial;
        delayProps.factor = options.delayFactor || 2;
        break;
    }

    return new strategy(delayProps).get(attempt);
  }

  publish (body, options) {
    options = this.mergePublishOptions(options);

    if (options.headers.attempts > options.headers.maxAttempts) {
      return Promise.reject("Rejecting publish due to too many attempts: ${options.headers.attempts} >= ${options.headers.maxAttempts}");
    }

    const logProperties = {queue: this.type, id: options.messageId, request: !! options.replyTo, exchange: options.exchange, routingKey: options.routingKey}
    if (this.log.child) {
      this.log = this.log.child(logProperties)
    }

    /*
      This codeblock allows the queue to instantly run a job in this process
       if the process does not have a pending queue larger than the defined concurrency.
      It uses the new Promise + process.nextTick pattern to avoid zalgo, maybe this is unrequired.
      The pending++ and pending-- is to prevents jobs processed in the next tick not being counted.
      It assumes that the local process is capable of processing the job.
    */
    if (options.bypass && this.queue.options.concurrency > this.queue.pending) {
      options.bypassed = true;
      this.queue.pending++
      debug(logProperties, 'Job.publish() triggered a bypass, with ' + this.queue.pending + ' active bypass jobs');
      return new Promise((resolve, reject) => {
        process.nextTick(() => {
          this.queue.pending--
          this.log.info('Immediately ran job in queue', body);
          this.queue.processJob({
            fields: {},
            properties: options,
            content: new Buffer(JSON.stringify(body))
          }).then(resolve, reject)
        })
      })
    }

    debug('Job.publish() publishing', options, body)
    const promise = this.channel.publish(options.exchange, options.routingKey, body, options);

    promise.then((a,b) => {
      this.queue.emit('publish', _.extend({}, options, {body})); // should set lastPublish time
      this.log.info({queue: this.type, id: options.messageId, request: !! options.replyTo, exchange: options.exchange, routingKey: options.routingKey}, 'Published job to queue', body);
      return options;
    }, (err) => {
      this.log.error({queue: this.type, id: options.messageId, request: !! options.replyTo, exchange: options.exchange, routingKey: options.routingKey, err: err}, 'Could not publish job!', body);
      return err;
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
    const props = message.properties;
    const headers = props.headers;
    headers.attempts += 1;
    headers.startedAt = Date.now();

    this.stats('timing', this.type, 'startDelay', Date.now() - props.timestamp);

    // support for bunyan child
    const logProperties = _.defaults({
      queue: this.type,
      id: message.properties.messageId,
      attempt: message.attempt,
      get delaySincePublished () {
        return Date.now() - props.timestamp
      },
      get delaySinceStarted () {
        return Date.now() - headers.startedAt
      }
    }, headers.bunyan);

    message.log = this.log;
    if (message.log.child) {
      message.log = message.log.child(logProperties);
    }

    debug(logProperties, 'Job.process() got message', message);

    message.finished = false
    message.finish = (err, result, final) => {
      if (message.finished) {
        return message.log.fatal(this.processLogMeta(message), 'Job was already acked/nacked');
      }

      debug(logProperties, 'Job.process().finish() called', {err, result, final})

      message.finished = true;
      if (!props.bypassed) {
        debug(logProperties, 'Job.process().finish() calling channel.ack()')
        this.channel.ack(message);
      }

      const reply = props.correlationId && props.replyTo && (final || !err);
      const body = {err, result, final};
      if (reply) {
        message.log.info(this.processLogMeta(message), 'Replying', body);
        if (props.bypassed) {
          this.connection.handleRequestCallback(props.correlationId, body);
        } else {
          debug(logProperties, 'Job.process().finish() replying with sendToQueue', {queue: props.replyTo, correlationId: props.correlationId, body: body})
          this.channel.sendToQueue(props.replyTo, body, {correlationId: props.correlationId})
        }
      } else {
        message.log.info(this.processLogMeta(message), 'Acking', body);
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

    message.log.info(this.processLogMeta(message, {timeout: headers.timeout}), 'Starting');

    return new TimeoutPromise(headers.timeout, (resolve, reject) => {
      message.reject = reject;
      message.resolve = resolve;
      this.queue.options.process(message);
    }).then((result) => {
      debug(logProperties, 'Job.process() resolved with:', result)
      this.processSuccess(message, result);
      this.processFinish(message);
    }).catch((err) => {
      debug(logProperties, 'Job.process() rejected with:', err)
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
      delaySinceStarted: Date.now() - message.properties.headers.startedAt,
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
