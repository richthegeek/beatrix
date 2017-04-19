(function() {
  var ExponentialBackoff, Job, Rabbit, Timeout, _, uuid,
    bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

  _ = require('lodash');

  uuid = require('uuid');

  Rabbit = require('amqplib');

  Timeout = require('./callbackTimeout');

  ExponentialBackoff = require('backoff-strategies').Exponential;

  module.exports = Job = (function() {
    function Job(type, queue) {
      this.type = type;
      this.queue = queue;
      this.processCallback = bind(this.processCallback, this);
      this.processLogMeta = bind(this.processLogMeta, this);
      this.connection = this.queue.connection;
      this.stats = this.connection.stats;
      this.log = this.connection.log;
      this.channel = this.queue.channel;
    }

    Job.prototype.mergePublishOptions = function(options) {
      var backoff;
      this.queue.lastPublish = Date.now();
      options = _.defaultsDeep({}, options, this.queue.options, {
        type: this.type,
        contentType: 'application/json',
        publishedAt: Date.now(),
        headers: {},
        attempts: 0,
        maxAttempts: 1,
        initialDelay: 0,
        delay: 1000,
        maxDelay: 86400 * 1000
      });
      if (options.messageId == null) {
        options.messageId = this.queue.options.name + '.' + (++this.queue.options.id);
      }
      options.timestamp = Date.now();
      if (options.routingKey == null) {
        options.routingKey = options.type;
      }
      delete options.timeout;
      _.defaults(options.headers, _.pick(options, ['attempts', 'maxAttempts', 'delay']));
      backoff = new ExponentialBackoff({
        minValue: options.initialDelay,
        maxValue: options.maxDelay,
        multiplier: options.delay,
        zeroMeansZero: true
      });
      options.headers['x-delay'] = backoff.get(options.headers.attempts);
      return options;
    };

    Job.prototype.publish = function(body, options, cb) {
      var result;
      options = this.mergePublishOptions(options);
      if (!(options.headers.attempts <= options.headers.maxAttempts)) {
        return typeof cb === "function" ? cb("Rejecting publish due to too many attempts: " + options.headers.attempts + " >= " + options.headers.maxAttempts) : void 0;
      }
      this.log.info({
        type: this.type,
        id: options.messageId,
        request: options.replyTo != null
      }, 'Publishing job to queue', body);
      body = new Buffer(JSON.stringify(body));
      result = this.channel.publish(this.connection.exchange.name, options.routingKey, body, options);
      if (result) {
        return typeof cb === "function" ? cb(null, 'OK') : void 0;
      } else {
        return typeof cb === "function" ? cb('Queue full.') : void 0;
      }
    };

    Job.prototype.request = function(body, options, cb) {
      options = _.defaults({}, options, {
        replyTimeout: 5000,
        replyTo: this.connection.responseQueue,
        correlationId: uuid.v4()
      });
      this.connection.addRequestCallback(options, cb);
      return this.publish(body, options);
    };

    Job.prototype.jobPartFailure = function(message, err, result) {
      var base;
      return typeof (base = this.queue).jobPartFailure === "function" ? base.jobPartFailure(message, err, result) : void 0;
    };

    Job.prototype.jobFullFailure = function(message, err, result) {
      var base;
      return typeof (base = this.queue).jobFullFailure === "function" ? base.jobFullFailure(message, err, result) : void 0;
    };

    Job.prototype.jobSuccess = function(message, result) {
      var base;
      return typeof (base = this.queue).jobSuccess === "function" ? base.jobSuccess(message, result) : void 0;
    };

    Job.prototype.process = function(message) {
      var callback, err, headers, props;
      props = message.properties;
      headers = props.headers;
      headers.attempts += 1;
      headers.startedAt = Date.now();
      this.stats('timing', this.type, 'startDelay', Date.now() - props.timestamp);
      message.ack = _.once((function(_this) {
        return function() {
          try {
            return _this.channel.ack(message);
          } catch (error) {}
        };
      })(this));
      message.nack = _.once((function(_this) {
        return function() {
          try {
            return _this.channel.nack(message);
          } catch (error) {}
        };
      })(this));
      message.body = JSON.parse(message.content);
      message.attempt = headers.attempts;
      message.firstAttempt = message.attempt === 1;
      message.lastAttempt = headers.attempts >= headers.maxAttempts;
      message.finish = (function(_this) {
        return function(err, result, final) {
          var body;
          message.ack();
          body = {
            err: err,
            result: result,
            final: final
          };
          if (props.correlationId && props.replyTo && (final || !err)) {
            _this.log.info(_this.processLogMeta(message), 'Replying', body);
            body = new Buffer(JSON.stringify(body));
            return _this.channel.sendToQueue(props.replyTo, body, {
              correlationId: props.correlationId
            });
          } else {
            return _this.log.info(_this.processLogMeta(message), 'Acking', body);
          }
        };
      })(this);
      this.log.info(this.processLogMeta(message, {
        timeout: this.queue.options.timeout
      }), 'Starting');
      callback = Timeout(this.queue.options.timeout, this.processCallback.bind(this, message));
      try {
        return this.queue.options.process(message, callback);
      } catch (error) {
        err = error;
        return callback(err, {
          retry: false
        });
      }
    };

    Job.prototype.processLogMeta = function(message, extra) {
      return _.extend(extra, {
        type: this.type,
        id: message.properties.messageId,
        attempt: message.attempt,
        delaySinceStarted: Date.now() - message.properties.headers.startedAt,
        delaySincePublished: Date.now() - message.properties.timestamp
      });
    };

    Job.prototype.processCallback = function(message, err, result) {
      var headers;
      headers = message.properties.headers;
      try {
        this.stats('timing', this.type, 'e2e', Date.now() - message.properties.timestamp);
        this.stats('timing', this.type, 'run', Date.now() - headers.startedAt);
        if (err && (result != null ? result.retry : void 0) !== false && message.shouldRetry !== false && !message.lastAttempt) {
          message.finish(err, null, false);
          this.queue.publish(message.body, message.properties);
          if (typeof this.jobPartFailure === "function") {
            this.jobPartFailure(message, err, result);
          }
          return false;
        }
        if (err) {
          message.finish(err, null, true);
          if (typeof this.jobFullFailure === "function") {
            this.jobFullFailure(message, err, result);
          }
          return false;
        } else {
          message.finish(null, result, true);
          if (typeof this.jobSuccess === "function") {
            this.jobSuccess(message, result);
          }
          return true;
        }
      } catch (error) {
        err = error;
        message.finish(err, null, true);
        if (typeof this.jobFullFailure === "function") {
          this.jobFullFailure(message, err, result);
        }
        return this.log.error('processCallback error', err);
      }
    };

    return Job;

  })();

}).call(this);

//# sourceMappingURL=job.js.map
