(function() {
  var Job, Queue, Rabbit, _;

  _ = require('lodash');

  Rabbit = require('amqplib');

  Job = require('./job');

  module.exports = Queue = (function() {
    function Queue(name1, options, connection) {
      var ref, ref1, ref2;
      this.name = name1;
      this.connection = connection;
      this.log = this.connection.log;
      this.stats = this.connection.stats;
      this.options = _.defaultsDeep(options, {
        name: this.name,
        type: this.connection.exchange.name + '.' + this.name,
        timeout: null,
        concurrency: 1,
        id: 0,
        durable: true,
        autoDelete: false
      });
      this.connected = false;
      this.stack = (ref = (ref1 = this.connection.queues) != null ? (ref2 = ref1[this.name]) != null ? ref2.stack : void 0 : void 0) != null ? ref : [];
      this.lastPublish = 0;
      this.lastComplete = 0;
      this.lastSuccess = 0;
      this.pending = 0;
    }

    Queue.prototype.connect = function(cb) {
      var concurrency, name, ref, ref1, routingKey, type;
      ref = this.options, name = ref.name, type = ref.type, concurrency = ref.concurrency, routingKey = ref.routingKey;
      try {
        if ((ref1 = this.channel) != null) {
          if (typeof ref1.close === "function") {
            ref1.close()["catch"]((function(_this) {
              return function(err) {
                return null;
              };
            })(this));
          }
        }
      } catch (error) {}
      return this.connection.connection.createChannel().then((function(_this) {
        return function(channel) {
          _this.channel = channel;
          return _this.channel.assertQueue(type, _.omit(_this.options, ['name', 'type', 'routingKey', 'concurrency']));
        };
      })(this)).then((function(_this) {
        return function() {
          return _this.channel.bindQueue(type, _this.connection.exchange.name, routingKey != null ? routingKey : type);
        };
      })(this)).then((function(_this) {
        return function() {
          return _this.channel.recover();
        };
      })(this)).then((function(_this) {
        return function() {
          return _this.channel.prefetch(concurrency);
        };
      })(this)).then((function(_this) {
        return function() {
          return _this.channel.consume(type, _this.processJob.bind(_this));
        };
      })(this)).then((function(_this) {
        return function() {
          var timer;
          timer = setInterval(_this.checkQueue.bind(_this), 15 * 1000);
          _this.channel.on('close', function() {
            _this.log.error({
              type: type
            }, 'Channel closed. Reconnecting.');
            clearTimeout(timer);
            _this.connected = false;
            return _this.connect();
          });
          _this.channel.on('error', function(err) {
            return _this.log.error({
              type: type
            }, 'Channel errored.', err);
          });
          _this.channel.on('returned', function(msg) {
            return _this.log.error({
              type: type
            }, 'Unroutable message returned!');
          });
          _this.connected = true;
          setTimeout((function() {
            var i, item, len, ref2, results;
            ref2 = _this.stack;
            results = [];
            for (i = 0, len = ref2.length; i < len; i++) {
              item = ref2[i];
              results.push(_this[item.type](item.body, item.options, item.cb));
            }
            return results;
          }), 100);
          return typeof cb === "function" ? cb(null, _this) : void 0;
        };
      })(this))["catch"]((function(_this) {
        return function(err) {
          _this.log.error({
            type: type
          }, "Could not connect queue", err.stack);
          return setTimeout((function() {
            return _this.connect(cb);
          }), 1000);
        };
      })(this));
    };

    Queue.prototype.checkQueue = function() {
      var type;
      type = this.options.type;
      return this.channel.checkQueue(type).then((function(_this) {
        return function(ok) {
          var lag, timeout;
          _this.stats('increment', type, 'pending', _this.pending);
          _this.stats('increment', type, 'consumers', ok.consumerCount);
          _this.stats('increment', type, 'messages', ok.messageCount);
          lag = Math.abs(Date.now() - _this.lastComplete);
          timeout = (_this.options.timeout || 60000) * 2;
          if (ok.messageCount > 0 && (_this.pending === 0 || lag > timeout)) {
            _this.pending = 0;
            return _this.channel.recover().then(function(outcome) {
              _this.log.info({
                type: type
              }, 'RECOVER', outcome);
              return _this.channel.get(type);
            }).then(function(message) {
              if (message) {
                _this.log.info({
                  type: type
                }, 'Manually retrieved message, consuming');
                return _this.processJob(message);
              } else {
                return _this.log.info({
                  type: type
                }, 'No message retrieved despite count=' + ok.messageCount + '. Investigate.');
              }
            });
          }
        };
      })(this));
    };

    Queue.prototype.publish = function(body, options, cb) {
      var job;
      if (!this.connected) {
        return this.stack.push({
          type: 'publish',
          body: body,
          options: options,
          cb: cb
        });
      }
      job = new Job(this.options.type, this);
      return job.publish(body, options, cb);
    };

    Queue.prototype.request = function(body, options, cb) {
      var job;
      if (!this.connected) {
        return this.stack.push({
          type: 'request',
          body: body,
          options: options,
          cb: cb
        });
      }
      job = new Job(this.options.type, this);
      return job.request(body, options, cb);
    };

    Queue.prototype.processJob = function(message) {
      var job;
      this.pending++;
      job = new Job(this.options.type, this);
      return job.process(message);
    };

    Queue.prototype.jobSuccess = function(message, result) {
      var base, base1;
      this.pending = Math.max(0, this.pending - 1);
      this.lastComplete = Date.now();
      this.lastSuccess = Date.now();
      this.stats('increment', this.options.type, 'ok', 1);
      if (typeof (base = this.options).jobSuccess === "function") {
        base.jobSuccess(message, result);
      }
      return typeof (base1 = this.connection).jobSuccess === "function" ? base1.jobSuccess(message, result) : void 0;
    };

    Queue.prototype.jobPartFailure = function(message, err, result) {
      var base, base1;
      this.pending = Math.max(0, this.pending - 1);
      this.lastComplete = Date.now();
      this.stats('increment', this.options.type, 'part_fail', 1);
      if (typeof (base = this.options).jobPartFailure === "function") {
        base.jobPartFailure(message, err, result);
      }
      return typeof (base1 = this.connection).jobPartFailure === "function" ? base1.jobPartFailure(message, err, result) : void 0;
    };

    Queue.prototype.jobFullFailure = function(message, err, result) {
      var base, base1;
      this.pending = Math.max(0, this.pending - 1);
      this.lastComplete = Date.now();
      this.stats('increment', this.options.type, 'full_fail', 1);
      if (typeof (base = this.options).jobFullFailure === "function") {
        base.jobFullFailure(message, err, result);
      }
      return typeof (base1 = this.connection).jobFullFailure === "function" ? base1.jobFullFailure(message, err, result) : void 0;
    };

    return Queue;

  })();

}).call(this);

//# sourceMappingURL=queue.js.map
