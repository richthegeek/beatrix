(function() {
  var Connection, Job, Queue, Rabbit, Timeout, _, os, uuid;

  _ = require('lodash');

  os = require('os');

  uuid = require('uuid');

  Rabbit = require('amqplib');

  Queue = require('./queue');

  Job = require('./job');

  Timeout = require('./callbackTimeout');

  module.exports = Connection = (function() {
    function Connection(options) {
      this.options = _.defaultsDeep(options, {
        log: new console.Console(process.stdout, process.stderr),
        stats: function(type, queue, stat, value) {
          return null;
        },
        connection: {
          uri: 'amqp://guest:guest@localhost/'
        },
        exchange: {
          name: 'beatrix',
          autoDelete: false,
          durable: true,
          type: 'x-delayed-message',
          "arguments": {
            'x-delayed-type': 'direct'
          }
        },
        onUnhandled: function(message) {
          this.log.error('Unhandled message', message);
          return message.nack();
        }
      });
      this.exchange = this.options.exchange;
      this.log = this.options.log;
      this.stats = this.options.stats;
      this.onUnhandled = this.options.onUnhandled;
      this.queues = {};
      this.pendingResponses = {};
      _.each(options.queues, (function(_this) {
        return function(opts, name) {
          return _this.queues[name] = {
            publish: function(body, options, cb) {
              return _this.queues[name].stack.push({
                type: 'publish',
                body: body,
                options: options,
                cb: cb
              });
            },
            request: function(body, options, cb) {
              return _this.queues[name].stack.push({
                type: 'publish',
                body: body,
                options: options,
                cb: cb
              });
            },
            stack: []
          };
        };
      })(this));
    }

    Connection.prototype.send = function(method, routingKey, body, options, cb) {
      var job, ref;
      if (options.messageId == null) {
        options.messageId = (ref = body.id) != null ? ref : uuid.v4();
      }
      if (options.routingKey == null) {
        options.routingKey = routingKey;
      }
      if (this.queues[routingKey]) {
        return this.queues[routingKey][method](body, options, cb);
      }
      job = new Job(routingKey, {
        options: {
          name: routingKey
        },
        channel: this.channel,
        connection: this
      });
      return job[method](body, options, cb);
    };

    Connection.prototype.publish = function(routingKey, body, options, cb) {
      return this.send('publish', routingKey, body, options, cb);
    };

    Connection.prototype.request = function(routingKey, body, options, cb) {
      return this.send('request', routingKey, body, options, cb);
    };

    Connection.prototype.connect = function(cb) {
      return Rabbit.connect(this.options.connection.uri, {
        heartbeat: 5
      }).then((function(_this) {
        return function(connection) {
          _this.connection = connection;
          return _this.connection.createChannel();
        };
      })(this)).then((function(_this) {
        return function(channel) {
          _this.channel = channel;
          return _this.channel.assertExchange(_this.exchange.name, _this.exchange.type, _.omit(_this.exchange, 'name', 'type'));
        };
      })(this)).then((function(_this) {
        return function() {
          _this.createResponseQueue();
          _this.log.info('RabbitMQ Connected!');
          cb(null, _this);
          return _this;
        };
      })(this))["catch"]((function(_this) {
        return function(err) {
          _this.log.error('Could not connect', err);
          cb(err, _this);
          return _this;
        };
      })(this));
    };

    Connection.prototype.createQueue = function(name, options, cb) {
      this.log.info('Create Queue: ' + name);
      this.queues[name] = new Queue(name, options, this);
      return this.queues[name].connect((function(_this) {
        return function(err) {
          if (err) {
            _this.log.error('Could not create queue', err);
            return typeof cb === "function" ? cb(err) : void 0;
          }
          return typeof cb === "function" ? cb(null, _this.queues[name]) : void 0;
        };
      })(this));
    };

    Connection.prototype.partFailure = function(message) {
      var base;
      return typeof (base = this.options).partFailure === "function" ? base.partFailure(message) : void 0;
    };

    Connection.prototype.fullFailure = function(message) {
      var base;
      return typeof (base = this.options).fullFailure === "function" ? base.fullFailure(message) : void 0;
    };

    Connection.prototype.createResponseQueue = function() {
      var queue;
      this.responseQueue = [os.hostname(), process.title, process.pid, 'response', 'queue'].join('.');
      queue = new Queue(this.responseQueue, {
        name: this.responseQueue,
        type: this.responseQueue,
        autoDelete: true,
        exclusive: true,
        messageTtl: 600 * 1000
      }, this);
      queue.processJob = (function(_this) {
        return function(message) {
          var body, cb;
          queue.channel.ack(message);
          body = JSON.parse(message.content);
          cb = _this.pendingResponses[message.properties.correlationId];
          return typeof cb === "function" ? cb(body.err, body.result) : void 0;
        };
      })(this);
      return queue.connect();
    };

    Connection.prototype.addRequestCallback = function(options, cb) {
      var fn;
      fn = _.once((function(_this) {
        return function(err, res) {
          delete _this.pendingResponses[options.correlationId];
          return cb(err, res);
        };
      })(this));
      return this.pendingResponses[options.correlationId] = Timeout(options.replyTimeout, fn);
    };

    return Connection;

  })();

}).call(this);

//# sourceMappingURL=connection.js.map
