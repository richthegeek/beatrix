var amqp = require('amqp-connection-manager');

var _ = require('lodash');
var os = require('os');
var uuid = require('uuid');
var Emitter = require('eventemitter2').EventEmitter2;

var Queue = require('./queue');
var Job = require('./job');
var PromiseTimeout = require('./promiseTimeout');

module.exports = class Connection extends Emitter {

  constructor (options) {
    super();

    this.name = _.get(options, 'name', 'beatrix');

    this.options = _.defaultsDeep(options, {
      log: new console.Console(process.stdout, process.stderr),
      stats: (type, queue, stat, value) => {},
      uri: 'amqp://guest:guest@localhost/',
      exchange: {
        name: this.name,
        autoDelete: false,
        durable: true,
        type: 'x-delayed-message',
        arguments: {
          'x-delayed-type': 'direct'
        }
      },
      responseQueue: {
        name: [os.hostname(), this.name, process.pid, 'responseQueue'].join('.'),
        routingKey: [os.hostname(), this.name, process.pid, 'responseQueue'].join('.'),
        autoDelete: true,
        exclusive: true,
        messageTtl: 600 * 1000, // clear messages out after 10 minutes
      },
      onUnhandled: (message) => {
        this.log.error('Unhandled message', message);
        message.nack();
      }
    });

    // for children to link to
    this.exchange = this.options.exchange;
    this.log = this.options.log;
    this.stats = this.options.stats;
    this.onUnhandled = this.options.onUnhandled;
    this.queues = {};
    this.pendingResponses = {};
  }

  isConnected () {
    return this.amqp && this.amqp.isConnected();
  }

  close () {
    return Promise.all([
      this.amqp.close(),
    ].concat(_.map(this.queues, (q) => {
      return q.close();
    })));
  }

  createChannel () {
    return this.amqp.createChannel({
      json: true,
      setup: (channel) => {
        return channel.assertExchange(this.exchange.name, this.exchange.type, _.omit(this.exchange, 'name', 'type'));
      }
    });
  }

  connect () {
    if (!this.amqp) {
      this.amqp = amqp.connect(_.castArray(this.options.uri));
      this.amqp.on('connect', () => {
        this.emit('connect', this);
        this.log.info('RabbitMQ Connected');
      });
      this.amqp.on('disconnect', (err) => {
        this.emit('disconnect', err);
        this.log.warn('RabbitMQ Disconnected', err)
      });

      _.each(this.options.queues, (opts, name) => {
        if (opts.enabled === false) {
          return;
        }
        this.createQueue(name, opts);
      });
    }
    return this;
  }

  send (method, routingKey, body, options) {
    options = _.defaults(options, {
      messageId: body.id || uuid.v4()
    });

    if (routingKey in this.queues) {
      return this.queues[routingKey][method](body, options);
    }

    let channel = this.responseQueue ? this.responseQueue.channel : this.channel;
    if (!channel) {
      this.channel = channel = this.createChannel();
    }

    var job = new Job(routingKey, _.extend(new Emitter, {
      options: {
        name: routingKey,
        routingKey: this.exchange.name + '.' + routingKey
      },
      channel: channel,
      connection: this,
      queue: new Emitter()
    }));

    return job[method](body, options);
  }

  publish (routingKey, body, options) {
    return this.send('publish', routingKey, body, options);
  }

  request (routingKey, body, options) {
    return this.send('request', routingKey, body, options);
  }

  createQueue (name, options) {
    if (!this.amqp) {
      throw new Error('BeatrixConnection.createQueue called before BeatrixConnection.connect');
    }

    this.queues[name] = new Queue(name, options, this);

    // passthrough events
    this.queues[name].on('success', this.emit.bind(this, 'success'));
    this.queues[name].on('partFailure', this.emit.bind(this, 'partFailure'));
    this.queues[name].on('fullFailure', this.emit.bind(this, 'fullFailure'));

    return this.queues[name];
  }

  createResponseQueue () {
    if (this.options.responseQueue === false) {
      return null;
    }

    if (this.responseQueue) {
      return this.responseQueue;
    }

    this.options.responseQueue.process = (message) => message.retry(false) && message.reject('Should not be consumed by this queue')

    this.responseQueue = new Queue(this.options.responseQueue.name, this.options.responseQueue, this);
    this.responseQueue.processJob = (message) => {
      this.responseQueue.channel.ack(message);

      var body = JSON.parse(message.content);
      var fn = _.get(this.pendingResponses, message.properties.correlationId);
      if (fn) {
        fn(body.err, body.result);
      }
    }

    return this.responseQueue;
  }

  addRequestCallback (options) {
    // ensure the RQ is created
    this.createResponseQueue();

    return new PromiseTimeout(options.replyTimeout, (resolve, reject) => {
      var fn = _.once((err, res) => {
        delete this.pendingResponses[options.correlationId];
        if (err) {
          if (err.isError) {
            err = _.extend(new Error(err.message), err)
          }
          return reject(err);
        } else {
          return resolve(res);
        }
      });
      this.pendingResponses[options.correlationId] = fn;
    });
  }
}
