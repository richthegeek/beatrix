'use strict';

var amqp = require('amqp-connection-manager');
var amqpMock = require('./mock');

var _ = require('lodash');
var os = require('os');
var url = require('url');
var uuid = require('uuid');
var debug = require('debug')('beatrix');
var Emitter = require('eventemitter2').EventEmitter2;

var Queue = require('./queue');
var Job = require('./job');
var PromiseTimeout = require('./promiseTimeout');

module.exports = class Connection extends Emitter {

  constructor (options) {
    super();

    this.options = this.getOptions(options);

    this.name = this.options.name;

    this.options.onUnhandled = (message) => {
      this.log.error('Unhandled message', message);
      message.nack();
    }

    const self = this; // annoying, need arguments and `this` here
    this.options.log.trace = this.options.debug ? _.wrap(this.options.log.trace, function (trace) {
      let args = _.tail(arguments);
      trace.apply(self.options.log, args);
      debug.apply(null, args);
    }) : _.noop;

    this.options.log.trace('Connection.constructor starting with options', _.omit(this.options, ['log', 'stats', 'onUnhandled']))

    // for children to link to
    this.exchange = this.options.exchange;
    this.log = this.options.log;
    this.stats = this.options.stats;
    this.onUnhandled = this.options.onUnhandled;
    this.queues = {};
    this.pendingResponses = {};

    this.on('connect', () => {
      this.log.info('RabbitMQ Connected', this.connection.url.replace(/\/\/(.+?)\:(.+?)@/, (a, user, pass) => {
        return '//' + user + ':' + _.pad('', pass.length, '*') + '@'
      }));
    });

    this.on('disconnect', (err) => {
      this.log.warn('RabbitMQ Disconnected', err);
    });

    this.onAny((event, value) => {
      if (_.has(value, 'message.properties.messageId')) {
        value = '[Job: ' + value.message.properties.messageId + ']'
      } else if (_.has(value, 'messageId')) {
        value = '[Job: ' + value.messageId + ']'        
      } else if (value instanceof Queue) {
        value = '[Queue: ' + value.name + ']'
      } else if (value instanceof Connection) {
        value = '[Connection: ' + value.name + ']'
      }
      this.log.trace('Connection{' + event + '} triggered', value);
    })
  }

  getOptions (options) {
    var name = _.get(options, 'name', 'beatrix');
    var responseQueueName = [os.hostname(), name, process.pid, _.uniqueId(), 'responseQueue'].join('.');

    return _.defaultsDeep({}, options, {
      debug: false,
      log: new console.Console(process.stdout, process.stderr),
      stats: (type, queue, stat, value) => {},
      uri: 'amqp://guest:guest@localhost/',
      exchange: {
        name: name,
        autoDelete: false,
        durable: true,
        type: 'x-delayed-message',
        arguments: {
          'x-delayed-type': 'direct'
        }
      },
      responseQueue: {
        enabled: true,
        name: responseQueueName,
        fullName: responseQueueName,
        routingKey: responseQueueName,
        autoDelete: true,
        exclusive: true,
        messageTtl: 600 * 1000, // clear messages out after 10 minutes
      }
    });    
  }

  isConnected () {
    return this.amqp && this.amqp.isConnected();
  }

  close () {
    this.log.trace('Connection.close() called')
    return Promise.all([
      this.amqp.close(),
    ].concat(_.map(this.queues, (q) => {
      return q.close();
    }))).then(() => {
      this.log.trace('Connection.close() closed all queues')
    }, (err) => {
      this.log.trace('Connectionc.close() produced an error', err);
      return err
    })
  }

  createChannel (name) {
    this.log.trace('Connection.createChannel(' + (name ? name : '') + ')', )
    return this.amqp.createChannel({
      name: name,
      json: true,
      setup: (channel) => {
        this.log.trace('Connection.createChannel() setup callback called, calling channel.assertExchange')
        return channel.assertExchange(this.exchange.name, this.exchange.type, _.omit(this.exchange, 'name', 'type')).then((result) => {
          this.log.trace('channel.assertExchange ok', result)
        }, (err) => {
          this.log.trace('channel.assertExchange produced an error', err);
          return err
        })
      }
    });
  }

  connect () {
    if (this.amqp) {
      this.log.trace('Connection.connect() called, but already connected');
      return this;
    }

    if (this.options.mock) {
      this.amqp = amqpMock.connect(_.castArray(this.options.uri));
    } else {
      this.amqp = amqp.connect(_.castArray(this.options.uri));
    }

    this.amqp.on('connect', (connection) => {
      this.connection = connection;
      this.emit('connect', this);
    });
    this.amqp.on('disconnect', (err) => {
      this.emit('disconnect', err);
    });

    _.each(this.options.queues, (opts, name) => {
      if (opts.enabled === false) {
        this.log.trace('Skipping disabled queue from constructor options', name)
        return;
      }
      this.log.trace('Creating queue from constructor options', name);
      this.createQueue(name, opts);
    });

    if (this.options.responseQueue && this.options.responseQueue.enabled !== false) {
      this.log.trace('Creating responseQueue because this.options.responseQueue is enabled');
      this.createResponseQueue();
    }
    return this;
  }

  send (method, routingKey, body, options) {
    options = _.defaults(options, {
      messageId: body.id || uuid.v4()
    });

    this.log.trace('Connection.send() called', {method, routingKey, body, options: _.omit(options, 'bunyan')})

    if (routingKey in this.queues) {
      return this.queues[routingKey][method](body, options);
    }

    let channel = this.responseQueue ? this.responseQueue.channel : this.channel;
    if (!channel) {
      this.channel = channel = this.createChannel('connection');
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

  assertQueue (name, options) {
    if (_.has(this.queues, name)) {
      this.log.trace('Connection.assertQueue() called for pre-existing queue', name);
      return this.queues[name];
    }
    this.log.trace('Connection.assertQueue() called for unknown queue', name);
    return this.createQueue(name, options);
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
      this.log.trace('Connection.createResponseQueue() called but responseQueue already exists');
      return this.responseQueue;
    }

    this.options.responseQueue.process = (message) => message.retry(false) && message.reject('Should not be consumed by this queue')

    this.responseQueue = new Queue(this.options.responseQueue.name, this.options.responseQueue, this);
    this.responseQueue.processJob = (message) => {
      this.responseQueue.channel.ack(message);
      this.handleRequestCallback(message.properties.correlationId, JSON.parse(message.content));
    }

    this.log.trace('Connection.createResponseQueue() created a new responseQueue');
    return this.responseQueue;
  }

  handleRequestCallback (id, body) {
    var fn = _.get(this.pendingResponses, id);
    if (fn) {
      this.log.trace('Connection.handleRequestCallback() triggering callback', {id, body})
      fn(body.err, body.result);
    } else {
      this.log.trace('Connection.handleRequestCallback() called with unknown ID', id);
    }
  }

  addRequestCallback (options) {
    this.log.trace('Connection.addRequestCallback called', _.omit(options, 'bunyan'))
    
    // ensure the RQ is created
    this.createResponseQueue();

    return new PromiseTimeout(options.replyTimeout, (resolve, reject) => {
      var fn = _.once((err, res) => {
        delete this.pendingResponses[options.correlationId];
        if (err) {
          if (err.isError) {
            err = _.extend(new Error(err.message), err)
          }
          this.log.trace('Connection(requestCallback) rejected with error', err)
          return reject(err);
        } else {
          this.log.trace('Connection(requestCallback) resolved', res)
          return resolve(res);
        }
      });
      this.pendingResponses[options.correlationId] = fn;
    });
  }
}
