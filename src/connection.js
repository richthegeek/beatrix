'use strict';

const amqp = require('amqp-connection-manager');
const amqpMock = require('./mock');

const _ = require('lodash');
const os = require('os');
const url = require('url');
const uuid = require('uuid');
const debug = require('debug')('beatrix');
const Emitter = require('eventemitter2').EventEmitter2;

const Queue = require('./queue');
const Job = require('./job');
const PromiseTimeout = require('./promiseTimeout');

module.exports = class Connection extends Emitter {

  constructor (options) {
    super();

    this.options = this.getOptions(options);

    this.name = this.options.name;

    this.options.onUnhandled = (message) => {
      this.log.error('Unhandled message', message);
      message.nack();
    }

    debug('Connection.constructor starting with options', _.omit(this.options, ['log', 'stats', 'onUnhandled']))

    // for children to link to
    this.exchange = this.options.exchange;
    this.log = this.options.log;
    this.stats = this.options.stats;
    this.onUnhandled = this.options.onUnhandled;
    this.queues = {};
    this.pendingResponses = new Map();

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
      debug('Connection{' + event + '} triggered', value);
    })
  }

  getOptions (options) {
    const name = _.get(options, 'name', 'beatrix');
    const responseQueueName = [os.hostname(), name, process.pid, _.uniqueId(), 'responseQueue'].join('.');

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
    debug('Connection.close() called')
    return Promise.all([
      this.amqp.close(),
    ].concat(_.map(this.queues, (q) => {
      return q.close();
    }))).then(() => {
      debug('Connection.close() closed all queues')
    }, (err) => {
      debug('Connectionc.close() produced an error', err);
      return err
    })
  }

  createChannel (name) {
    debug('Connection.createChannel(' + (name ? name : '') + ')', )
    return this.amqp.createChannel({
      name: name,
      json: true,
      setup: (channel) => {
        debug('Connection.createChannel() setup callback called, calling channel.assertExchange')
        return channel.assertExchange(this.exchange.name, this.exchange.type, _.omit(this.exchange, 'name', 'type')).then((result) => {
          debug('channel.assertExchange ok', result)
        }, (err) => {
          debug('channel.assertExchange produced an error', err);
          return err
        })
      }
    });
  }

  connect () {
    if (this.amqp) {
      debug('Connection.connect() called, but already connected');
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
        debug('Skipping disabled queue from constructor options', name)
        return;
      }
      debug('Creating queue from constructor options', name);
      this.createQueue(name, opts);
    });

    if (this.options.responseQueue && this.options.responseQueue.enabled !== false) {
      debug('Creating responseQueue because this.options.responseQueue is enabled');
      this.createResponseQueue();
    }
    return this;
  }

  send (method, routingKey, body, options) {
    options = _.defaults(options, {
      messageId: body.id || uuid.v4()
    });

    debug('Connection.send() called', {method, routingKey, body, options: _.omit(options, 'bunyan')})

    if (routingKey in this.queues) {
      return this.queues[routingKey][method](body, options);
    }

    const channel = this.responseQueue ? this.responseQueue.channel : this.channel;
    if (!channel) {
      this.channel = channel = this.createChannel('connection');
    }

    const job = new Job(routingKey, _.extend(new Emitter, {
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
      debug('Connection.assertQueue() called for pre-existing queue', name);
      return this.queues[name];
    }
    debug('Connection.assertQueue() called for unknown queue', name);
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
      debug('Connection.createResponseQueue() called but responseQueue already exists');
      return this.responseQueue;
    }

    this.options.responseQueue.process = (message) => message.retry(false) && message.reject('Should not be consumed by this queue')

    this.responseQueue = new Queue(this.options.responseQueue.name, this.options.responseQueue, this);
    this.responseQueue.processJob = (message) => {
      this.responseQueue.channel.ack(message);
      this.handleRequestCallback(message.properties.correlationId, JSON.parse(message.content));
    }

    debug('Connection.createResponseQueue() created a new responseQueue');
    return this.responseQueue;
  }

  handleRequestCallback (id, body) {
    if (this.pendingResponses.has(id)) {
      debug('Connection.handleRequestCallback() triggering callback', {id, body})
      this.pendingResponses.get(id)(body.err, body.result);
    } else {
      debug('Connection.handleRequestCallback() called with unknown ID', id);
    }
  }

  addRequestCallback (options) {
    debug('Connection.addRequestCallback called', _.omit(options, 'bunyan'))
    
    // ensure the RQ is created
    this.createResponseQueue();

    return new PromiseTimeout(options.replyTimeout, (resolve, reject) => {
      const fn = _.once((err, res) => {
        this.pendingResponses.delete(options.correlationId);
        if (err) {
          if (err.isError) {
            err = _.extend(new Error(err.message), err)
          }
          debug('Connection(requestCallback) rejected with error', err)
          return reject(err);
        } else {
          debug('Connection(requestCallback) resolved', res)
          return resolve(res);
        }
      });

      this.pendingResponses.set(options.correlationId, fn);
    });
  }
}
