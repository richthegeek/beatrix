'use strict';

var _ = require('lodash');
var Job = require('./job');
var Emitter = require('eventemitter2').EventEmitter2;

module.exports = class Queue extends Emitter {

  constructor (name, options, connection) {
    super();
    this.name = _.get(options, 'name', name);
    this.connection = connection;
    this.log = connection.log;
    this.stats = connection.stats;
    this.options = _.defaultsDeep(options, {
      fullName: connection.name + '.' + this.name,
      routingKey: this.name,
      concurrency: 1,
      id: 0,
      durable: true,
      autoDelete: false,
      bypass: false
    });

    if (this.log.child) {
      this.log = this.log.child({queue: this.name})
    }
    this.log.trace = this.connection.log.trace.bind(this.connection.log, {queue: this.name})

    this.channel = connection.createChannel(this.name);
    this.channel.addSetup(this.setup.bind(this));

    this.channel.on('connect', () => this.emit('connect', this));
    this.channel.on('close', this.emit.bind(this, 'close'));
    this.channel.on('error', this.emit.bind(this, 'error'));
    this.channel.on('drop', this.emit.bind(this, 'drop'));

    this.channel.on('connect', () => {
      this.log.info('Queue Connected');
      this.connected = true
    });
    this.channel.on('close', () => {
      this.log.info('Queue Closed');
      this.connected = false
    });
    this.channel.on('error', this.log.error.bind(this.log, 'RabbitMQ Queue Error!'));
    this.channel.on('drop', this.log.error.bind(this.log, 'RabbitMQ Queue Drop!'));

    this.lastPublish = 0;
    this.lastComplete = 0;
    this.lastSuccess = 0;
    this.pending = 0;

    this.on('success', this.onSuccess.bind(this));
    this.on('partFailure', this.onPartFailure.bind(this));
    this.on('fullFailure', this.onFullFailure.bind(this));

    this.log.trace('Queue.constructor starting with options', this.options);
    var self = this;
    this.onAny((event, value) => {
      if (_.has(value, 'message.properties.messageId')) {
        value = '[Job: ' + value.message.properties.messageId + ']'
      } else if (_.has(value, 'messageId')) {
        value = '[Job: ' + value.messageId + ']'        
      } else if (value instanceof Queue) {
        value = '[Queue: ' + value.name + ']'
      }
      this.log.trace('Queue{' + event + '} triggered', value);
    });

    setInterval(() => this.status(false), 5000)
  }

  setup (channel) {
    var queueOptions = _.omit(this.options, ['concurrency', 'routingKey']);

    var process = 'function' === typeof this.options.process;

    var promise = Promise.all([
      channel.assertQueue(this.options.fullName, queueOptions),
      channel.bindQueue(this.options.fullName, this.connection.exchange.name, this.options.routingKey),
      process ? channel.prefetch(this.options.concurrency) : Promise.resolve(0),
      process ? channel.consume(this.options.fullName, this.processJob.bind(this)) : Promise.resolve(0)
    ]);

    if (process) {
      this.log.info({queue: this.name}, 'Queue is being consumed by this process');
    } else {
      this.log.info({queue: this.name}, 'Queue is not being consumed by this process');
    }

    return promise.then((result) => {
      result = _.merge({}, result[0], result[3]);
      // bind and prefetch have no interesting information
      this.emit('setup', result);
    }, (err) => {
      this.log.trace('Queue.setup() rejected', err)
      return err
    });
  }

  close () {
    return this.channel.close();
  }

  purge () {
    this.log.trace('Queue.purge() called')
    return this.channel._channel.purgeQueue(this.options.fullName);
  }

  delete () {
    this.log.trace('Queue.delete() called')
    return this.channel._channel.deleteQueue(this.options.fullName);
  }

  status (wait) {
    if (!this.channel || !this.channel._channel) {
      return wait ?
        new Promise((resolve, reject) => {
          this.once('setup', () => {
            this.status().then(resolve, reject);
          });
        })
        : Promise.resolve({connected: false});
    }

    return this.channel._channel.checkQueue(this.options.fullName).then((ok) => {
      // stats the number of messages and consumers every 30 seconds
      this.stats('increment', this.name, 'pending', this.pending);
      this.stats('increment', this.name, 'consumers', ok.consumerCount);
      this.stats('increment', this.name, 'messages', ok.messageCount);
      this.emit('check', {pendingCount: this.pending, consumerCount: ok.consumerCount, messageCount: ok.messageCount});

      return {
        connected: this.connected,
        pendingCount: this.pending,
        consumerCount: ok.consumerCount,
        messageCount: ok.messageCount
      };
    });
  }

  publish (body, options) {
    var job = new Job(this.name, this);
    return job.publish(body, options);
  }

  request (body, options) {
    var job = new Job(this.name, this);
    return job.request(body, options);
  }

  processJob (message) {
    if (!message) {
      this.log.warn({queue: this.name}, 'Empty job received');
      return;
    }
    this.log.trace('Queue.processJob() received a Job', message.properties.messageId, message.content.toString());
    this.pending++;
    var job = new Job(this.name, this);
    return job.process(message);
  }

  onSuccess (message, result) {
    this.pending = Math.max(0, this.pending - 1);
    this.lastComplete = Date.now();
    this.lastSuccess = Date.now();
    this.stats('increment', this.name, 'ok', 1);
    this.options.onSuccess && this.options.onSuccess(message, result);
  }

  onPartFailure (message, err, result) {
    this.pending = Math.max(0, this.pending - 1);
    this.lastComplete = Date.now();
    this.stats('increment', this.name, 'part_fail', 1);
    this.options.onPartFailure && this.options.onPartFailure(message, err, result);
  }

  onFullFailure (message, err, result) {
    this.pending = Math.max(0, this.pending - 1);
    this.lastComplete = Date.now();
    this.stats('increment', this.name, 'part_fail', 1);
    this.options.onFullFailure && this.options.onFullFailure(message, err, result);
  }
}
