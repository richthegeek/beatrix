const Emitter = require('eventemitter2').EventEmitter2;
const _ = require('lodash');

class MockAmqpConnectionManager extends Emitter {
  constructor (urls, options) {
    super();
    this.urls = urls;
    this.options = options;
    this.connected = false;
    this.channels = [];
    this.exchanges = {};
    this.queues = {};
  }

  static connect (urls, options) {
    const manager = new MockAmqpConnectionManager(urls, options);
    manager.connect();
    return manager;
  }

  connect () {
    process.nextTick(() => {
      this.connected = true;
      this.emit('connect', {
        connection: this,
        url: this.urls[0]
      })
    });
    setImmediate(this.keepalive.bind(this));
  }

  keepalive () {
    if (this.connected) {
      setImmediate(this.keepalive.bind(this));
    }
  }

  createChannel (options) {
    const channel = new MockChannelWrapper(this, options);
    channel.connect();
    this.channels.push(channel);
    return channel;
  }

  isConnected () {
    return this.connected;
  }

  close () {
    this.connected = false;
    this.emit('close');
    this.channels.forEach((channel) => channel.close());
  }

  async assertExchange (exchange, type, options) {
    if (_.has(this.exchanges, exchange)) {
      if (!_.isEqual(this.exchanges[exchange], {type, options})) {
        throw new Error('Exchange assertion failed due to differing parameters');
      }
    } else {
      this.exchanges[exchange] = new MockExchange(exchange, type, options);
    }
    return this.exchanges[exchange];
  }

  async assertQueue (queue, options) {
    if (_.has(this.queues, queue)) {
      if (!_.isEqual(this.queues[queue], options)) {
        throw new Error('Queue assertion failed due to differing parameters');
      }
    } else {
      this.queues[queue] = new MockQueue(queue, options);
    }
    return this.queues[queue];
  }

  async bindQueue (queue, exchange, routingKey) {
    if (!_.get(this.exchanges, exchange)) {
      throw new Error(`Unknown exchange "${exchange}" in bindQueue call`);
    }
    if (!_.get(this.queues, queue)) {
      throw new Error(`Unknown queue "${queue}" in bindQueue call`);
    }
    return this.exchanges[exchange].bindQueue(this.queues[queue], routingKey);
  }

  async publish (exchange, routingKey, body, options) {
    if (!_.get(this.exchanges, exchange)) {
      throw new Error(`Unknown exchange "${exchange}" in publish call`);
    }
    let message = new MockMessage({
      exchange: exchange,
      routingKey: routingKey
    }, options, body);
    return this.exchanges[exchange].publish(routingKey, message);
  }
  
  async sendToQueue (queue, body, options) {
    if (!_.get(this.queues, queue)) {
      throw new Error(`Unknown queue "${queue}" in sendToQueue call`);
    }
    let message = new MockMessage({
      exchange: null,
      routingKey: queue
    }, options, body);
    return this.queues[queue].receive(message);
  }

  async consume (channel, queue, handler) {
    if (!_.get(this.queues, queue)) {
      throw new Error(`Unknown queue "${queue}" in consume call`);
    }    
    return this.queues[queue].consume(handler);    
  }
}

class MockChannelWrapper extends Emitter {
  constructor (connection, options) {
    super();
    this.connection = connection;
    this.options = options;
    this.connected = false;
    if (options.setup) {
      this.addSetup(options.setup);
    }
  }

  connect () {
    process.nextTick(() => {
      this.connected = true;
      this.emit('connect', {
        channel: this
      })
    });
  }

  close () {
    this.connected = false;
    this.emit('close');
  }

  isConnected () {
    return this.connected;
  }

  addSetup (fn) {
    if (this.isConnected()) {
      process.nextTick(() => fn(this));
    }
    this.on('connect', ({channel}) => fn(channel));
  }

  async publish (exchange, routingKey, body, options) {
    if (this.options.json) {
      body = JSON.stringify(body);
    }
    return this.connection.publish(exchange, routingKey, body, options);
  }
  
  async sendToQueue (queue, body, options) {
    if (this.options.json) {
      body = JSON.stringify(body);
    }
    return this.connection.sendToQueue(queue, body, options);
  }
  
  // TODO
  ack () {}
  nack () {}
  queueLength () { return 0; }

  async assertExchange (exchange, type, options) {
    return this.connection.assertExchange(exchange, type, options);
  }

  async assertQueue (queue, options) {
    return this.connection.assertQueue(queue, options);
  }

  async bindQueue (queue, exchange, routingKey) {
    return this.connection.bindQueue(queue, exchange, routingKey);
  }

  // currently does nothing
  async prefetch (concurrency) {
    return this.concurrency = concurrency;
  }
  
  async consume (queue, handler) {
    return this.connection.consume(this, queue, handler);
  }
}

class MockExchange {
  constructor (name, type, options) {
    this.name = name;
    this.type = type;
    this.options = options;

    this.bindings = [];
  }

  async bindQueue (queue, routingKey) {
    let regex = routingKey.replace(/\./, '\\.').replace(/\*/g, '[^\.]+').replace(/\#\./g, '.*')

    regex = new RegExp('^' + regex + '$', 'i');

    this.bindings.push({queue, routingKey, regex});
    return true;
  }
  
  async publish (routingKey, message) {
    // TODO: Support header/fanout exchanges.
    // This acts as just a 'direct' or 'topic' exchange
    for (let i = 0; i < this.bindings.length; i++) {
      let binding = this.bindings[i];
      if (binding.regex.test(routingKey)) {
        return binding.queue.receive(message)
      }
    }
  }
}

class MockQueue {

  constructor (name, options) {
    this.name = name;
    this.options = options;
  }

  // TODO : concurrency, nack
  receive (message) {
    this.handler(message);
  }

  consume (handler) {
    this.handler = handler;
  }

}

class MockMessage {
  constructor (fields, properties, content) {
    this.fields = _.defaults({}, fields, {
      consumerTag: 'amq.ctag-mock', /* actually unsure of what this field is for */
      deliveryTag: Number(_.uniqueId()),
      redelivered: false,
    });
    this.properties = _.defaults({}, properties, {
      contentType: undefined,
      contentEncoding: undefined,
      headers: {},
      deliveryMode: undefined,
      priority: undefined,
      correlationId: undefined,
      replyTo: undefined,
      expiration: undefined,
      messageId: undefined,
      timestamp: undefined,
      type: undefined,
      userId: undefined,
      appId: undefined,
      clusterId: undefined
    });
    this.content = Buffer.from(content)
  }
}

module.exports = MockAmqpConnectionManager;