var Beatrix = require('../');
var _ = require('lodash');
var assert = require('assert');

before((done) => {
  if (global.connection) {
    return done();
  }

  var uri = process.env.RABBIT_URI
  if (!uri) {
    let user = process.env.RABBIT_USERNAME || 'guest';
    let pass = process.env.RABBIT_PASSWORD || 'guest';
    let host = process.env.RABBIT_HOST || '127.0.0.1';
    let port = process.env.RABBIT_PORT || '5672';
    let vhost = process.env.RABBIT_VPORT || ''
    uri = 'amqp://' + user + ':' + pass + '@' + host + ':' + port + '/' + vhost
  }

  global.connection = Beatrix({
    uri: uri,
    name: 'testing',
    queues: {
      testcore: {
        durable: false,
        autoDelete: true
      }
    }
  });
  global.connection.once('connect', () => done());
});

after(() => {
  return Promise.all(_.map(global.connection.queues, (q) => {
    return q.delete();
  }).concat([
    global.connection.createChannel((channel) => {
      return channel.deleteExchange('testing');
    })
  ])).catch((err) => { return null });
});
