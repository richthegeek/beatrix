var Beatrix = require('../');
var _ = require('lodash');
var assert = require('assert');

before((done) => {
  if (global.connection) {
    return done();
  }

  global.connection = Beatrix({
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
  return Promise.all([_.map(global.connection.queues, (q) => {
    return q.delete();
  })].concat([
    global.connection.createChannel((channel) => {
      return channel.deleteExchange('testing');
    })
  ]));
});
