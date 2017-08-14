require('./_connection');

var assert = require('assert');
var _ = require('lodash');

describe('Delays', () => {

  it('should support initialDelay on the queue options', (done) => {
    var name = 'test-delay-1-' + Date.now();
    var time = Date.now();

    var q = connection.createQueue(name, {
      durable: false,
      autoDelete: true,
      initialDelay: 500,
      process: (message) => {
        assert(Date.now() - time >= 500, 'Delay was not at least 500ms');
        assert(Date.now() - time <= 600, 'Delay was more than 500ms');
        message.resolve('ok')
        done();
      }
    });

    q.publish({time}).then(_, done);
  });

  it('should support overriding initialDelay on the job options', (done) => {
    var name = 'test-delay-2-' + Date.now();
    var time = Date.now();

    var q = connection.createQueue(name, {
      durable: false,
      autoDelete: true,
      initialDelay: 100,
      process: (message) => {
        assert(Date.now() - time >= 500, 'Delay was not at least 500ms');
        assert(Date.now() - time <= 600, 'Delay was more than 500ms');
        message.resolve('ok')
        done();
      }
    });

    q.publish({time}, {initialDelay: 500}).then(_, done);
  });

});
