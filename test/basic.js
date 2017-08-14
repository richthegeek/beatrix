process.title = 'beatrix-test';
require('./_connection');

var assert = require('assert');
var _ = require('lodash');

describe('Beatrix basic ', () => {

  it('should create a queue from the initial options defined in _connection.js', () => {
    assert(_.has(connection, 'queues.testcore'), 'Connection did not have the testcore queue');
    return connection.queues.testcore.status(true).then((result) => {
      assert.equal(result.connected, true, 'Queue.status(wait=true) response did not have connected=true');
      assert.equal(result.pendingCount, 0, 'Queue.status(wait=true) response did not have pendingCount=0');
      assert(_.isNumber(result.consumerCount), 'Queue.status(wait=true) response did not have numeric consumerCount');
      assert(_.isNumber(result.messageCount), 'Queue.status(wait=true) response did not have numeric messageCount');
    });
  });

  it('should be able to create a queue', () => {
    var name = 'test-basic-1-' + Date.now();

    var queue = connection.createQueue(name, {
      durable: false,
      autoDelete: true
    })

    return queue.status(true).then(() => {
      return queue.channel._channel.checkQueue('testing.' + name)
    }).then(() => {
      return queue.channel._channel.deleteQueue('testing.' + name)
    });
  });

  it('should be able to publish a job on the connection object with a queue pass-through', (done) => {
    connection.queues.testcore.on('publish', (job) => {
      assert(job.body.foo, 'Job did not have expected `body.foo` property');
      done();
    });
    connection.publish('testcore', {foo: true});
  });

  it('should be able to publish a job on the connection object directly', () => {
    return connection.publish('dontexist', {foo: true});
  });

  it('should be able to publish and run a job directly on the queue', (done) => {
    var name = 'test-basic-2-' + Date.now();

    var q = connection.createQueue(name, {
      durable: false,
      autoDelete: true,
      process: (message) => {
        assert.equal(message.body.test, 'foo');
        message.resolve('ok')
        done()
      }
    })

    q.publish({test: 'foo'}).then(() => {
      // do nothing really
    }, done);
  });

  it('should be able to request a job and receive a reply quickly', (done) => {
    var name = 'test-basic-3-' + Date.now();
    var time = Date.now();

    var q = connection.createQueue(name, {
      durable: false,
      autoDelete: true,
      process: (message) => {
        assert.equal(message.body.time, time);
        message.resolve(message.body.time)
      }
    })

    q.request({time}).then((xtime) => {
      assert.equal(time, xtime);
      done();
    }, (err) => {
      done(err);
    });
  });

  it('should reject a requested job with a thrown Error object', (done) => {
    var name = 'test-basic-4-' + Date.now();
    var time = Date.now();

    var q = connection.createQueue(name, {
      durable: false,
      autoDelete: true,
      process: (message) => {
        throw new Error('No thankyou');
      }
    })

    q.request({time}).then((xtime) => {
      done(new Error('test-basic-4 was resolved when it should not have been'));
    }, (err) => {
      assert(err instanceof Error, 'Rejected error was not an Error object');
      assert.equal(err.message, 'No thankyou', 'Rejected error message was not "No thankyou"');
      done();
    });
  });

});
