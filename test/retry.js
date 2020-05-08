require('./_connection');

var assert = require('assert');
var _ = require('lodash');

describe('Retries', () => {

  it('should support retrying a job twice with no delay', (done) => {
    var name = 'test-retry-1-' + Date.now();
    var time = Date.now();
    var i = 0;

    var q = connection.createQueue(name, {
      durable: false,
      autoDelete: true,
      maxAttempts: 2,
      delay: 0,
      process: (message) => {
        message.retry(false);
        assert(!_.has(message, 'properties.headers.x-delay'), 'message had an x-delay header');
        assert(message.attempt, i, 'message.attempt was not ' + i);
        if (i === 0) {
          assert(message.firstAttempt, 'message.firstAttempt was not true for the first attempt');
          assert(message.lastAttempt == false, 'message.lastAttempt was true for the first attempt');
          message.retry(true);
          message.reject('retry please');
        } else {
          assert(Date.now() - time < 150, 'message was delayed during processing unacceptably');
          assert(message.firstAttempt == false, 'message.firstAttempt was true for the last attempt');
          assert(message.lastAttempt, 'message.lastAttempt was not true for the last attempt');
          message.resolve('ok' + time);
        }
        i++;
      }
    });

    q.request({time}).then((result) => {
      assert.equal(result, 'ok' + time, 'Result was not received from second attempt');
      done();
    }, done);
  });

  it('should support retrying a job twice with a delay', (done) => {
    var name = 'test-retry-1-' + Date.now();
    var time = Date.now();
    var i = 0;

    var q = connection.createQueue(name, {
      durable: false,
      autoDelete: true,
      maxAttempts: 2,
      delay: 500,
      process: (message) => {
        if (i === 0) {
          message.reject('retry please');
        } else {
          assert(Date.now() - time >= 500, 'message attempt 2 was not delayed for at least 500ms');
          assert(Date.now() - time <= 1000, 'message attempt 2 was much longer than 500ms');
          message.resolve('ok' + time);
        }
        i++;
      }
    });

    q.request({time}).then((result) => {
      assert.equal(result, 'ok' + time, 'Result was not received from second attempt');
      done();
    }, done);
  });


});
