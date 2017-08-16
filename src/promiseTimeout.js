'use strict';

class TimeoutError extends Error {
  constructor (timeout) {
    super()
    this.name = 'TimeoutError';
    this.code = 'ETIMEOUT';
    this.message = `timeout of ${timeout}ms exceeded`;
  }
}

module.exports = class TimeoutPromise extends Promise {
  constructor(ms, callback) {
    // We need to support being called with no milliseconds
    // value, because the various Promise methods (`then` and
    // such) correctly call the subclass constructor when
    // building the new promises they return.
    if ('function' === typeof ms) {
      return super(ms);
    }

    super((resolve, reject) => {
      callback(resolve, reject);
      ms > 0 && setTimeout(() => {
        reject(new TimeoutError(ms));
      }, ms);
    });
  }
}
