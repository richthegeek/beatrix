(function() {
  var TimeoutError,
    extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    hasProp = {}.hasOwnProperty;

  TimeoutError = (function(superClass) {
    extend(TimeoutError, superClass);

    function TimeoutError(timeout) {
      this.name = 'TimeoutError';
      this.code = 'ETIMEOUT';
      this.message = "timeout of " + timeout + "ms exceeded";
    }

    return TimeoutError;

  })(Error);

  module.exports = function(timeout, fn) {
    var callback;
    callback = function(e, r) {
      if (!callback.called) {
        fn(e, r);
        callback.called = true;
        return clearTimeout(callback.timer);
      }
    };
    if (timeout) {
      callback.timer = setTimeout(callback, timeout, new TimeoutError(timeout));
    }
    callback.called = false;
    return callback;
  };

}).call(this);

//# sourceMappingURL=callbackTimeout.js.map
