(function() {
  var Connection, Job, Queue;

  try {
    require('source-map-support').install();
  } catch (error) {}

  Connection = require('./connection');

  Queue = require('./queue');

  Job = require('./job');

  module.exports = function(options, cb) {
    var connection;
    connection = new Connection(options);
    connection.connect(function(err) {
      var name, queueopts, ref;
      if (err) {
        return typeof cb === "function" ? cb(err) : void 0;
      }
      ref = options.queues;
      for (name in ref) {
        queueopts = ref[name];
        if (options.enabled !== false) {
          connection.createQueue(name, queueopts);
        }
      }
      return typeof cb === "function" ? cb(null, connection) : void 0;
    });
    return connection;
  };

  module.exports.Connection = Connection;

  module.exports.Queue = Queue;

  module.exports.Job = Job;

}).call(this);

//# sourceMappingURL=beatrix.js.map
