var Connection = require('./connection');
var Queue = require('./queue');
var Job = require('./job');

var connections = {};

module.exports = (options) => {
  options = Connection.prototype.getOptions(options);
  var key = [options.uri, options.exchange.name].join('::');

  if (options.deduplicateConnection !== false && connections[key]) {
    return connections[key];
  }

  return connections[key] = new Connection(options).connect();
}

module.exports.Connection = Connection
module.exports.Queue = Queue
module.exports.Job = Job
