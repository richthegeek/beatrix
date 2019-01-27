const Connection = require('./connection');
const Queue = require('./queue');
const Job = require('./job');

const connections = new Map();

module.exports = (options) => {
  options = Connection.prototype.getOptions(options);
  const key = [options.uri, options.exchange.name].join('::')

  if (options.deduplicateConnection === false || !connections.has(key)) {
    connections.set(key, new Connection(options).connect());
  }

  return connections.get(key);
}

module.exports.Connection = Connection
module.exports.Queue = Queue
module.exports.Job = Job
