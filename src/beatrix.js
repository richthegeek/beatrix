var Connection = require('./connection')
var Queue = require('./queue')
var Job = require('./job')

module.exports = (options) => {
  return new Connection(options).connect();
}

module.exports.Connection = Connection
module.exports.Queue = Queue
module.exports.Job = Job
