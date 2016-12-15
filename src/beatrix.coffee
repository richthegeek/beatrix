try require('source-map-support').install()

Connection = require('./connection')
Queue = require('./queue')
Job = require('./job')
Timeout = require('callback-timeout')
_ = require 'lodash'

module.exports = (options, cb) ->
  connection = new Connection(options)
  connection.connect (err) ->
    if err
      return cb? err

    for name, queueopts of options.queues when options.enabled isnt false
      connection.createQueue(name, queueopts)
    
    return cb? null, connection

  return connection

module.exports.Connection = Connection
module.exports.Queue = Queue
module.exports.Job = Job
