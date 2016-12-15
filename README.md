# Beatrix

An implementation of the Rabbot library for the `delayed-message` RabbitMQ plugin.

Heavily opiononated towards a specific style of use.

## Example usage
```javascript
Beatrix = require('beatrix');

var initOptions = {
  connection: {
    uri: "amqp://user:password@amqphost.com/path"
  },
  exchange: {
    name: "myexchange",
    autoDelete: true
  }
}

Beatrix(initOptions, function (err, connection) {
  if(err) {
    throw err
  }

  var queueOptions = {
    process: function (message, cb) {
      setTimeout(function () {
        cb(null, 'Done!')
      }, 1000)
    },
    maxAttempts: 3,
    delay: 500
  }

  connection.createQueue('testing', queueOptions, function (err, queue) {
    queue.request({foo: 'bar'}, {}, function (err, result) {
      console.log('result?', result)
    })
  });
});
```

## Initialisation options
`Beatrix(options, function (err, connection) {})`
* `connection`: passed directly to [Rabbot.addConnection](https://github.com/arobson/rabbot#addconnection--options-)
* `exchange`: supports `name`, `type`, and all options in [Rabbot.addExchange](https://github.com/arobson/rabbot#addexchange-exchangename-exchangetype-options-connectionname-)
* `queues`: a map of queues to create immediately.

## Connection properties and methods
* `options.log`: a standard Console object, override with Bunyan for example
* `options.stats(type, queue, stat, value)`: a method that recieves stats related to queues and job processing
* `options.onUhandled(message)`: a method that recieves and unhandled messages
* `partFailure(message)`: a method that recieves any messages which failed but are due to be retried
* `fullFailure(message)`: a method that recieves any messages which have failed for the last time

## Queue properties and methods
* `options.type`: override the default type value
* `options.timeout`: if specified, jobs which take longer will fail due to timeout
* `options.concurrency`: define how many concurrent jobs can be processed
* `options.id`: specify the base ID. This will be incremented as needed
* `lastPublish`: holds the timestamp of when the last message was published to this queue
* `lastComplete`: holds the timestamp of when the last message was processed in this queue
* `partFailure(message)`: a method that recieves any messages which failed but are due to be retried
* `fullFailure(message)`: a method that recieves any messages which have failed for the last time
