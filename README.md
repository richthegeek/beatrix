# Beatrix

An implentation of a job queue for the `delayed-message` RabbitMQ plugin.

Built on top of [amqp-connection-manager](https://github.com/benbria/node-amqp-connection-manager) and [amqplib](https://github.com/squaremo/amqp.node).

Changed in version 3 to be entirely promise-based.

Strongly recommended that you use Bunyan or similar library.

## Example Usage
```javascript
const Beatrix = require('beatrix')({
  uri: "amqp://user:password@amqphost.com/path",
  exchange: {
    name: "myapp"
  }
});

// Connects to URI and asserts a Delayed Message Exchange named myapp

Beatrix.createQueue('echo', {
  process: function (message) {
    message.log('Got a message to echo!', message.body.text);
    message.resolve(message.body.text);
  }
});

Beatrix.publish('echo', {text: 'hello'});

Beatrix.request('echo', {text: 'test'}).then((result) => {
  // result = "test"
});
```

## Connection
`var connection = Beatrix({...});`
`var connection = new Beatrix.Connection({...}).connect();`

### options
* `name` - (default: "beatrix") used as a default for the Exchange name, part of the Queue fullnames, and as part of the process response queue.
* `log` - (default: console) an object containing methods "info", "error", "fatal", "warn", "log" when most things happen.
* `stats` - (default: void) a function which recieves arguments `type, queue, stat, value`, such as `("timing", "beatrix.test", "run", 14)` for publishing to a metrics library such as StatsD.
* `uri`: (default: localhost) connection string, or array of connection strings to try to connect to.
* `exchange`: an object containing standard properties for the exchange such as:
* `exchange.name`: (default: .name)
* `exchange.type`: (default: x-delayed-message)
* `exchange.arguments.x-delayed-type`: (default: direct)
* `responseQueue`: an object defining how the responseQueue is set up:
* `responseQueue.name`: defaults to a hopefully-unique string like "localhost.beatrix.81933.response.queue"
* `responseQueue.messageTtl`: (default: 10 minutes)
* `queues`: optional object containing queue definitions to immediately instatiate, see "Queue Options".
* `mock`: (default: false) wether to use a light mocking interface instead of a real RabbitMQ connection. Does not support acking/nacking or concurrency at this time.

### methods
* `on/once(event, callback)`: trigger the passed callback when this event occurs.
* `connect() -> this`: ensures there is a connection manager set up and binds passthroughs for certain events emmitted by the connection manager
* `isConnected() -> boolean`: returns true if there is an active connection to an AMQP server.
* `close() -> Promise(boolean[])`: attempts to close all channels and connections.
* `createChannel() -> Promise(channel)`: creates a new channel and asserts the exchange.
* `createQueue(name, options) -> Queue`: creates a new Queue on this exchange. See "Queue options".
* `assertQueue(name, options) -> Queue`: Idempotent version of createQueue.
* `send(method, routingKey, body, options) -> Promise(result)`: creates a new job and publishes it to the server when possible. Result type depends on method.
* `publish(routingKey, body, options) -> Promise(true)`: creates a new job and publishes it to the queue, resolving once received by the server.
* `request(routingKey, body, options) -> Promise(result)`: creates a new job and awaits the result from the processor before resolving.

### events
* `connect -> Connection` - emmitted every time the Connection associates, so may be multiple times per process.
* `disconnect -> Error(reason)` - emitted every time the Connection drops.
* `success`: passthrough of Queue/Job sucess event
* `partFailure`: passthrough of Queue/Job partFailure event
* `fullFailure`: passthrough of Queue/Job fullFailure event

## Queue
`Connection.createQueue(name, options) -> Queue`
`Connection.assertQueue(name, options) -> Queue`
### options
* `name` : name for logging and used as the routingKey unless overridden
* `fullName`: name of the queue within RabbitMQ, defaults to connection name + queue name
* `routingKey`: defaults to queue name, override if there are conflicts within the exchange
* `concurrency`: passed through as `prefetch`, essentially the max concurrent jobs on this Queue.
* `id`: initial incremented ID for messages if they dont have IDs on the body.
* `durable` and `autoDelete` as well as other normal AMQP Queue options can be added here.
* `bypass`: (default: false) if true and the local process has less running jobs than the Queue concurrency, then the job will be instantly run locally rather than publishing into RabbitMQ and then being retrieved.

### methods
* `on/once(event, callback)`: standard EventEmitter2 method
* `close()` - closes the Queue channel which should prevent it from consuming any more messages.
* `delete()` - deletes the Queue from the AMQP server
* `status(false)` - if connected, returns an object describing the Queue status. Otherwise returns false
* `status(true)` - waits until the Queue is connected before returning the Queue status.
* `publish(body, options) -> Promise(true)` - publishes a Job to this Queue.
* `request(body, options) -> Promise(result)` - requests a Job on this Queue and awaits the result.

### events
* `connect -> Queue` - emitted every time the channel connects/reconnects
* `disconnect -> Error(reason)` - emitted if an error occurs setting up the channel
* `drop -> {message, err}` - called when a message was dropped because it could not be JSON-encoded
* `close` - emitted when the Queue is closed manually
* `setup -> {queue, consumerCount, messageCount, consumerTag}` - emits whenever the channel connects and the Queue is set up successfully
* `check -> {pendingCount, consumerCount, messageCount}` - emitted every 15 seconds with basic Queue size information
* `publish -> {options, body}` - emitted whenever a Job is published and accepted by the server
* `success -> {message, result}` - emitted whenever Job is processed successfully
* `partFailure -> {message, error}` - emitted when a Job fails but will be retried
* `fullFailure -> {message, error}` - emitted when a Job fails and will not be retried

## Job
### Publishing options
* `maxAttempts` (default: 1) - how many times should this job be retried before failing?
* `timeout` (default: null) - how long in ms should we allow the processor to take before rejecting with a TimeoutError and potentially retrying?
* `replyTimeout` (default: 5000) - how long should the requester wait before rejecting with a TimeoutError? Note, the job may successfully resolve after this.
* `initialDelay` (default: 0) - should the first attempt be delayed?
* `delay` (default: 1000) - how many milliseconds should we delay between attempts?
* `delayStrategy` (default: Exponential) - see [backoff-strategies](https://github.com/richthegeek/node-backoff-strategies) for available strategies.
* `maxDelay` (default: 86400000 (1 day)) - what is the longest between attempts, overriding any other delay results?
* `bunyan` (default: null) - if passed a Bunyan instance, will attempt to copy over fields to message.log logger
* `exchange` (default: parent) - will override the exchange the job is being published to from the connection default

>**A note on delays**: by default it's Exponential at 1 second, so doubling every attempt: 1s, 2s, 4s, etc... So the 5 attempts will occur at least 31 seconds after publishing. Tuning the maxAttempts/delay/maxDelay properties is something you should probably spend a lot of time on. And delays won't work at all if you don't use the x-delayed-message plugin!
