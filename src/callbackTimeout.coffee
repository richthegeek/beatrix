class TimeoutError extends Error
  constructor: (timeout) ->
    @name = 'TimeoutError'
    @code = 'ETIMEOUT'
    @message = "timeout of #{timeout}ms exceeded"

module.exports = (timeout, fn) ->
  callback = (e, r) ->
    unless callback.called
      fn e, r
      callback.called = true
      clearTimeout(callback.timer)

  if timeout
    callback.timer = setTimeout callback, timeout, new TimeoutError timeout

  callback.called = false
  return callback