class TimeoutError extends Error
  constructor: (timeout) ->
    @name = 'TimeoutError'
    @code = 'ETIMEOUT'
    @message = "timeout of #{timeout}ms exceeded"

module.exports = (timeout, fn) ->
  callback = (e, r) ->
    unless callback.called
      fn e, r
      clearTimeout(callback.timer)

  callback.timer = setTimeout callback, timeout, new TimeoutError timeout
  callback.called = false

  return callback