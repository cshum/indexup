var levelup = require('levelup')
var sublevel = require('sublevelup')
var transaction = require('level-transactions')
var bytewise = require('bytewise-core')
var ginga = require('ginga')
var xtend = require('xtend')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter

function IndexUP (dir, opts) {
  if (!(this instanceof IndexUP)) return new IndexUP(dir, opts)

  var db = typeof dir === 'string'
    ? sublevel(levelup(dir, opts))
    : sublevel.apply(null, arguments)

  this.options = db.options
  this.db = db
  this.store = db.sublevel('st')
  this.indexes = db.sublevel('ix')

  EventEmitter.call(this)
  this.setMaxListeners(Infinity)
}

// ginga params middleware factory
function params () {
  var names = Array.prototype.slice.call(arguments)
  var len = names.length
  return function (ctx) {
    var l = Math.min(ctx.args.length, len)
    for (var i = 0; i < l; i++) ctx[names[i]] = ctx.args[i]
  }
}

function encode (source) {
  return bytewise.encode(source).toString()
}

// function decode (source) {
//   return bytewise.decode(new Buffer(source, 'binary'))
// }

inherits(IndexUP, EventEmitter)
IndexUP.fn = ginga(IndexUP.prototype)

IndexUP.fn.transaction = function (opts) {
  return transaction(this.db, opts)
}

IndexUP.fn.define('get', params('key', 'options'), function (ctx, done) {
  ctx.options = xtend(this.options, ctx.options)
  var prefix = (ctx.options.index || '') + '!'
  var key = prefix + encode(ctx.key)

  this.indexes.get(key, function (err, doc) {
    if (err) return done(err)
    done(null, doc.value)
  })
})

function pre (ctx, next) {
  ctx.options = xtend(this.options, ctx.options)

  if (ctx.options.transaction) {
    ctx.tx = ctx.options.transaction
    // nested transaction
    ctx.root = false
    if (!(ctx.tx instanceof transaction)) {
      return next('Invalid transaction instance.')
    }
  } else {
    // root transaction
    ctx.root = true
    ctx.tx = transaction(this.store)
  }

  // rollback transaction if error
  ctx.on('end', function (err) {
    if (err) ctx.tx.rollback(err)
  })

  if (ctx.method === 'batch') {
    // batch
    if (!Array.isArray(ctx.batch)) {
      return next(new Error('batch must be Array.'))
    }
  } else {
    // put or del
    // key check
    if (
      ctx.key === '' ||
      ctx.key === null ||
      ctx.key === undefined
    ) return next(new Error('Key required.'))
  }
}

function current (ctx, next) {

}

function put (ctx, next) {

}

function del (ctx, next) {

}

function batch (ctx, next) {
  for (var i = 0, l = ctx.batch.length; i < l; i++) {
    var op = xtend(ctx.batch[i], {
      transaction: ctx.tx
    })
    if (op.type === 'put') {
      this.put(op.key, op.value, op)
    } else if (op.type === 'del') {
      this.del(op.key, op)
    } else {
      return next(new Error('Invalid batch operation'))
    }
  }
  next()
}

// commit index write
function write (ctx, done) {
  if (ctx.root) {
    ctx.tx.commit(done)
  } else {
    // no need commit if nested tx
    done()
  }
}

IndexUP.fn.define('del', params('key', 'options'), pre, current, del, write)
IndexUP.fn.define('put', params('key', 'value', 'options'), pre, current, put, write)
// IndexUP.fn.define('_index', params('key', 'options'), pre, del, put, write)
IndexUP.fn.define('batch', params('batch', 'options'), pre, batch, write)

IndexUP.fn.createReadStream =
IndexUP.fn.readStream = function (opts) {
}

IndexUP.fn.define('validate', params(
  'key', 'value', 'transaction'
), function (ctx, next) {

})

IndexUP.fn.define('merge', params(
  'key', 'value', 'current', 'transaction'
), function (ctx, next) {

})

IndexUP.fn.register = function (name, fn) {

}

IndexUP.fn.define('rebuild', params('tag'), function (ctx, next) {

})

module.exports = IndexUP
