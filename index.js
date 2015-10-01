var levelup = require('levelup')
var sublevel = require('sublevelup')
var transaction = require('level-transactions')
var ginga = require('ginga')
var xtend = require('xtend')
var inherits = require('util').inherits
var EventEmitter = require('events').EventEmitter

function IndexUP (dir, opts) {
  if (!(this instanceof IndexUP)) return new IndexUP(dir, opts)

  var db = typeof dir === 'string'
    ? sublevel(levelup(dir, opts))
    : sublevel(dir, opts)

  this.options = db.options
  this.db = db

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

inherits(IndexUP, EventEmitter)
IndexUP.fn = ginga(IndexUP.prototype)

IndexUP.fn.transaction = function (opts) {
  return transaction(this.db, opts)
}

IndexUP.fn.define('get', params('key', 'options'), function (ctx, done) {
  ctx.options = xtend(this.options, ctx.options)
})

function pre (ctx, next) {
  ctx.options = xtend(this.options, ctx.options)
}

module.exports = IndexUP
