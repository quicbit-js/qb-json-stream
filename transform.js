const jtokenizer = require('qb-json-tokenizer')
const { Transform, Writable } = require('node:stream')
const jpath = require('./jpath')
const TOK = jtokenizer.TOK

function err(msg) {
  throw Error(msg)
}

/**
 * Transform stream of bytes to objects.
 */
class Json2Object extends Transform {
  constructor(opt) {
    super(Object.assign({}, opt, { readableObjectMode: true }))
    this.tokenizer = jtokenizer.create()
    this.obj2obj_transforms = []
    this.obj2bytes_transform = null
  }

  _transform(chunk, _enc, cb) {
    this.tokenizer.leaves(chunk, (tokenizer) => {
      if (tokenizer === null) {
        cb()
        return
      }
      let v = tokenizer
      for (let o2o of this.obj2obj_transforms) {
        v = o2o.obj2obj(v)
        if (v === null) {
          return
        }
      }
      if (this.obj2bytes_transform) {
        this.obj2bytes_transform.obj2bytes(v, this)
      } else {
        this.push(v)        // assuming v at this point is a string or buffer object
      }
    })
  }

  pipe2obj(trans) {
    typeof trans.obj2obj === 'function' || err(`expected trans.obj2obj to be a function, not "${typeof trans.obj2obj}"`)
    this.obj2obj_transforms.push(trans)
    return this
  }

  pipe2bytes(trans) {
    typeof trans.obj2bytes === 'function' || err(`expected trans.obj2bytes to be a function, not "${typeof trans.obj2bytes}"`)
    this.obj2bytes_transform = trans
    return this
  }
}

function wildcard_re(s) {
  s = s.replace(/[-[\]{}()+?.,\\^$|#\s]/g, '\\$&');  // escape everything except '*'
  s = s.replace(/[*]/g, '.*')
  return new RegExp(s)
}

function hasmatch(expressions, s) {
  for (expr of expressions) {
    if (expr.test(s)) {
      return true
    }
  }
  return false
}

/**
 * Synchronous filter for working with stateful json tokenizer. Stateful tokenizer creates no objects, but
 * requires synchronous usage (not standard pipes)
 */
class FilterLeaves {
  constructor(opt = {}) {
    this.include = (opt.include || []).map((s) => wildcard_re(s))
    this.exclude = (opt.exclude || []).map((s) => wildcard_re(s))
    this.maxdepth = opt.maxdepth || 0
  }

  /**
   * Called repeatedly with tokenizer object
   */
  obj2obj(tokenizer) {
    if (this.maxdepth && tokenizer.depth >= this.maxdepth) {
      return null
    }
    const pathstr = tokenizer.path.join('/').replace(/:/g, ".") // ensure no colon before value
    const path_and_type = `${pathstr}:${tokenizer.typestr(false)}`
    if (
      (this.include.length && !hasmatch(this.include, path_and_type)) ||
      (this.exclude.length && hasmatch(this.exclude, path_and_type))
    ) {
      return null
    }
    return tokenizer
  }
}

/**
 * Convert JSON leaf callbacks to path:value pairs.
 */
class Leaves2Json {
  obj2bytes(tokenizer, bytestream) {
    let tok = tokenizer.tok
    let ps = tokenizer.ps
    let v
    if (ps.voff >= ps.vlim) {
      bytestream.push(null)
      return
    }
    if (tok === TOK.ARR_END || tok === TOK.OBJ_END) {
      v = tok === TOK.ARR_END ? '[]' : '{}'
    }
    bytestream.push(`{ "${tokenizer.path.join('/')}:`)
    bytestream.push(v)
    bytestream.push(' }\n')
  }
}


/**
 * Convert callbacks from json2leaves into path:type strings. Handy for type analysis.
 */
class Leaves2Type {
  obj2obj(tokenizer) {
    return `${tokenizer.path.join('/')}:${tokenizer.typestr(false)}\n`
  }
}

class NL2Obj extends Transform {
  constructor(parsejson, opt) {
    super(Object.assign({}, opt, { readableObjectMode: true }))
    this.parsejson = parsejson
  }

  _transform(chunk, _enc, cb) {
    if (chunk === null) {
      this.push(null)
      cb()
      return
    }
    let chunkstr = chunk.toString()
    if (this.pending != null) {
      chunkstr = this.pending + chunkstr
    }
    let lines = chunkstr.split(/\r?\n/)
    this.pending = lines.pop()
    for (let ln of lines) {
      if (this.parsejson) {
        this._push_as_object(ln.trim())
      } else {
        this.push(ln)
      }
    }
    cb()
  }
  _flush(cb) {
    if (this.parsejson) {
      this._push_as_object(this.pending.trim())
    } else {
      this.push(this.pending)
    }
    this.end()
    cb()
  }
  _push_as_object(s) {
    if (!s) {
      // skip empty strings
      return
    }
    let obj
    try {
      obj = JSON.parse(s)
    } catch (e) {
      obj = `JSON ERROR: ${e}`
    }
    this.push(obj)
  }
}

class ObjStats {
  constructor() {
    let t0 = Date.now()
    this.count = 0
    this.first_time = t0
    this.last_time = t0
    this.max_interval = 0
  }
  get total_time() {
    return this.last_time - this.first_time
  }
  get average_interval() {
    if (this.count === 0) {
      return 0
    }
    return this.total_time / this.count
  }
  update(obj) {
    let now = Date.now()
    if (now - this.last_time > this.max_interval) {
      this.max_interval = now - this.last_time
    }
    this.last_time = now
    this.count = this.count + 1
  }
  report() {
    let ret = {}
    ret.count = this.count
    ret.max_interval = this.max_interval + ' ms'
    ret.total_time = this.total_time + ' ms'
    ret.average_interval = this.average_interval.toFixed(3) + ' ms'
    return ret
  }
}

function objstats_trans(opt = {}) {
  let write_when_done = opt.write_when_done === undefined ? true : !!opt.write_when_done
  let stats = new ObjStats()
  ret = new Transform({
    readableObjectMode: true,
    writableObjectMode: true,
    readableHighWaterMark: 1,
    writableHighWaterMark: 1,
    transform(obj, _enc, cb) {
      stats.update(obj)
      cb(null, obj)
    }
  })
  ret.stats = stats
  if (write_when_done) {
    ret.on('end', () => process.stderr.write(`stream ended. stats: ${JSON.stringify(stats.report(), null, '  ')}\n`))
  }
  return ret
}

function objstats_write(opt = {}) {
  let write_when_done = opt.write_when_done === undefined ? true : !!opt.write_when_done
  let stats = new ObjStats()
  ret = new Transform({
    objectMode: true,
    highWaterMark: true,
    write(obj, _enc, cb) {
      stats.update(obj)
      cb(null, obj)
    }
  })
  ret.stats = stats
  if (write_when_done) {
    ret.on('finish', () => process.stderr.write(`stream ended. stats: ${JSON.stringify(stats.report(), null, '  ')}\n`))
  }
  return ret
}

function obj2json() {
  return Transform({
    writableObjectMode: true,
    writableHighWaterMark: 1,
    transform(obj, _enc, cb) {
      this.push(JSON.stringify(obj))
      this.push('\n')
      cb()
    }
  })
}

class Path2Obj extends Transform {
  constructor(prefix_expr) {
    super({ readableObjectMode: true, readableHighWaterMark: 1 })
    this.prefix_expr = new RegExp(`^(${prefix_expr})`)
    this.cur = null     // object or array being constructed
  }
  _transform(chunk, _enc, cb) {
    let [pathstr, val] = chunk.toString().split(/:(.*)/)
    val = JSON.parse(val)
    let m = pathstr.match(this.prefix_expr)
    if (!m) {
      this._flush(cb)
      return
    }
    let obj_path = m[1]
    pathstr = pathstr.substr(obj_path.length + 1)
    if (this.cur && obj_path !== this.cur._obj_path) {
      this._flush() // no cb()
    }
    this.cur = jpath.set_value(this.cur || { _obj_path: obj_path }, pathstr, val)
    cb()
  }
  _flush(cb) {
    if (this.cur != null) {
      // todo: move this filter logic out
      this.push(this.cur)
      this.cur = null
    }
    cb && cb()
  }
}

module.exports = {
  json2obj: (opt) => new Json2Object(opt),        // bytes in, object out with following subfilter options:

  filterLeaves: (opt) => new FilterLeaves(opt),           // tokenizer in, tokenizer out
  leaves2type: (opt) => new Leaves2Type(opt),             // tokenizer in, string out
  leaves2json: (opt) => new Leaves2Json(opt),             // tokenizer in, buffer out (NLJSON)
  path2obj: (prefix_expr) => new Path2Obj(prefix_expr),   // buffer in (NLJSON), object out

  // generic streams
  objstats_trans: objstats_trans,                 // object in, object out
  objstats_write: objstats_write,                 // object in
  obj2json: obj2json,                             // object in, NLJSON out
  splitlines: (opt) => NL2Obj(false, opt),        // buffer in (NL), string out
  nljson2obj: (opt) => NL2Obj(true, opt),         // buffer in (NLJSON), object out
}