// Software License Agreement (ISC License)
//
// Copyright (c) 2023, Matthew Voss
//
// Permission to use, copy, modify, and/or distribute this software for
// any purpose with or without fee is hereby granted, provided that the
// above copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

var test = require('test-kit').tape()
var jpath = require('./jpath')

test('jpath set_value', function (t) {
  t.table_assert([
    ['parent', 'path', 'val', 'exp'],
    [null, '0', 'x', ['x']],
    [null, 'a', 'x', { a: 'x' }],
    [null, 'a/b/c', 'x', { a: { b: { c: 'x' } } }],
    [null, '0/0/0', 'x', [[['x']]]],
    [['x'], '0', 'x', ['x']],
    [['x', 'y'], '1', 'y', ['x', 'y']],
    [[{ a: 'x', b: 'y' }], '0/b', 'y', [{ a: 'x', b: 'y' }]],
    [[{ a: 'x' }, { a: 'y' }], '1/a', 'y', [{ a: 'x' }, { a: 'y' }]],
    [[{ a: ['x'] }], '0/a/0', 'x', [{ a: ['x'] }]],
    [[{ a: ['x', 'y'] }], '0/a/1', 'y', [{ a: ['x', 'y'] }]],
    [{ a: ['x', 'y'] }, 'a/1', 'y', { a: ['x', 'y'] }],],
    jpath.set_value
  )
})

test('jpath errors', function (t) {
  t.table_assert([
    ['parent', 'path', 'val', 'exp'],
    [{ a: 33 }, 'a/b/c', 'x', /expected parent of b in \"a\/b\/c\" to be type object, but got number \(33\)/],
    [[['x']], '0/a', 'x', /expected parent of a in \"0\/a\" to be type object/],
    [{ a: { b: 'x' } }, 'a/0', 'x', /expected parent of 0 in \"a\/0\" to be type array/],
  ],
    jpath.set_value,
    { assert: 'throws' },
  )
})

const fs = require('node:fs')
const Readable = require('node:stream').Readable
const jstream = require('.')
const process = require('process')

const sample_hal = `{
  "log": {
    "version": "1.2",
    "pages": [
      {
        "startedDateTime": "2023-05-23T23:44:27.587Z",
        "id": "page_2"
      }
    ],
    "entries": [
      {
        "pageref": "page_2",
        "request": {
          "method": "POST",
          "headers": [
            {
              "name": "user-agent",
              "value": {}
            },
            {
              "name": "users",
              "value": []
            }
          ],
          "queryString": [
            {
              "name": "done",
              "value": 0
            },
            {
              "name": "scrumb",
              "value": "t2351"
            }
          ],
          "cookies": [
            {
              "name": "AS",
              "path": "/",
              "secure": true
            }
        ]
        }
    }
    ]
  }
}`

function readStream(src) {
  const ret = new Readable()
  ret.push(sample_hal)    
  ret.push(null)     
  return ret
}

const exp_types = [
  "0/log/version:string",
  "0/log/pages/0/startedDateTime:string",
  "0/log/pages/0/id:string",
  "0/log/entries/0/pageref:string",
  "0/log/entries/0/request/method:string",
  "0/log/entries/0/request/headers/0/name:string",
  "0/log/entries/0/request/headers/0/value:object",
  "0/log/entries/0/request/headers/1/name:string",
  "0/log/entries/0/request/headers/1/value:array",
  "0/log/entries/0/request/queryString/0/name:string",
  "0/log/entries/0/request/queryString/0/value:number",
  "0/log/entries/0/request/queryString/1/name:string",
  "0/log/entries/0/request/queryString/1/value:string",
  "0/log/entries/0/request/cookies/0/name:string",
  "0/log/entries/0/request/cookies/0/path:string",
  "0/log/entries/0/request/cookies/0/secure:true",
  "",
].join('\n')

test('stream leaves2types', function (t) {
  readStream(sample_hal)
    .pipe(jstream.json2obj())
    .pipe2obj(jstream.leaves2type())
    .pipe(jstream.collect((lines) => {
      t.same(lines.join(''), exp_types)
      t.end()
    }))
})

test('stream leaves2objects', function (t) {
  readStream(sample_hal)
  .pipe(jstream.json2obj())
  .pipe2bytes(jstream.leaves2json())
  .pipe(jstream.nljson2obj())
  .pipe(jstream.pathobj2obj('0/log/entries/0/request/queryString/\\d+'))
  .pipe(jstream.collect((objs) => {
    t.same(objs, [ 
      { _obj_path: '0/log/entries/0/request/queryString/0', name: 'done', value: 0 }, 
      { _obj_path: '0/log/entries/0/request/queryString/1', name: 'scrumb', value: 't2351' } 
    ])
    t.end()
  }))
})


test('stream filterLeaves', function (t) {
  readStream(sample_hal)
  .pipe(jstream.json2obj())
  .pipe2obj(jstream.filterLeaves({include: ['0/log/entries/0/request/queryString/1']}))
  .pipe2bytes(jstream.leaves2json())
  .pipe(jstream.nljson2obj())
  .pipe(jstream.pathobj2obj('0/log/entries/0/request/queryString/\\d+'))
  .pipe(jstream.collect((objs) => {
    t.same(objs, [ 
      { _obj_path: '0/log/entries/0/request/queryString/1', name: 'scrumb', value: 't2351' } 
    ])
    t.end()
  }))
})


test('stream stats', function (t) {
  let stats = jstream.objstats_trans()
  readStream(sample_hal)
  .pipe(jstream.json2obj())
  .pipe2obj(jstream.filterLeaves({include: ['0/log/entries/0/request/queryString/1']}))
  .pipe2bytes(jstream.leaves2json())
  .pipe(jstream.nljson2obj())
  .pipe(jstream.pathobj2obj('0/log/entries/0/request/queryString/\\d+'))
  .pipe(stats)
  .pipe(jstream.collect((objs) => {
    t.same(objs, [ 
      { _obj_path: '0/log/entries/0/request/queryString/1', name: 'scrumb', value: 't2351' } 
    ])
    t.same(stats.stats.count, 1)
    t.end()
  }))
})

test('stream obj2json', function (t) {
  readStream(sample_hal)
  .pipe(jstream.json2obj())
  .pipe2obj(jstream.filterLeaves({include: ['0/log/entries/0/request/queryString/1']}))
  .pipe2bytes(jstream.leaves2json())
  .pipe(jstream.nljson2obj())
  .pipe(jstream.obj2json())
  .pipe(jstream.nljson2obj())
  .pipe(jstream.collect((objs) => {
    t.same(objs, [ 
      { '0/log/entries/0/request/queryString/1/name': 'scrumb' }, 
      { '0/log/entries/0/request/queryString/1/value': 't2351' } 
    ])
    t.end()
  }))
})

test('errors', function (t) {
  t.throws(() => jstream.json2obj().pipe2bytes({}), /expected trans.obj2bytes to be a function/)
  t.throws(() => jstream.json2obj().pipe2obj({}), /expected trans.obj2obj to be a function/)
  t.throws(() => jstream.json2obj().pipe2bytes(jstream.leaves2json()).pipe2bytes({}), /transform is already set/)
  t.end()
})

