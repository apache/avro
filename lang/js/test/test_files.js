/* jshint node: true, mocha: true */

/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

'use strict';

var files = require('../lib/files'),
    protocols = require('../lib/protocols'),
    schemas = require('../lib/schemas'),
    assert = require('assert'),
    fs = require('fs'),
    path = require('path'),
    tmp = require('tmp');

var DPATH = path.join(__dirname, 'dat');
var Header = files.HEADER_TYPE.getRecordConstructor();
var MAGIC_BYTES = files.MAGIC_BYTES;
var SYNC = Buffer.from('atokensyncheader');
var createType = schemas.createType;
var streams = files.streams;
var types = schemas.types;


describe('files', function () {

  describe('parse', function () {

    var parse = files.parse;

    it('type object', function () {
      var obj = {
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      };
      assert(parse(obj) instanceof types.RecordType);
    });

    it('protocol object', function () {
      var obj = {protocol: 'Foo'};
      assert(parse(obj) instanceof protocols.Protocol);
    });

    it('schema instance', function () {
      var type = parse({
        type: 'record',
        name: 'Person',
        fields: [{name: 'so', type: 'Person'}]
      });
      assert.strictEqual(parse(type), type);
    });

    it('stringified schema', function () {
      assert(parse('"int"') instanceof types.IntType);
    });

    it('type name', function () {
      assert(parse('double') instanceof types.DoubleType);
    });

    it('file', function () {
      var t1 = parse({type: 'fixed', name: 'id.Id', size: 64});
      var t2 = parse(path.join(__dirname, 'dat', 'Id.avsc'));
      assert.deepEqual(JSON.stringify(t1), JSON.stringify(t2));
    });

  });

  describe('RawEncoder', function () {

    var RawEncoder = streams.RawEncoder;

    it('flush once', function (cb) {
      var t = createType('int');
      var buf;
      var encoder = new RawEncoder(t)
        .on('data', function (chunk) {
          assert.strictEqual(buf, undefined);
          buf = chunk;
        })
        .on('end', function () {
          assert.deepEqual(buf, Buffer.from([2, 0, 3]));
          cb();
        });
      encoder.write(1);
      encoder.write(0);
      encoder.end(-2);
    });

    it('write multiple', function (cb) {
      var t = createType('int');
      var bufs = [];
      var encoder = new RawEncoder(t, {batchSize: 1})
        .on('data', function (chunk) {
          bufs.push(chunk);
        })
        .on('end', function () {
          assert.deepEqual(bufs, [Buffer.from([1]), Buffer.from([2])]);
          cb();
        });
      encoder.write(-1);
      encoder.end(1);
    });

    it('resize', function (cb) {
      var t = createType({type: 'fixed', name: 'A', size: 2});
      var data = Buffer.from([48, 18]);
      var buf;
      var encoder = new RawEncoder(t, {batchSize: 1})
        .on('data', function (chunk) {
          assert.strictEqual(buf, undefined);
          buf = chunk;
        })
        .on('end', function () {
          assert.deepEqual(buf, data);
          cb();
        });
      encoder.write(data);
      encoder.end();
    });

    it('flush when full', function (cb) {
      var t = createType({type: 'fixed', name: 'A', size: 2});
      var data = Buffer.from([48, 18]);
      var chunks = [];
      var encoder = new RawEncoder(t, {batchSize: 2})
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [data, data]);
          cb();
        });
      encoder.write(data);
      encoder.write(data);
      encoder.end();
    });

    it('empty', function (cb) {
      var t = createType('int');
      var chunks = [];
      var encoder = new RawEncoder(t, {batchSize: 2})
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, []);
          cb();
        });
      encoder.end();
    });

    it('missing writer type', function () {
      assert.throws(function () { new RawEncoder(); });
    });

    it('writer type from schema', function () {
      var encoder = new RawEncoder('int');
      assert(encoder._type instanceof types.IntType);
    });

    it('invalid object', function (cb) {
      var t = createType('int');
      var encoder = new RawEncoder(t)
        .on('error', function () { cb(); });
      encoder.write('hi');
    });

  });

  describe('RawDecoder', function () {

    var RawDecoder = streams.RawDecoder;

    it('single item', function (cb) {
      var t = createType('int');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [0]);
          cb();
        });
      decoder.end(Buffer.from([0]));
    });

    it('no writer type', function () {
      assert.throws(function () { new RawDecoder(); });
    });

    it('decoding', function (cb) {
      var t = createType('int');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [1, 2]);
          cb();
        });
      decoder.write(Buffer.from([2]));
      decoder.end(Buffer.from([4]));
    });

    it('no decoding', function (cb) {
      var t = createType('int');
      var bufs = [Buffer.from([3]), Buffer.from([124])];
      var objs = [];
      var decoder = new RawDecoder(t, {decode: false})
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, bufs);
          cb();
        });
      decoder.write(bufs[0]);
      decoder.end(bufs[1]);
    });

    it('write partial', function (cb) {
      var t = createType('bytes');
      var objs = [];
      var decoder = new RawDecoder(t)
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [Buffer.from([6])]);
          cb();
        });
      decoder.write(Buffer.from([2]));
      // Let the first read go through (and return null).
      process.nextTick(function () { decoder.end(Buffer.from([6])); });
    });

  });

  describe('BlockEncoder', function () {

    var BlockEncoder = streams.BlockEncoder;

    it('invalid type', function () {
      assert.throws(function () { new BlockEncoder(); });
    });

    it('invalid codec', function (cb) {
      var t = createType('int');
      var encoder = new BlockEncoder(t, {codec: 'foo'})
        .on('error', function () { cb(); });
      encoder.write(2);
    });

    it('invalid object', function (cb) {
      var t = createType('int');
      var encoder = new BlockEncoder(t)
        .on('error', function () { cb(); });
      encoder.write('hi');
    });

    it('empty', function (cb) {
      var t = createType('int');
      var chunks = [];
      var encoder = new BlockEncoder(t)
        .on('data', function (chunk) { chunks.push(chunk); })
        .on('finish', function () {
          assert.equal(chunks.length, 0);
          cb();
        });
      encoder.end();
    });

    it('flush on finish', function (cb) {
      var t = createType('int');
      var chunks = [];
      var encoder = new BlockEncoder(t, {
        omitHeader: true,
        syncMarker: SYNC
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [
            Buffer.from([6]),
            Buffer.from([6]),
            Buffer.from([24, 0, 8]),
            SYNC
          ]);
          cb();
        });
      encoder.write(12);
      encoder.write(0);
      encoder.end(4);
    });

    it('flush when full', function (cb) {
      var chunks = [];
      var encoder = new BlockEncoder(createType('int'), {
        omitHeader: true,
        syncMarker: SYNC,
        blockSize: 2
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(
            chunks,
            [
              Buffer.from([2]), Buffer.from([2]), Buffer.from([2]), SYNC,
              Buffer.from([2]), Buffer.from([4]), Buffer.from([128, 1]), SYNC
            ]
          );
          cb();
        });
      encoder.write(1);
      encoder.end(64);
    });

    it('resize', function (cb) {
      var t = createType({type: 'fixed', size: 8, name: 'Eight'});
      var buf = Buffer.from('abcdefgh');
      var chunks = [];
      var encoder = new BlockEncoder(t, {
        omitHeader: true,
        syncMarker: SYNC,
        blockSize: 4
      }).on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          var b1 = Buffer.from([4]);
          var b2 = Buffer.from([32]);
          assert.deepEqual(chunks, [b1, b2, Buffer.concat([buf, buf]), SYNC]);
          cb();
        });
      encoder.write(buf);
      encoder.end(buf);
    });

    it('compression error', function (cb) {
      var t = createType('int');
      var codecs = {
        invalid: function (data, cb) { cb(new Error('ouch')); }
      };
      var encoder = new BlockEncoder(t, {codec: 'invalid', codecs: codecs})
        .on('error', function () { cb(); });
      encoder.end(12);
    });

    it('write non-canonical schema', function (cb) {
      var obj = {type: 'fixed', size: 2, name: 'Id', doc: 'An id.'};
      var id = Buffer.from([1, 2]);
      var ids = [];
      var encoder = new BlockEncoder(obj);
      var decoder = new streams.BlockDecoder()
        .on('metadata', function (type, codec, header) {
          var schema = JSON.parse(header.meta['avro.schema'].toString());
          assert.deepEqual(schema, obj); // Check that doc field not stripped.
        })
        .on('data', function (id) { ids.push(id); })
        .on('end', function () {
          assert.deepEqual(ids, [id]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(id);
    });

  });

  describe('BlockDecoder', function () {

    var BlockDecoder = streams.BlockDecoder;

    it('invalid magic bytes', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      decoder.write(Buffer.from([0, 3, 2, 1])); // !== MAGIC_BYTES
      decoder.write(Buffer.from([0]));
      decoder.end(SYNC);
    });

    it('invalid sync marker', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': Buffer.from('"int"'),
          'avro.codec': Buffer.from('null')
        },
        SYNC
      );
      decoder.write(header.$toBuffer());
      decoder.write(Buffer.from([0, 0])); // Empty block.
      decoder.end(Buffer.from('alongerstringthansixteenbytes'));
    });

    it('missing codec', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('end', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {'avro.schema': Buffer.from('"int"')},
        SYNC
      );
      decoder.end(header.$toBuffer());
    });

    it('unknown codec', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': Buffer.from('"int"'),
          'avro.codec': Buffer.from('"foo"')
        },
        SYNC
      );
      decoder.end(header.$toBuffer());
    });

    it('invalid schema', function (cb) {
      var decoder = new BlockDecoder()
        .on('data', function () {})
        .on('error', function () { cb(); });
      var header = new Header(
        MAGIC_BYTES,
        {
          'avro.schema': Buffer.from('"int2"'),
          'avro.codec': Buffer.from('null')
        },
        SYNC
      );
      decoder.end(header.$toBuffer());
    });

  });

  describe('encode & decode', function () {

    it('uncompressed int', function (cb) {
      var t = createType('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder()
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [12, 23, 48]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.write(12);
      encoder.write(23);
      encoder.end(48);
    });

    it('uncompressed int non decoded', function (cb) {
      var t = createType('int');
      var objs = [];
      var encoder = new streams.BlockEncoder(t);
      var decoder = new streams.BlockDecoder({decode: false})
        .on('data', function (obj) { objs.push(obj); })
        .on('end', function () {
          assert.deepEqual(objs, [Buffer.from([96])]);
          cb();
        });
      encoder.pipe(decoder);
      encoder.end(48);
    });

    it('deflated records', function (cb) {
      var t = createType({
        type: 'record',
        name: 'Person',
        fields: [
          {name: 'name', type: 'string'},
          {name: 'age', type: 'int'}
        ]
      });
      var Person = t.getRecordConstructor();
      var p1 = [
        new Person('Ann', 23),
        new Person('Bob', 25)
      ];
      var p2 = [];
      var encoder = new streams.BlockEncoder(t, {codec: 'deflate'});
      var decoder = new streams.BlockDecoder()
        .on('data', function (obj) { p2.push(obj); })
        .on('end', function () {
          assert.deepEqual(p2, p1);
          cb();
        });
      encoder.pipe(decoder);
      var i, l;
      for (i = 0, l = p1.length; i < l; i++) {
        encoder.write(p1[i]);
      }
      encoder.end();
    });

    it('decompression error', function (cb) {
      var t = createType('int');
      var codecs = {
        'null': function (data, cb) { cb(new Error('ouch')); }
      };
      var encoder = new streams.BlockEncoder(t, {codec: 'null'});
      var decoder = new streams.BlockDecoder({codecs: codecs})
        .on('error', function () { cb(); });
      encoder.pipe(decoder);
      encoder.end(1);
    });

    it('decompression late read', function (cb) {
      var chunks = [];
      var encoder = new streams.BlockEncoder(createType('int'));
      var decoder = new streams.BlockDecoder();
      encoder.pipe(decoder);
      encoder.end(1);
      decoder.on('data', function (chunk) { chunks.push(chunk); })
        .on('end', function () {
          assert.deepEqual(chunks, [1]);
          cb();
        });
    });

  });

  it('createFileDecoder', function (cb) {
    var n = 0;
    var type = loadSchema(path.join(DPATH, 'Person.avsc'));
    files.createFileDecoder(path.join(DPATH, 'person-10.avro'))
      .on('metadata', function (writerType) {
        assert.equal(writerType.toString(), type.toString());
      })
      .on('data', function (obj) {
        n++;
        assert(type.isValid(obj));
      })
      .on('end', function () {
        assert.equal(n, 10);
        cb();
      });
  });

  it('createFileEncoder', function (cb) {
    var type = createType({
      type: 'record',
      name: 'Person',
      fields: [
        {name: 'name', type: 'string'},
        {name: 'age', type: 'int'}
      ]
    });
    var path = tmp.fileSync().name;
    var encoder = files.createFileEncoder(path, type);
    encoder.write({name: 'Ann', age: 32});
    encoder.end({name: 'Bob', age: 33});
    var n = 0;
    encoder.getDownstream().on('finish', function () {
      files.createFileDecoder(path)
        .on('data', function (obj) {
          n++;
          assert(type.isValid(obj));
        })
        .on('end', function () {
          assert.equal(n, 2);
          cb();
        });
    });
  });

  it('extractFileHeader', function () {
    var header;
    var fpath = path.join(DPATH, 'person-10.avro');
    header = files.extractFileHeader(fpath);
    assert(header !== null);
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = files.extractFileHeader(fpath, {decode: false});
    assert(Buffer.isBuffer(header.meta['avro.schema']));
    header = files.extractFileHeader(fpath, {size: 2});
    assert.equal(typeof header.meta['avro.schema'], 'object');
    header = files.extractFileHeader(path.join(DPATH, 'person-10.avro.raw'));
    assert(header === null);
    header = files.extractFileHeader(
      path.join(DPATH, 'person-10.no-codec.avro')
    );
    assert(header !== null);
  });

});

// Helpers.

function loadSchema(path) {
  return createType(JSON.parse(fs.readFileSync(path)));
}
