'use strict';

var chai = require('chai');
var mongoose = require('mongoose');
var Promise = require('bluebird');

var TestHarness = require('../test_harness');

// Chai uses properties rather than methods for assertions.
/* eslint-disable no-unused-expressions */

describe('aggregate', function() {
  var expect = chai.expect;

  var id1 = new mongoose.Types.ObjectId();
  var id2 = new mongoose.Types.ObjectId();
  var id3 = new mongoose.Types.ObjectId();

  var fakeDatabase = {};
  var harness = new TestHarness({fakedb: fakeDatabase});
  var Item;

  function assertAggregationResults(input, operation, results, done) {
    fakeDatabase.items = input;

    Item.aggregate(
      operation,
      function(error, items) {
        if (error) return done(error);

        expect(items).to.deep.equal(results);
        done();
    });
  }

  function assertAggregationError(input, operation, code, message, done) {
    fakeDatabase.items = input;

    Item.aggregate(
      operation,
      function(error) {
        expect(error).to.have.property('ok', false);
        code && expect(error).to.have.property('code', code);
        expect(error)
          .to.have.property('message')
          .to.equal(message);
        done();
    });
  }

  function assertSumResults(input, results, done) {
    assertAggregationResults(
      input,
      [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
      results,
      done);
  }

  function assertComparisonResult(operator, lhs, rhs, result) {
    var operatorExpression = {};
    operatorExpression[operator] = ['$valueLhs', '$valueRhs'];

    return new Promise(function(resolve, reject) {
      assertAggregationResults(
        [{_id: id1, key: 'a', valueLhs: lhs, valueRhs: rhs}],
        [{'$group': {_id: '$key', result: {'$max': operatorExpression}}}],
        [{_id: 'a', result: result}],
        function(error) {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
    });
  }

  function assertStrictComparisonResult(operator, lhs, rhs, result) {
    return assertComparisonResult(operator, lhs, rhs, result)
      .then(function() {
        return assertComparisonResult(operator, rhs, lhs, !result);
      });
  }

  before(function(done) {
    harness.setUp(function(error) {
      Item = mongoose.connection.models.Item;
      done(error);
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  it('empty sequence returns all documents', function(done) {
    fakeDatabase.items = [
      {_id: id1, key: 'a', value: 1},
      {_id: id2, key: 'b', value: 1},
      {_id: id3, key: 'b', value: 2}
    ];
    Item.collection.aggregate([], function(error, items) {
      if (error) return done(error);

      expect(items).to.deep.equal(fakeDatabase.items);
      done();
    });
  });

  it('performs simple group operation', function(done) {
    assertSumResults(
      [
        {_id: id1, key: 'a', value: 1},
        {_id: id2, key: 'b', value: 1},
        {_id: id3, key: 'b', value: 2}
      ],
      [
        {_id: 'a', total: 1},
        {_id: 'b', total: 3}
      ],
      done);
  });

  describe('$group', function() {
    it('supports multiple groupings', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 'a', value: 1, value2: 10},
          {_id: id2, key: 'b', value: 1, value2: 15},
          {_id: id3, key: 'b', value: 2, value2: 25}
        ],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': '$value'},
            total2: {'$sum': '$value2'}
          }
        }],
        [
          {_id: 'a', total: 1, total2: 10},
          {_id: 'b', total: 3, total2: 40}
        ],
        done);
    });

    it('supports dates as _ids', function(done) {
      assertSumResults(
        [
          {_id: id1, key: new Date('1995-08-08'), value: 1},
          {_id: id2, key: new Date('1998-10-28'), value: 1},
          {_id: id3, key: new Date('1998-10-28'), value: 2}
        ],
        [
          {_id: new Date('1995-08-08'), total: 1},
          {_id: new Date('1998-10-28'), total: 3}
        ],
        done);
    });

    it('supports single stage pipeline as object', function(done) {
      assertSumResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: 1},
          {_id: id3, key: 'b', value: 2}
        ],
        [
          {_id: 'a', total: 1},
          {_id: 'b', total: 3}
        ],
        done);
    });

    it('chains stages in a pipeline', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 1, value: 1},
          {_id: id2, key: 1, value: 2},
          {_id: id3, key: 2, value: 2},
          {_id: id3, key: 3, value: 5}
        ],
        [
          {'$group': {_id: '$key', total: {'$sum': '$value'}}},
          {'$match': {total: {'$gt': 2}}}
        ],
        [
          {_id: 1, total: 3},
          {_id: 3, total: 5}
        ],
        done);
    });

    it('treats missing _id keys as nulls when grouping', function(done) {
      assertSumResults(
        [
          {_id: id1, key: 'a', value: 3},
          {_id: id3, value: 5}
        ],
        [
          {_id: 'a', total: 3},
          {_id: null, total: 5}
        ],
        done);
    });

    it('returns empty array given empty input', function(done) {
      assertSumResults([], [], done);
    });

    it('rejects non-objects as pipeline stages', function(done) {
      Item.collection.aggregate(
        [1],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 15942);
          expect(error)
            .to.have.property('message')
            .to.match(/exception: pipeline element 0 is not an object/);
          done();
      });
    });

    it('rejects multiple fields in pipeline stages', function(done) {
      Item.collection.aggregate(
        [{
          '$match': {a: 1, b: 1},
          '$group': {_id: '$a', total: {'$sum': '$b'}}
        }],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 16435);
          expect(error)
            .to.have.property('message')
            .to.have.string(
              'exception: A pipeline stage specification object ' +
              'must contain exactly one field.');
          done();
      });
    });

    it('rejects unknown pipeline stage names', function(done) {
      Item.collection.aggregate(
        [{a: 1}],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 16436);
          expect(error)
            .to.have.property('message')
            .to.have.string("exception: Unrecognized pipeline stage name: 'a'");
          done();
      });
    });

    it('rejects group specification without _id', function(done) {
      assertAggregationError(
        [],
        [{'$group': {total: {'$sum': 1}}}],
        15955,
        'exception: a group specification must include an _id',
        done);
    });

    it('rejects invalid group aggregate field expressions', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: 1}}],
        15951,
        "exception: the group aggregate field 'total' " +
        'must be defined as an expression inside an object',
        done);
    });

    it('rejects empty computed aggregate', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {}}}],
        15954,
        "exception: the computed aggregate 'total' " +
        'must specify exactly one operator',
        done);
    });

    it('rejects computed aggregate with multiple keys', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': '$a', '$max': '$a'}}}],
        15954,
        "exception: the computed aggregate 'total' " +
        'must specify exactly one operator',
        done);
    });

    it('rejects computed aggregate non-operator keys', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {x: '$b'}}}],
        15952,
        "exception: unknown group operator 'x'",
        done);
    });
  });

  it('rejects unknown operators', function(done) {
    assertAggregationError(
      [],
      [{'$group': {_id: '$a', total: {'$sum': {'$dummy': 42}}}}],
      15999,
      "exception: invalid operator '$dummy'",
      done);
  });

  it('rejects expressions not starting with operator', function(done) {
    assertAggregationError(
      [],
      [{'$group': {_id: '$a', total: {'$sum': {abc: 19}}}}],
      16420,
      'exception: field inclusion is not allowed inside of $expressions',
      done);
  });

  it('rejects extra fields in operator expressions', function(done) {
    assertAggregationError(
      [],
      [{
        '$group': {
          _id: '$a',
          total: {'$sum': {'$ifNull': ['$a', 42], abc: 19}}
      }}],
      15990,
      'exception: this object is already an operator expression, ' +
      "and can't be used as a document expression (at 'abc')",
      done);
  });

  it('rejects extra unknown operators in operator expressions', function(done) {
    assertAggregationError(
      [],
      [{
        '$group': {
          _id: '$a',
          total: {'$sum': {'$ifNull': ['$a', 42], '$abc': ['$c', 55]}}
      }}],
      15983,
      'exception: the operator must be the only field ' +
      "in a pipeline object (at '$abc'",
      done);
  });

  describe('$match', function() {
    it('filters documents', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: 3},
          {_id: id3, key: 'b', value: 4}
        ],
        [{'$match': {value: {'$gt': 2}}}],
        [
          {_id: id2, key: 'b', value: 3},
          {_id: id3, key: 'b', value: 4}
        ],
        done);
    });

    it('supports filtering on subdocuments', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: {x: 5}},
          {_id: id3, key: 'b', value: {x: 7}}
        ],
        [{'$match': {'value.x': 5}}],
        [{_id: id2, key: 'b', value: {x: 5}}],
        done);
    });

    it('reject invalid match queries', function(done) {
      assertAggregationError(
        fakeDatabase.items,
        [{'$match': {'$eq': 3}}],
        undefined,
        'BadValue unknown top level operator: $eq',
        done);
    });
  });

  describe('$sum', function() {
    it('sums values in input', function(done) {
      assertSumResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: 1},
          {_id: id3, key: 'b', value: 2}
        ],
        [
          {_id: 'a', total: 1},
          {_id: 'b', total: 3}
        ],
        done);
    });

    it('ignores nonexistent values', function(done) {
      assertSumResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: 5},
          {_id: id3, key: 'b'}
        ],
        [
          {_id: 'a', total: 1},
          {_id: 'b', total: 5}
        ],
        done);
    });

    it('ignores non-numeric values', function(done) {
      assertSumResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: 7},
          {_id: id3, key: 'b', value: 'not a number'}
        ],
        [
          {_id: 'a', total: 1},
          {_id: 'b', total: 7}
        ],
        done);
    });

    it('returns zero when no numeric values', function(done) {
      assertSumResults(
        [
          {_id: id1, key: 'a'},
          {_id: id2, key: 'b'},
          {_id: id3, key: 'b', value: 'not a number'}
        ],
        [
          {_id: 'a', total: 0},
          {_id: 'b', total: 0}
        ],
        done);
    });
  });

  describe('$max', function() {
    function assertMaxResults(input, results, done) {
      assertAggregationResults(
        input,
        [{'$group': {_id: '$key', result: {'$max': '$value'}}}],
        results,
        done);
    }

    it('computes max', function(done) {
      assertMaxResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: 7},
          {_id: id3, key: 'b', value: 8}
        ],
        [
          {_id: 'a', result: 1},
          {_id: 'b', result: 8}
        ],
        done);
    });

    it('ignores non-existent values', function(done) {
      assertMaxResults(
        [
          {_id: id1, key: 'b', value: 7},
          {_id: id2, key: 'b'}
        ],
        [{_id: 'b', result: 7}],
        done);
    });

    it('ignores null values', function(done) {
      assertMaxResults(
        [
          {_id: id1, key: 'b', value: 7},
          {_id: id2, key: 'b', value: null}
        ],
        [{_id: 'b', result: 7}],
        done);
    });

    it('returns null when no values', function(done) {
      assertMaxResults(
        [{_id: id1, key: 'b'}],
        [{_id: 'b', result: null}],
        done);
    });

    it('returns null on just null values', function(done) {
      assertMaxResults(
        [{_id: id1, key: 'b', value: null}],
        [{_id: 'b', result: null}],
        done);
    });

    it('calculates value for strings', function(done) {
      assertMaxResults(
        [
          {_id: id1, key: 'a', value: 'abc'},
          {_id: id2, key: 'a', value: 'def'}
        ],
        [{_id: 'a', result: 'def'}],
        done);
    });

    it('ranks strings higher than numbers', function(done) {
      assertMaxResults(
        [
          {_id: id1, key: 'a', value: 123},
          {_id: id2, key: 'a', value: 'def'}
        ],
        [{_id: 'a', result: 'def'}],
        done);
    });

    it('ranks object higher than strings', function(done) {
      assertMaxResults(
        [
          {_id: id1, key: 'a', value: 'def'},
          {_id: id2, key: 'a', value: {a: 1}}
        ],
        [{_id: 'a', result: {a: 1}}],
        done);
    });

    it('ranks objects on key values in same positions', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: {a: 1}},
          {_id: id1, key: 'a', value: {b: 0, a: 0}}
        ],
        [{_id: 'a', result: {b: 0, a: 0}}],
        done);
    });

    it('ranks objects with equal keys on values', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: {b: 1}},
          {_id: id1, key: 'a', value: {b: 0, a: 0}}
        ],
        [{_id: 'a', result: {b: 1}}],
        done);
    });

    it('compares objects recursively', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: {b: {x: 1}}},
          {_id: id1, key: 'a', value: {b: {x: 2}}}
        ],
        [{_id: 'a', result: {b: {x: 2}}}],
        done);
    });

    it('ranks prefix objects lower', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: {a: 1}},
          {_id: id1, key: 'a', value: {a: 1, b: 2}}
        ],
        [{_id: 'a', result: {a: 1, b: 2}}],
        done);
    });

    it('ranks arrays higher than objects', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: {a: 1}},
          {_id: id1, key: 'a', value: [1]}
        ],
        [{_id: 'a', result: [1]}],
        done);
    });

    it('ranks array elements lexicographically', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: [1]},
          {_id: id1, key: 'a', value: [2]}
        ],
        [{_id: 'a', result: [2]}],
        done);
    });

    it('performs recursive comparison of array elements', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: [{a: 1}]},
          {_id: id1, key: 'a', value: [{b: 1}]}
        ],
        [{_id: 'a', result: [{b: 1}]}],
        done);
    });

    it('ranks prefix arrays lower', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: ['x', 1]},
          {_id: id1, key: 'a', value: ['x']}
        ],
        [{_id: 'a', result: ['x', 1]}],
        done);
    });

    it('compares ObjectIds', function(done) {
      var idCompare1 = new mongoose.Types.ObjectId('5567d9a0f9932aef26f23bf1');
      var idCompare2 = new mongoose.Types.ObjectId('5567d9a0f9932aef26f23bf2');

      assertMaxResults(
        [
          {_id: id2, key: 'a', value: idCompare2},
          {_id: id1, key: 'a', value: idCompare1}
        ],
        [{_id: 'a', result: idCompare2}],
        done);
    });

    it('ranks ObjectIds higher than arrays', function(done) {
      var id = new mongoose.Types.ObjectId('5567d9a0f9932aef26f23bf1');

      assertMaxResults(
        [
          {_id: id2, key: 'a', value: [1]},
          {_id: id1, key: 'a', value: id}
        ],
        [{_id: 'a', result: id}],
        done);
    });

    it('ranks true higher than false', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: true},
          {_id: id1, key: 'a', value: false}
        ],
        [{_id: 'a', result: true}],
        done);
    });

    it('ranks Booleans higher than ObjectIds', function(done) {
      var id = new mongoose.Types.ObjectId();

      assertMaxResults(
        [
          {_id: id2, key: 'a', value: id},
          {_id: id1, key: 'a', value: false}
        ],
        [{_id: 'a', result: false}],
        done);
    });

    it('calculates value for dates', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: new Date('2009-10-01')},
          {_id: id1, key: 'a', value: new Date('1998-11-28')}
        ],
        [{_id: 'a', result: new Date('2009-10-01')}],
        done);
    });

    it('ranks dates higher than Booleans', function(done) {
      assertMaxResults(
        [
          {_id: id2, key: 'a', value: true},
          {_id: id1, key: 'a', value: new Date('2009-10-01')}
        ],
        [{_id: 'a', result: new Date('2009-10-01')}],
        done);
    });
  });

  // $min shares the implementation with $max, so we only need to have a
  // bare-bones test.
  describe('$min', function() {
    it('computes min', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 'a', value: 1},
          {_id: id2, key: 'b', value: 7},
          {_id: id3, key: 'b', value: 8}
        ],
        [{'$group': {_id: '$key', result: {'$min': '$value'}}}],
        [
          {_id: 'a', result: 1},
          {_id: 'b', result: 7}
        ],
        done);
    });
  });

  describe('$ifNull', function() {
    it('provides default for missing values', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 'a', value: 5},
          {_id: id2, key: 'a'}
        ],
        [{
          '$group': {
            _id: '$key',
            result: {'$sum': {'$ifNull': ['$value', 100]}}
        }}],
        [{_id: 'a', result: 105}],
        done);
    });

    it('provides default for null values', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 'a', value: 5},
          {_id: id2, key: 'a', value: null}
        ],
        [{
          '$group': {
            _id: '$key',
            result: {'$sum': {'$ifNull': ['$value', 200]}}
        }}],
        [{_id: 'a', result: 205}],
        done);
    });

    it('recursively computes second parameter', function(done) {
      assertAggregationResults(
        [
          {_id: id1, key: 'a', value: 5},
          {_id: id2, key: 'a'}
        ],
        [{
          '$group': {
            _id: '$key',
            result: {
              '$sum': {'$ifNull': ['$value', {'$ifNull': ['$value2', 50]}]}
            }
        }}],
        [{_id: 'a', result: 55}],
        done);
    });

    it('rejects non-array parameters', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$ifNull': 18}}}}],
        16020,
        'exception: Expression $ifNull takes exactly 2 arguments. ' +
        '1 were passed in.',
        done);
    });

    it('rejects number of parameters other than two', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$ifNull': ['$b', 12, 25]}}}}],
        16020,
        'exception: Expression $ifNull takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });

    it('validates parameter recursively if non-array', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$ifNull': {a: 1}}}}}],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates first parameter recursively', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$ifNull': [{a: 1}]}}}}],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates second parameter recursively', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$ifNull': ['$b', {a: 1}]}}}}],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });
  });

  describe('$size', function() {
    it('calculates size of an array expression', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a', value: [42, 36]}],
        [{'$group': {_id: '$key', result: {'$sum': {'$size': '$value'}}}}],
        [{_id: 'a', result: 2}],
        done);
    });

    it('accepts argument in an array', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a', value: [42, 36]}],
        [{'$group': {_id: '$key', result: {'$sum': {'$size': ['$value']}}}}],
        [{_id: 'a', result: 2}],
        done);
    });

    it('rejects empty array argument', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': []}}}}],
        16020,
        'exception: Expression $size takes exactly 1 arguments. ' +
        '0 were passed in.',
        done);
    });

    it('rejects array with multiple arguments', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': ['$value', '$b']}}}}],
        16020,
        'exception: Expression $size takes exactly 1 arguments. ' +
        '2 were passed in.',
        done);
    });

    it('recursively validates argument', function(done) {
      assertAggregationError(
        [],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': {a: 1}}}}}],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('rejects missing fields', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', othervalue: 1}],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': ['$value']}}}}],
        17124,
        'exception: The argument to $size must be an Array, ' +
        'but was of type: EOO',
        done);
    });

    it('rejects objects', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: {x: 1}}],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': ['$value']}}}}],
        17124,
        'exception: The argument to $size must be an Array, ' +
        'but was of type: Object',
        done);
    });

    it('rejects strings', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: 'abc'}],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': ['$value']}}}}],
        17124,
        'exception: The argument to $size must be an Array, ' +
        'but was of type: String',
        done);
    });

    it('rejects numbers', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: 42}],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': ['$value']}}}}],
        17124,
        'exception: The argument to $size must be an Array, ' +
        'but was of type: NumberDouble',
        done);
    });

    it('rejects dates', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: new Date()}],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': ['$value']}}}}],
        17124,
        'exception: The argument to $size must be an Array, ' +
        'but was of type: Date',
        done);
    });

    it('rejects Booleans', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{'$group': {_id: '$a', total: {'$sum': {'$size': ['$value']}}}}],
        17124,
        'exception: The argument to $size must be an Array, ' +
        'but was of type: Bool',
        done);
    });
  });

  describe('$cond', function() {
    function assertCondResult(value, result, done) {
      assertAggregationResults(
        [{_id: id1, key: 'a', value: value}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': {'if': '$value', then: 10, 'else': 1}}}
          }
        }],
        [{_id: 'a', total: result ? 10 : 1}],
        done);
    }

    it('returns if value when condition is true', function(done) {
      assertCondResult(true, true, done);
    });

    it('returns else value when condition is false', function(done) {
      assertCondResult(false, false, done);
    });

    it('treats zero as false', function(done) {
      assertCondResult(0, false, done);
    });

    it('treats null as false', function(done) {
      assertCondResult(null, false, done);
    });

    it('treats non-zero number as true', function(done) {
      assertCondResult(3, true, done);
    });

    it('treats dates as true', function(done) {
      // Even a date with the timestamp of zero is treated as true.
      assertCondResult(new Date(0), true, done);
    });

    it('treats objects as true', function(done) {
      // Even empty object is treated as true.
      assertCondResult({}, true, done);
    });

    it('treats arrays as true', function(done) {
      // Even empty array is treated as true.
      assertCondResult([], true, done);
    });

    it('accepts arguments in array', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {_id: '$key', total: {'$sum': {'$cond': ['$value', 10, 1]}}}
        }],
        [{_id: 'a', total: 10}],
        done);
    });

    it('accepts arguments in array with else branch', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a', value: false}],
        [{
          '$group': {_id: '$key', total: {'$sum': {'$cond': ['$value', 10, 1]}}}
        }],
        [{_id: 'a', total: 1}],
        done);
    });

    it('recursively computes expressions in then branch', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {
              '$sum': {
                '$cond': {
                  'if': '$value',
                  'then': {'$ifNull': ['$backup', 100]},
                  'else': 1
                }
              }
            }
          }
        }],
        [{_id: 'a', total: 100}],
        done);
    });

    it('recursively computes expressions in then branch', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a', value: false, backup: 50}],
        [{
          '$group': {
            _id: '$key',
            total: {
              '$sum': {
                '$cond': {
                  'if': '$value',
                  'then': 10,
                  'else': {'$ifNull': ['$backup', 100]}
                }
              }
            }
          }
        }],
        [{_id: 'a', total: 50}],
        done);
    });

    it('rejects missing if parameter', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': {then: 10, 'else': 1}}}
          }
        }],
        17080,
        "exception: Missing 'if' parameter to $cond",
        done);
    });

    it('rejects missing then parameter', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': {'if': '$value', 'else': 1}}}
          }
        }],
        17080,
        "exception: Missing 'then' parameter to $cond",
        done);
    });

    it('rejects missing else parameter', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': {'if': '$value', 'then': 10}}}
          }
        }],
        17080,
        "exception: Missing 'else' parameter to $cond",
        done);
    });

    it('rejects extra parameters', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {
              '$sum': {
                '$cond': {'if': '$value', 'then': 10, 'else': 1, otherwise: 3}
              }
            }
          }
        }],
        17083,
        'exception: Unrecognized parameter to $cond: otherwise',
        done);
    });

    it('rejects bad number of array arguments', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': ['$value', 10]}}
          }
        }],
        16020,
        'exception: Expression $cond takes exactly 3 arguments. ' +
        '2 were passed in.',
        done);
    });

    it('rejects scalar parameter', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{'$group': {_id: '$key', total: {'$sum': {'$cond': 22}}}}],
        16020,
        'exception: Expression $cond takes exactly 3 arguments. ' +
        '1 were passed in.',
        done);
    });

    it('rejects argument object inside array', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': [{'if': '$value', then: 10, 'else': 1}]}}
          }
        }],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates first parameter recursively', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {_id: '$key', total: {'$sum': {'$cond': [{a: 1}, 10, 1]}}}
        }],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates second parameter recursively', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': ['$value', {a: 1}, 1]}}
          }
        }],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates third parameter recursively', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': ['$value', 10, {a: 1}]}}
          }
        }],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates if parameter recursively', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': {'$cond': {'if': {a: 1}, then: 10, 'else': 1}}}
          }
        }],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates then parameter recursively', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {
              '$sum': {
                '$cond': {'if': '$value', then: {a: 1}, 'else': 1}
              }
            }
          }
        }],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });

    it('validates else parameter recursively', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a', value: true}],
        [{
          '$group': {
            _id: '$key',
            total: {
              '$sum': {
                '$cond': {'if': '$value', then: 10, 'else': {a: 1}}
              }
            }
          }
        }],
        16420,
        'exception: field inclusion is not allowed inside of $expressions',
        done);
    });
  });

  describe('$eq', function() {
    it('returns true on equal values', function() {
      return assertComparisonResult('$eq', 5, 5, true);
    });

    it('returns false on unequal values', function() {
      return assertComparisonResult('$eq', 5, 4, false);
    });

    it('compares objects recursively', function() {
      return assertComparisonResult('$eq', {a: 1}, {a: 1}, true);
    });

    it('compares arrays recursively', function() {
      return assertComparisonResult('$eq', [1, 2], [1, 2], true);
    });

    it('evaluates arguments recursively', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a'}],
        [{
          '$group': {
            _id: '$key',
            result: {
              '$max': {
                '$eq': [
                  {'$ifNull': ['$valueLhs', 3]},
                  {'$ifNull': ['$valueRhs', 3]}
                ]
              }
            }
          }
        }],
        [{_id: 'a', result: true}],
        done);
    });

    // $eq shares validation code with other operators so we just perform a
    // minimal check here.
    it('rejects number of arguments other than two', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a'}],
        [{'$group': {_id: '$key', result: {'$max': {'$eq': [3, 4, 5]}}}}],
        16020,
        'exception: Expression $eq takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });
  });

  describe('$ne', function() {
    it('returns false on equal values', function() {
      return assertComparisonResult('$ne', 5, 5, false);
    });

    it('returns true on unequal values', function() {
      return assertComparisonResult('$ne', 5, 4, true);
    });

    it('compares objects recursively', function() {
      return assertComparisonResult('$ne', {a: 1}, {a: 1}, false);
    });

    it('compares arrays recursively', function() {
      return assertComparisonResult('$ne', [1, 2], [1, 2], false);
    });

    it('evaluates arguments recursively', function(done) {
      assertAggregationResults(
        [{_id: id1, key: 'a'}],
        [{
          '$group': {
            _id: '$key',
            result: {
              '$max': {
                '$ne': [
                  {'$ifNull': ['$valueLhs', 3]},
                  {'$ifNull': ['$valueRhs', 3]}
                ]
              }
            }
          }
        }],
        [{_id: 'a', result: false}],
        done);
    });

    it('rejects number of arguments other than two', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a'}],
        [{'$group': {_id: '$key', result: {'$max': {'$ne': [3, 4, 5]}}}}],
        16020,
        'exception: Expression $ne takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });
  });

  describe('$lt', function() {
    it('returns true when lhs is less than rhs', function() {
      return assertStrictComparisonResult('$lt', 3, 5, true);
    });

    it('returns false when arguments are equal', function() {
      return assertComparisonResult('$lt', 5, 5, false);
    });

    it('ranks numbers higher than null', function() {
      return assertStrictComparisonResult('$lt', null, 5, true);
    });

    it('ranks strings higher than numbers', function() {
      return assertStrictComparisonResult('$lt', 4, '4', true);
    });

    it('ranks object higher than strings', function() {
      return assertStrictComparisonResult('$lt', 'abc', {a: 1}, true);
    });

    it('ranks objects on key values in same positions', function() {
      return assertStrictComparisonResult('$lt', {a: 1}, {b: 0, a: 0}, true);
    });

    it('ranks objects with equal keys on values', function() {
      return assertStrictComparisonResult('$lt', {b: 0, a: 0}, {b: 1}, true);
    });

    it('compares objects recursively', function() {
      return assertStrictComparisonResult(
        '$lt',
        {b: {x: 1}},
        {b: {x: 2}},
        true);
    });

    it('ranks prefix objects lower', function() {
      return assertStrictComparisonResult('$lt', {a: 1}, {a: 1, b: 2}, true);
    });

    it('ranks arrays higher than objects', function() {
      return assertStrictComparisonResult('$lt', {a: 1}, [1], true);
    });

    it('ranks array elements lexicographically', function() {
      return assertStrictComparisonResult('$lt', [1], [2], true);
    });

    it('performs recursive comparison of array elements', function() {
      return assertStrictComparisonResult('$lt', [{a: 1}], [{b: 1}], true);
    });

    it('ranks prefix arrays lower', function() {
      return assertStrictComparisonResult('$lt', ['x'], ['x', 1], true);
    });

    it('compares ObjectIds', function() {
      var idCompare1 = new mongoose.Types.ObjectId('5567d9a0f9932aef26f23bf1');
      var idCompare2 = new mongoose.Types.ObjectId('5567d9a0f9932aef26f23bf2');
      return assertStrictComparisonResult('$lt', idCompare1, idCompare2, true);
    });

    it('ranks ObjectIds higher than arrays', function() {
      var id = new mongoose.Types.ObjectId('5567d9a0f9932aef26f23bf1');
      return assertStrictComparisonResult('$lt', ['x'], id, true);
    });

    it('ranks true higher than false', function() {
      return assertStrictComparisonResult('$lt', false, true, true);
    });

    it('ranks Booleans higher than ObjectIds', function() {
      var id = new mongoose.Types.ObjectId();
      return assertStrictComparisonResult('$lt', id, false, true);
    });

    it('compares dates', function() {
      return assertStrictComparisonResult(
        '$lt',
        new Date('1998-11-28'),
        new Date('2009-10-01'),
        true);
    });

    it('ranks dates higher than Booleans', function() {
      return assertStrictComparisonResult(
        '$lt',
        true,
        new Date('2009-10-01'),
        true);
    });

    it('rejects number of arguments other than two', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a'}],
        [{'$group': {_id: '$key', result: {'$max': {'$lt': [3, 4, 5]}}}}],
        16020,
        'exception: Expression $lt takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });
  });

  describe('$lte', function() {
    it('returns true when lhs is less than rhs', function() {
      return assertComparisonResult('$lte', 3, 5, true);
    });

    it('returns false when lhs is greater than rhs', function() {
      return assertComparisonResult('$lte', 5, 3, false);
    });

    it('returns true when lhs is equal to rhs', function() {
      return assertComparisonResult('$lte', 5, 5, true);
    });

    it('rejects number of arguments other than two', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a'}],
        [{'$group': {_id: '$key', result: {'$max': {'$lte': [3, 4, 5]}}}}],
        16020,
        'exception: Expression $lte takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });
  });

  describe('$gt', function() {
    it('returns true when lhs is greater than rhs', function() {
      return assertStrictComparisonResult('$gt', 5, 3, true);
    });

    it('returns false when arguments are equal', function() {
      return assertComparisonResult('$gt', 5, 5, false);
    });

    it('rejects number of arguments other than two', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a'}],
        [{'$group': {_id: '$key', result: {'$max': {'$gt': [3, 4, 5]}}}}],
        16020,
        'exception: Expression $gt takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });
  });

  describe('$lte', function() {
    it('returns true when lhs is less than rhs', function() {
      return assertComparisonResult('$gte', 5, 3, true);
    });

    it('returns false when lhs is greater than rhs', function() {
      return assertComparisonResult('$gte', 3, 5, false);
    });

    it('returns true when lhs is equal to rhs', function() {
      return assertComparisonResult('$gte', 5, 5, true);
    });

    it('rejects number of arguments other than two', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a'}],
        [{'$group': {_id: '$key', result: {'$max': {'$gte': [3, 4, 5]}}}}],
        16020,
        'exception: Expression $gte takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });
  });

  describe('$cmp', function() {
    it('returns -1 when lhs is less than rhs', function() {
      return assertComparisonResult('$cmp', 3, 5, -1);
    });

    it('returns 1 when lhs is greater than rhs', function() {
      return assertComparisonResult('$cmp', 5, 3, 1);
    });

    it('returns 0 when lhs is equal to rhs', function() {
      return assertComparisonResult('$cmp', 5, 5, 0);
    });

    it('rejects number of arguments other than two', function(done) {
      assertAggregationError(
        [{_id: id1, key: 'a'}],
        [{'$group': {_id: '$key', result: {'$max': {'$cmp': [3, 4, 5]}}}}],
        16020,
        'exception: Expression $cmp takes exactly 2 arguments. ' +
        '3 were passed in.',
        done);
    });
  });
});
