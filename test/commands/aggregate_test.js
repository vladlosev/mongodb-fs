'use strict';

var chai = require('chai');
var mongoose = require('mongoose');

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

  before(function(done) {
    harness.setUp(function(error) {
      Item = mongoose.connection.models.Item;
      done(error);
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  beforeEach(function() {
    fakeDatabase.items = [
      {_id: id1, key: 'a', value: 1},
      {_id: id2, key: 'b', value: 1},
      {_id: id3, key: 'b', value: 2}
    ];
  });

  it('empty sequence returns all documents', function(done) {
    Item.collection.aggregate([], function(error, items) {
      if (error) return done(error);
      expect(items).to.deep.equal(fakeDatabase.items);
      done();
    });
  });

  it('performs simple group operation', function(done) {
    Item.aggregate(
      [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
      function(error, items) {
        if (error) return done(error);
        expect(items).to.deep.equal([
          {_id: 'a', total: 1},
          {_id: 'b', total: 3}
        ]);
        done();
    });
  });

  describe('$group', function() {
    it('supports multiple groupings', function(done) {
      fakeDatabase.items = [
        {_id: id1, key: 'a', value: 1, value2: 10},
        {_id: id2, key: 'b', value: 1, value2: 15},
        {_id: id3, key: 'b', value: 2, value2: 25}
      ];
      Item.aggregate(
        [{
          '$group': {
            _id: '$key',
            total: {'$sum': '$value'}, total2: {'$sum': '$value2'}
          }
        }],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: 'a', total: 1, total2: 10},
            {_id: 'b', total: 3, total2: 40}
          ]);
          done();
      });
    });

    it('supports dates as _ids', function(done) {
      fakeDatabase.items = [
        {_id: id1, key: new Date('1995-08-08'), value: 1},
        {_id: id2, key: new Date('1998-10-28'), value: 1},
        {_id: id3, key: new Date('1998-10-28'), value: 2}
      ];
      Item.aggregate(
        [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: new Date('1995-08-08'), total: 1},
            {_id: new Date('1998-10-28'), total: 3}
          ]);
          done();
      });
    });

    it('supports single stage pipeline as object', function(done) {
      Item.aggregate(
        {'$group': {_id: '$key', total: {'$sum': '$value'}}},
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: 'a', total: 1},
            {_id: 'b', total: 3}
          ]);
          done();
      });
    });

    it('treats missing _id keys as nulls when grouping', function(done) {
      fakeDatabase.items = [
        {_id: id1, key: 'a', value: 3},
        {_id: id3, value: 5}
      ];
      Item.aggregate(
        [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: 'a', total: 3},
            {_id: null, total: 5}
          ]);
          done();
      });
    });

    it('returns empty array given empty input', function(done) {
      fakeDatabase.items = [];
      Item.aggregate(
        [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([]);
          done();
      });
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
      Item.collection.aggregate(
        [{'$group': {total: {'$sum': 1}}}],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 15955);
          expect(error)
            .to.have.property('message')
            .to.have.string(
              'exception: a group specification must include an _id');
          done();
      });
    });

    it('rejects invalid group aggregate field expressions', function(done) {
      Item.collection.aggregate(
        [{'$group': {_id: '$a', total: 1}}],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 15951);
          expect(error)
            .to.have.property('message')
            .to.have.string(
              "exception: the group aggregate field 'total' " +
              'must be defined as an expression inside an object');
          done();
      });
    });

    it('rejects empty computed aggregate', function(done) {
      Item.collection.aggregate(
        [{'$group': {_id: '$a', total: {}}}],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 15954);
          expect(error)
            .to.have.property('message')
            .to.have.string(
              "exception: the computed aggregate 'total' " +
              'must specify exactly one operator');
          done();
      });
    });

    it('rejects computed aggregate with multiple keys', function(done) {
      Item.collection.aggregate(
        [{'$group': {_id: '$a', total: {'$sum': '$a', '$max': '$a'}}}],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 15954);
          expect(error)
            .to.have.property('message')
            .to.have.string(
              "exception: the computed aggregate 'total' " +
              'must specify exactly one operator');
          done();
      });
    });

    it('rejects computed aggregate with multiple keys', function(done) {
      Item.collection.aggregate(
        [{'$group': {_id: '$a', total: {x: '$b'}}}],
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('code', 15952);
          expect(error)
            .to.have.property('message')
            .to.equal("exception: unknown group operator 'x'");
          done();
      });
    });
  });

  describe('$match', function() {
    it('filters documents', function(done) {
      fakeDatabase.items = [
        {_id: id1, key: 'a', value: 1},
        {_id: id2, key: 'b', value: 3},
        {_id: id3, key: 'b', value: 4}
      ];
      Item.aggregate(
        [{'$match': {value: {'$gt': 2}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: id2, key: 'b', value: 3},
            {_id: id3, key: 'b', value: 4}
          ]);
          done();
      });
    });

    it('supports filtering on subdocuments', function(done) {
      fakeDatabase.items = [
        {_id: id1, key: 'a', value: 1},
        {_id: id2, key: 'b', value: {x: 5}},
        {_id: id3, key: 'b', value: {x: 7}}
      ];
      Item.aggregate(
        [{'$match': {'value.x': 5}}],
        function(error, items) {
          if (error) return done(error);

          expect(items)
            .to.deep.equal([{_id: id2, key: 'b', value: {x: 5}}]);
          done();
      });
    });

    it('reject invalid match queries', function(done) {
      Item.aggregate(
        [{'$match': {'$eq': 3}}],
        function(error, items) {
          expect(error).to.have.property('ok', false);
          expect(error)
            .to.have.property('message')
            .to.match(/BadValue unknown top level operator: \$eq/);
          done();
      });
    });
  });

  describe('$sum', function() {
    it('sums values in input', function(done) {
      Item.aggregate(
        [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: 'a', total: 1},
            {_id: 'b', total: 3}
          ]);
          done();
      });
    });

    it('ignores nonexistent values', function(done) {
      fakeDatabase.items = [
        {_id: id1, key: 'a', value: 1},
        {_id: id2, key: 'b', value: 5},
        {_id: id3, key: 'b'}
      ];
      Item.aggregate(
        [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: 'a', total: 1},
            {_id: 'b', total: 5}
          ]);
          done();
      });
    });

    it('ignores non-numeric values', function(done) {
      fakeDatabase.items = [
        {_id: id1, key: 'a', value: 1},
        {_id: id2, key: 'b', value: 7},
        {_id: id3, key: 'b', value: 'non a number'}
      ];
      Item.aggregate(
        [{'$group': {_id: '$key', total: {'$sum': '$value'}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: 'a', total: 1},
            {_id: 'b', total: 7}
          ]);
          done();
      });
    });

    it('returns zero when no numeric values', function(done) {
      Item.aggregate(
        [{'$group': {_id: '$key', total: {'$sum': '$nonexistent'}}}],
        function(error, items) {
          if (error) return done(error);
          expect(items).to.deep.equal([
            {_id: 'a', total: 0},
            {_id: 'b', total: 0}
          ]);
          done();
      });
    });
  });
});
