'use strict';

var _ = require('lodash');
var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('../test_harness');

// Chai uses properties rather than methods for assertions.
/* eslint-disable no-unused-expressions */

describe('update', function() {
  var expect = chai.expect;

  var id1 = new mongoose.Types.ObjectId();
  var id2 = new mongoose.Types.ObjectId();

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

  it('updates all documents', function(done) {
    fakeDatabase.items = [{key: 'value1', _id: id1}, {key: 'value2', _id: id2}];
    Item.collection.update(
      {},
      {'$set': {key: 'new value'}},
      {multi: true},
      function(error, updatedDocuments) {
        if (error) return done(error);
        expect(updatedDocuments).to.equal(2);
        expect(fakeDatabase.items).to.have.length(2);
        expect(fakeDatabase.items[0])
          .to.have.property('key', 'new value');
        expect(fakeDatabase.items[1])
          .to.have.property('key', 'new value');
        expect(updatedDocuments).to.equal(2);
        done();
    });
  });

  it('updates documents by query', function(done) {
    fakeDatabase.items = [{key: 'value1', _id: id1}, {key: 'value2', _id: id2}];
    Item.collection.update(
      {key: 'value2'},
      {'$set': {key: 'new value'}},
      function(error, updatedDocuments) {
        if (error) return done(error);
        expect(updatedDocuments).to.equal(1);
        expect(fakeDatabase.items).to.have.length(2);
        expect(fakeDatabase.items[0])
          .to.have.property('key', 'value1');
        expect(fakeDatabase.items[1])
          .to.have.property('key', 'new value');
        expect(updatedDocuments).to.equal(1);
        done();
    });
  });

  it('updates nothing when query does not match', function(done) {
    fakeDatabase.items = [{key: 'value1', _id: id1}, {key: 'value2', _id: id2}];
    Item.collection.update(
      {key: 'value3'},
      {'$set': {key: 'new value'}},
      function(error, updatedDocuments) {
        if (error) return done(error);

        expect(updatedDocuments).to.equal(0);
        expect(fakeDatabase.items).to.have.length(2);
        expect(_.pluck(fakeDatabase.items, 'key'))
          .to.deep.equal(['value1', 'value2']);
        done();
    });
  });

  it('replaces fields', function(done) {
    fakeDatabase.items = [{a: 'value', b: 1, c: 'there', _id: id1}];
    Item.collection.update(
      {a: 'value'},
      {a: 'new value', '$inc': {b: 1}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items)
          .to.deep.equal([{a: 'new value', b: 2, c: 'there', _id: id1}]);
        done();
    });
  });

  it('replaces subfields', function(done) {
    fakeDatabase.items = [{a: 'value1', b: {c: 1}, _id: id1}];
    Item.collection.update(
      {a: 'value1'},
      {'$set': {'b.c': 42}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items)
          .to.deep.equal([{a: 'value1', b: {c: 42}, _id: id1}]);
        done();
    });
  });

  it('rejects subfield literals', function(done) {
    fakeDatabase.items = [{a: 'value1', b: {c: 1}, _id: id1}];
    Item.collection.update(
      {a: 'value1'},
      {'b.c': 42},
      function(error) {
        expect(error).to.have.property('ok', false);
        expect(error).to.have.property('name', 'MongoError');
        expect(error)
          .to.have.property('err')
          .to.have.string("can't have . in field names [b.c]");
        done();
    });
  });

  it('replaces entire documents', function(done) {
    fakeDatabase.items = [{a: 'value1', b: 1, _id: id1}];
    Item.collection.update({a: 'value1'}, {b: 42}, function(error) {
      if (error) return done(error);
      // The subfield a.c should be gone as the update document does not
      // specify any operators.
      expect(fakeDatabase.items)
        .to.deep.equal([{b: 42, _id: id1}]);
      done();
    });
  });

  it('does not create non-existent collection', function(done) {
    delete fakeDatabase.items;
    Item.collection.update({a: 'value1'}, {b: 42}, function(error, n) {
      if (error) return done(error);

      expect(n).to.equal(0);
      // The subfield a.c should be gone as the update document does not
      // specify any operators.
      expect(fakeDatabase).to.not.have.property('items');
      done();
    });
  });

  it('$pushAll to array', function(done) {
    fakeDatabase.items = [{_id: id1, key: ['value1']}];
    Item.collection.update(
      {_id: id1},
      {'$pushAll': {key: ['value2']}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .deep.equal(['value1', 'value2']);
        done();
      });
  });

  it('$pushAll to non-existent field creates array', function(done) {
    fakeDatabase.items = [{_id: id1}];
    Item.collection.update(
      {},
      {'$pushAll': {key: ['a', 'b']}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .to.deep.equal(['a', 'b']);
        done();
      });
  });

  it('$pushAll to non-array fails', function(done) {
    fakeDatabase.items = [{key: {a: 1}}];
    Item.collection.update(
      {},
      {'$pushAll': {'key': ['a']}},
      function(error) {
        expect(error).to.exist;
        expect(error.ok).to.be.false;
        expect(error)
          .to.have.property('err', "The field 'key' must be an array.");
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.deep.equal({key: {a: 1}});
        done();
    });
  });

  it('$pushAll with non-array argument fails', function(done) {
    fakeDatabase.items = [{key: ['value1', 'value2']}];
    Item.collection.update(
      {},
      {'$pushAll': {key: 'abc'}},
      function(error) {
        expect(error).to.exist;
        expect(error.ok).to.be.false;
        expect(error)
          .to.have.property('err')
          .to.contain(
            '$pushAll requires an array of values but was given an String');
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .to.deep.equal(['value1', 'value2']);
        done();
      });
  });

  it('set array value', function(done) {
    fakeDatabase.items = [{_id: id1, key: ['value1', 'value2']}];
    Item.collection.update(
      {_id: id1},
      {'$set': {key: ['one', 'two']}},
      function(error, n) {
        if (error) return done(error);
        expect(n).to.equal(1);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .deep.equal(['one', 'two']);
        done();
      });
  });

  it('set Date field', function(done) {
    var tenSecondsAgo = new Date(Date.now() - 10 * 1000);
    var now = new Date();
    fakeDatabase.items = [{_id: id1, date: tenSecondsAgo}];
    Item.findOne({_id: id1}, function(error, item) {
      if (error) return done(error);
      expect(item.toObject())
        .to.have.property('date')
        .to.be.instanceof(Date)
        .and.to.eql(tenSecondsAgo);
      Item.collection.update({}, {'$set': {date: now}}, function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('date')
          .to.be.instanceof(Date)
          .and.to.eql(now);
        done();
      });
    });
  });

  it('set array of dates field', function(done) {
    var tenSecondsAgo = new Date(Date.now() - 10 * 1000);
    var now = new Date();
    fakeDatabase.items = [{_id: id1, date: tenSecondsAgo}];
    Item.collection.update({}, {$set: {date: [now]}}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.have.deep.property('date')
        .to.have.length(1)
        .and.to.have.property('[0]')
        .to.be.instanceof(Date)
        .and.to.eql(now);
      done();
    });
  });

  it('$pull', function(done) {
    fakeDatabase.items = [{_id: id1, key: ['value1', 'value2']}];
    Item.collection.update(
      {},
      {$pull: {key: 'value1'}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .to.deep.equal(['value2']);
        done();
      });
  });

  it('$pull ObjectIds', function(done) {
    var idCopy = new mongoose.Types.ObjectId(id1.toString());
    fakeDatabase.items = [{key: [id1]}];
    Item.collection.update({}, {$pull: {key: idCopy}}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.deep.equal({key: []});
      done();
    });
  });

  it('$pull multiple fields', function(done) {
    fakeDatabase.items = [{
      _id: id1,
      key: ['a', 'b'],
      key2: ['c', 'd']
    }];
    Item.collection.update(
      {},
      {$pull: {key: 'a', key2: 'd'}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .to.deep.equal(['b']);
        expect(fakeDatabase.items[0])
          .to.have.property('key2')
          .to.deep.equal(['c']);
        done();
      });
  });

  it('$pull by query removes multiple values', function(done) {
    fakeDatabase.items = [{_id: id1, key: [1, 11, 2, 12, 3, 13, 4]}];
    Item.collection.update(
      {},
      {$pull: {key: {'$gt': 10}}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .to.deep.equal([1, 2, 3, 4]);
        done();
      });
  });

  it('$pull by query removes repeated values', function(done) {
    fakeDatabase.items = [{_id: id1, key: [1, 13, 2, 13, 3, 13]}];
    Item.collection.update(
      {},
      {$pull: {key: {'$gt': 10}}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.property('key')
          .to.deep.equal([1, 2, 3]);
        done();
      });
  });

  it('$pull by query works with subfields', function(done) {
    fakeDatabase.items = [{_id: id1, key: {a: [1, 2, 10, 20]}}];
    Item.collection.update(
      {},
      {$pull: {'key.a': {'$lt': 10}}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.deep.property('key')
          .to.deep.equal({a: [10, 20]});
        done();
      });
  });

  it('$pull by query removes documents from array', function(done) {
    fakeDatabase.items = [{
      _id: id1,
      key: [{a: 1}, {a: 2}, {a: 10}, {a: 20}]
    }];
    Item.collection.update(
      {},
      {$pull: {'key': {a: {'$lt': 10}}}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items).to.have.length(1);
        expect(fakeDatabase.items[0])
          .to.have.deep.property('key')
          .to.deep.equal([{a: 10}, {a: 20}]);
        done();
      });
  });

  it('$pull by query removes documents from array using top level operator',
    function(done) {
      fakeDatabase.items = [{
        _id: id1,
        key: [{a: 5}, {a: 15}, {a: 25}]
      }];
      Item.collection.update(
        {},
        {$pull: {key: {'$or': [{a: {'$lt': 10}}, {a: {'$gt': 20}}]}}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.have.deep.property('key')
            .to.deep.equal([{a: 15}]);
          done();
        });
  });

  it('$pull from non-array fails', function(done) {
    fakeDatabase.items = [{key: ['value1', 'value2']}];
    Item.collection.update({}, {$pull: {'key.1': 'a'}}, function(error) {
      expect(error).to.exist;
      expect(error.ok).to.be.false;
      expect(error)
        .to.have.property('err', 'Cannot apply $pull to a non-array value');
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.deep.equal({key: ['value1', 'value2']});
      done();
    });
  });

  it('with non-container in path fails', function(done) {
    fakeDatabase.items = [{key: 'value1'}];
    Item.collection.update(
      {key: 'value1'},
      {$set: {'key.k2.k3': 5}},
      function(error) {
        expect(error).to.exist;
        expect(error.ok).to.be.false;
        expect(error)
          .to.have.property(
            'err',
            'cannot use the part (k2 of key.k2.k3)' +
            " to traverse the element ({ key: 'value1' })");
        done();
      });
  });

  it('rejects invalid filters', function(done) {
    fakeDatabase.items = [{key: 'value1'}];
    Item.collection.update({'$eq': 2}, {$set: {'value': 5}}, function(error) {
      expect(error).to.exist;
      expect(error.ok).to.be.false;
      expect(error)
        .to.have.property('err', 'BadValue unknown top level operator: $eq');
      done();
    });
  });

  describe('$unset', function() {
    it('deletes fields', function(done) {
      fakeDatabase.items = [{a: 'value1', b: 33, _id: id1}];
      Item.collection.update(
        {_id: id1},
        {'$unset': {b: 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', _id: id1});
          done();
      });
    });

    it('deletes compound fields', function(done) {
      fakeDatabase.items = [{a: 'value1', b: {c: 1}, _id: id1}];
      Item.collection.update(
        {_id: id1},
        {'$unset': {b: 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', _id: id1});
          done();
      });
    });

    it('deletes subfields', function(done) {
      fakeDatabase.items = [{
        a: 'value1',
        b: {c: 1, d: 2},
        _id: id1}];
      Item.collection.update(
        {_id: id1},
        {'$unset': {'b.c': 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', b: {d: 2}, _id: id1});
          done();
      });
    });

    it('deletes multiple fields', function(done) {
      fakeDatabase.items = [{a: 'value1', b: 33, _id: id1}];
      Item.collection.update(
        {_id: id1},
        {'$unset': {a: 'value ignored', b: 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({_id: id1});
          done();
      });
    });

    it('ignores non-existing fields', function(done) {
      fakeDatabase.items = [{a: 'value1', b: 33, _id: id1}];
      Item.collection.update(
        {_id: id1},
        {'$unset': {c: 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', b: 33, _id: id1});
          done();
      });
    });

    it('ignores non-existing subfields', function(done) {
      fakeDatabase.items = [{
        a: 'value1',
        b: {c: 1, d: {x: 2}},
        _id: id1
      }];
      Item.collection.update(
        {_id: id1},
        {'$unset': {'b.h': 0, 'b.d.y': 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', b: {c: 1, d: {x: 2}}, _id: id1});
          done();
      });
    });

    it('ignores subfields of non-objects', function(done) {
      fakeDatabase.items = [{a: 'value1', _id: id1}];
      Item.collection.update(
        {_id: id1},
        {'$unset': {'a.f': 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', _id: id1});
          done();
      });
    });

    it('nulls out elements of arrays', function(done) {
      fakeDatabase.items = [{
        a: 'value1',
        b: [1, 2, 3],
        _id: id1
      }];
      Item.collection.update(
        {_id: id1},
        {'$unset': {'b.1': 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', b: [1, null, 3], _id: id1});
          done();
      });
    });

    it('ignores out-of-index elements of arrays', function(done) {
      fakeDatabase.items = [{
        a: 'value1',
        b: [1, 2, 3],
        _id: id1
      }];
      Item.collection.update(
        {_id: id1},
        {'$unset': {'b.8': 0}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', b: [1, 2, 3], _id: id1});
          done();
      });
    });
  });

  describe('upsert', function() {
    it('updates existing documents', function(done) {
      fakeDatabase.items = [{a: 'value', b: 1, _id: id1}];
      Item.collection.update(
        {a: 'value'},
        {'$set': {a: 'new value'}, '$inc': {b: 10}},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items)
            .to.deep.equal([{a: 'new value', b: 11, _id: id1}]);
          done();
      });
    });

    it('inserts new document when no matches', function(done) {
      fakeDatabase.items = [{a: 'value1', b: 1, _id: id1}];
      Item.collection.update(
        {a: 'value2'},
        {b: 10},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(2);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', b: 1, _id: id1});
          var newDocument = fakeDatabase.items[1];
          expect(newDocument)
            .to.have.deep.property('_id.constructor.name', 'ObjectID');
          expect(newDocument).to.have.property('b', 10);
          done();
      });
    });

    it('creates a collection if does not exist when inserting', function(done) {
      delete fakeDatabase.items;
      Item.collection.update(
        {a: 'value2'},
        {b: 10},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          var newDocument = fakeDatabase.items[0];
          expect(newDocument)
            .to.have.deep.property('_id.constructor.name', 'ObjectID');
          expect(newDocument).to.have.property('b', 10);
          done();
      });
    });

    it('uses only update document values if update contains no operators',
      function(done) {
        fakeDatabase.items = [{a: 'value1', b: 1, _id: id1}];
        Item.collection.update(
          {a: 'value2'},
          {b: 10, c: 'whatever'},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(fakeDatabase.items).to.have.length(2);
            expect(fakeDatabase.items[0])
              .to.deep.equal({a: 'value1', b: 1, _id: id1});
            var newDocument = fakeDatabase.items[1];
            expect(newDocument)
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            expect(_.omit(newDocument, '_id'))
              .to.deep.equal({b: 10, c: 'whatever'});
            done();
        });
    });

    it('uses update and find document values if update contains operators',
      function(done) {
        fakeDatabase.items = [{a: 'value1', b: 1, _id: id1}];
        Item.collection.update(
          {a: 'value2', b: {'$gt': 5}},
          {'$set': {c: 10}, d: 'whatever'},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(fakeDatabase.items).to.have.length(2);
            expect(fakeDatabase.items[0])
              .to.deep.equal({a: 'value1', b: 1, _id: id1});
            var newDocument = fakeDatabase.items[1];
            expect(newDocument)
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            // Non-equality comparison should not be a basis for the new
            // document.
            expect(_.omit(newDocument, '_id'))
              .to.deep.equal({a: 'value2', c: 10, d: 'whatever'});
            done();
        });
    });

    it('pulls values from $and conjunctions if update contains operators',
      function(done) {
        fakeDatabase.items = [{a: 'value1', b: 1, _id: id1}];
        Item.collection.update(
          {'$and': [
            {a: 'value2', b: 18},
            {'$and': [{c: 42}, {'d.e': 36}]},
            {'$or': [{m: 4458}, {n: 5577}]}]},
          {'$set': {z: 'whatever'}},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(fakeDatabase.items).to.have.length(2);
            expect(fakeDatabase.items[0])
              .to.deep.equal({a: 'value1', b: 1, _id: id1});
            var newDocument = fakeDatabase.items[1];
            expect(newDocument)
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            // Equalities under $or should not get transplanted to a new
            // document, so the result should not contain m or n.
            expect(_.omit(newDocument, '_id'))
              .to.deep.equal({
                a: 'value2',
                b: 18,
                c: 42,
                d: {e: 36},
                z: 'whatever'});
            done();
        });
    });

    it('does not unpack ObjectIDs when copying from query', function(done) {
      fakeDatabase.items = [{a: 'value1', _id: id1}];
      Item.collection.update(
        {a: 'value2', b: id2},
        {'$set': {c: 10}},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(2);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', _id: id1});
          var newDocument = fakeDatabase.items[1];
          expect(newDocument)
            .to.have.deep.property('_id.constructor.name', 'ObjectID');
          // An ObjectID must be pulled in its entirety and no pulled apart.
          expect(newDocument).to.have.property('b');
          expect(newDocument.b.equals(id2)).to.be.true;
          done();
      });
    });

    it('$setOnInsert modifies inserted records', function(done) {
      fakeDatabase.items = [{a: 'value1', _id: id1}];
      Item.collection.update(
        {a: 'value2'},
        {'$setOnInsert': {c: 10}},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(2);
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', _id: id1});
          var newDocument = fakeDatabase.items[1];
          // $setOnInsert puts its arguments into the new document.
          expect(_.omit(newDocument, '_id'))
            .to.deep.equal({a: 'value2', c: 10});
          done();
      });
    });

    it('$setOnInsert does not touch updated records', function(done) {
      fakeDatabase.items = [{a: 'value1', _id: id1}];
      Item.collection.update(
        {a: 'value1'},
        {'$setOnInsert': {c: 10}, '$set': {b: 5}},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          // $setOnInsert's arguments do not apply to existing documents.
          expect(fakeDatabase.items[0])
            .to.deep.equal({a: 'value1', b: 5, _id: id1});
          done();
      });
    });
  });

  describe('multi', function() {
    it('updates single document by default', function(done) {
      fakeDatabase.items = [
        {a: 'value', b: 1, _id: id1},
        {a: 'value', b: 2, _id: id2}
      ];
      Item.collection.update(
        {a: 'value'},
        {'$set': {a: 'new value'}, '$inc': {b: 10}},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items)
            .to.deep.equal([
              {a: 'new value', b: 11, _id: id1},
              {a: 'value', b: 2, _id: id2}
            ]);
          done();
      });
    });

    it('updates single document when set to false', function(done) {
      fakeDatabase.items = [
        {a: 'value', b: 1, _id: id1},
        {a: 'value', b: 2, _id: id2}
      ];
      Item.collection.update(
        {a: 'value'},
        {'$set': {a: 'new value'}, '$inc': {b: 10}},
        {multi: false},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items)
            .to.deep.equal([
              {a: 'new value', b: 11, _id: id1},
              {a: 'value', b: 2, _id: id2}
            ]);
          done();
      });
    });

    it('updates multiple documents when set to true', function(done) {
      fakeDatabase.items = [
        {a: 'value', b: 1, _id: id1},
        {a: 'value', b: 2, _id: id2}
      ];
      Item.collection.update(
        {a: 'value'},
        {'$set': {a: 'new value'}, '$inc': {b: 10}},
        {multi: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items)
            .to.deep.equal([
              {a: 'new value', b: 11, _id: id1},
              {a: 'new value', b: 12, _id: id2}
            ]);
          done();
      });
    });

    it('rejects update documents with literal fields', function(done) {
      fakeDatabase.items = [
        {a: 'value', b: 1, _id: id1},
        {a: 'value', b: 2, _id: id2}
      ];
      Item.collection.update(
        {a: 'value'},
        {a: 'new value', '$inc': {b: 10}},
        {multi: true},
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('err')
            .to.have.string('multi update only works with $ operators');
          done();
      });
    });
  });
});
