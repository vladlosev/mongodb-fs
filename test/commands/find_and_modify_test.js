'use strict';

var _ = require('lodash');
var chai = require('chai');
var mongodb = require('mongodb');

var TestHarness = require('../test_harness');

describe('findAndModify', function() {
  var expect = chai.expect;

  var id1 = new mongodb.ObjectId();
  var id2 = new mongodb.ObjectId();

  var fakeDatabase = {};
  var harness = new TestHarness({fakedb: fakeDatabase});

  var originalItems = [
    {a: 'value', b: 1, _id: id1},
    {a: 'value', b: 2, _id: id2}

  ];

  before(function(done) {
    harness.setUp(function(error) {
      if (error) return done(error);
      harness.items = harness.dbClient.db('fakedb').collection('items');
      done();
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  beforeEach(function() {
    fakeDatabase.items = _.cloneDeep(
      originalItems,
      function(value) {
        return value instanceof mongodb.ObjectId ?
          new mongodb.ObjectId(value) :
          undefined;
      });
  });

  it('finds and updates a document', function(done) {
    harness.items.findAndModify(
      {b: 1},
      {},
      {'$set': {a: 'new value'}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase.items)
          .to.deep.equal([
            {a: 'new value', b: 1, _id: id1},  // Has new value.
            originalItems[1]]);
        done();
    });
  });

  it('returns original document', function(done) {
    harness.items.findAndModify(
      {b: 1},
      {},
      {'$set': {a: 'new value'}},
      function(error, item) {
        if (error) return done(error);
        expect(item)
          .to.have.property('value')
          .to.deep.equal(originalItems[0]);
        done();
    });
  });

  it('returns requested projection of original document', function(done) {
    harness.items.findAndModify(
      {b: 1},
      null,
      {'$set': {a: 'new value'}},
      {fields: {a: 1}},
      function(error, item) {
        if (error) return done(error);
        expect(item)
          .to.have.property('value')
          .to.deep.equal({a: 'value', _id: id1});
        done();
    });
  });

  it('returns new document when new is set', function(done) {
    harness.items.findAndModify(
      {b: 1},
      {},
      {'$set': {a: 'new value'}},
      {'new': true},
      function(error, item) {
        if (error) return done(error);
        expect(item)
          .to.have.property('value')
          .to.deep.equal({a: 'new value', b: 1, _id: id1});
        done();
    });
  });

  it('returns requested projection of updated document when new is set',
    function(done) {
      harness.items.findAndModify(
        {b: 1},
        null,
        {'$set': {a: 'new value'}},
        {fields: {a: 1}, 'new': 1},
        function(error, item) {
          if (error) return done(error);
          expect(item)
            .to.have.property('value')
            .to.deep.equal({a: 'new value', _id: id1});
          done();
      });
  });

  it('does not create non-existent collection', function(done) {
    delete fakeDatabase.items;
    harness.items.findAndModify(
      {b: 1},
      {},
      {'$set': {a: 'new value'}},
      function(error) {
        if (error) return done(error);
        expect(fakeDatabase).to.not.have.property('items');
        done();
    });
  });

  it('rejects invalid filters', function(done) {
    harness.items.findAndModify(
      {'$eq': 2},
      null,
      {'$set': {a: 'new value'}},
      function(error) {
        expect(error).to.have.property('ok', false);
        expect(error).to.have.property('name', 'MongoError');
        expect(error)
          .to.have.property('message')
          .to.have.string('BadValue unknown top level operator: $eq');
        done();
    });
  });

  it('rejects invalid requested projections', function(done) {
    harness.items.findAndModify(
      {b: 1},
      null,
      {'$set': {a: 'new value'}},
      {fields: {a: 1, b: 0}},
      function(error) {
        expect(error).to.have.property('ok', false);
        expect(error).to.have.property('name', 'MongoError');
        expect(error)
          .to.have.property('message')
          .to.have.string(
            'You cannot currently mix including and excluding fields');

        // The collection must remain unchanged.
        expect(fakeDatabase.items).to.deep.equal(originalItems);
        done();
    });
  });

  it('returns null when document is not found', function(done) {
    harness.items.findAndModify(
      {b: 'non-existent'},
      {},
      {'$set': {a: 'new value'}},
      function(error, item) {
        if (error) return done(error);
        expect(item)
          .to.have.property('value')
          .to.equal(null);
        done();
    });
  });

  it('fails when update document is not specified', function(done) {
    harness.items.findAndModify(
      {b: 1},
      function(error) {
        expect(error).to.have.property('ok', false);
        expect(error).to.have.property('name', 'MongoError');
        expect(error)
          .to.have.property('message')
          .to.have.string('need remove or update');

        // The collection must remain unchanged.
        expect(fakeDatabase.items).to.deep.equal(originalItems);
        done();
    });
  });

  it('fails when operators follow fields in update', function(done) {
    harness.items.findAndModify(
      {b: 1},
      {},
      {a: 'new value', $set: {b: 5}},
      function(error) {
        expect(error).to.have.property('ok', false);
        expect(error).to.have.property('name', 'MongoError');
        expect(error)
          .to.have.property('message')
          .to.have.string(
            "exception: The dollar ($) prefixed field '$set' " +
            "in '$set' is not valid for storage.");

        // The collection must remain unchanged.
        expect(fakeDatabase.items).to.deep.equal(originalItems);
        done();
    });
  });

  it('fails when fields follow operators in update', function(done) {
    harness.items.findAndModify(
      {b: 1},
      {},
      {$set: {b: 5}, a: 'new value'},
      function(error) {
        expect(error).to.have.property('ok', false);
        expect(error).to.have.property('name', 'MongoError');
        expect(error)
          .to.have.property('message')
          .to.have.string('exception: Unknown modifier: a');

        // The collection must remain unchanged.
        expect(fakeDatabase.items).to.deep.equal(originalItems);
        done();
    });
  });

  it('fails when direct field asignments use dot notation', function(done) {
    harness.items.findAndModify(
      {b: 1},
      {},
      {'y.z': 5},
      function(error) {
        expect(error).to.have.property('ok', false);
        expect(error).to.have.property('name', 'MongoError');
        expect(error)
          .to.have.property('message')
          .to.have.string(
            "exception: The dotted field 'y.z' in 'y.z' " +
            'is not valid for storage.');

        // The collection must remain unchanged.
        expect(fakeDatabase.items).to.deep.equal(originalItems);
        done();
    });
  });

  xit('updates the first document in specified sort order', function(done) {
    done();
  });

  xit('updates the first document in the default sort order', function(done) {
    done();
  });

  describe('with remove option set', function() {
    it('deletes the found document', function(done) {
      harness.items.findAndModify(
        {b: 1},
        null,
        null,
        {remove: true},
        function(error) {
          if (error) return done(error);
          // The first record is deleted.
          expect(fakeDatabase.items).to.deep.equal(originalItems.slice(1, 2));
          done();
      });
    });

    it('returns the deleted document', function(done) {
      harness.items.findAndModify(
        {b: 1},
        null,
        null,
        {remove: true},
        function(error, item) {
          if (error) return done(error);
          // The first record was deleted and is returned.
          expect(item)
            .to.have.property('value')
            .to.deep.equal(originalItems[0]);
          done();
      });
    });

    it('ignores update document', function(done) {
      harness.items.findAndModify(
        {b: 1},
        null,
        {a: 'new value'},
        {remove: true},
        function(error) {
          if (error) return done(error);

          // The first record is deleted even though update is specified.
          expect(fakeDatabase.items).to.deep.equal(originalItems.slice(1, 2));
          done();
      });
    });

    it('fails when new is specified', function(done) {
      harness.items.findAndModify(
        {b: 1},
        null,
        null,
        {remove: true, 'new': true},
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('message')
            .to.have.string("remove and returnNew can't co-exist");

          // The collection must remain unchanged.
          expect(fakeDatabase.items).to.deep.equal(originalItems);
          done();
      });
    });

    xit('removes first document in specified sort order', function(done) {
      done();
    });

    xit('removes first document in default sort order', function(done) {
      done();
    });
  });

  describe('with upsert options set', function() {
    it('updates existing document', function(done) {
      harness.items.findAndModify(
        {b: 1},
        null,
        {'$set': {a: 'new value'}},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items)
            .to.deep.equal([
              {a: 'new value', b: 1, _id: id1},  // Has new value.
              originalItems[1]]);
          done();
      });
    });

    it('inserts new document when no matches', function(done) {
      harness.items.findAndModify(
        {b: 3},
        null,
        {'$set': {a: 'new value'}},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(3);
          expect(fakeDatabase.items.slice(0, 2))
            .to.deep.equal(originalItems);
          expect(fakeDatabase.items[2])
            .to.have.deep.property('_id.constructor.name', 'ObjectID');
          expect(_.omit(fakeDatabase.items[2], '_id'))
            .to.deep.equal({b: 3, a: 'new value'});
          done();
      });
    });

    it('returns null when upserting', function(done) {
      harness.items.findAndModify(
        {b: 3},
        null,
        {'$set': {a: 'new value'}},
        {upsert: true},
        function(error, item) {
          if (error) return done(error);
          expect(item)
            .to.have.property('value')
            .to.equal(null);
          done();
      });
    });

    it('returns new document when upserting and new is set', function(done) {
      harness.items.findAndModify(
        {b: 3},
        null,
        {'$set': {a: 'new value'}},
        {upsert: true, 'new': true},
        function(error, item) {
          if (error) return done(error);
          expect(item).to.have.property('value');
          expect(item.value)
            .to.have.deep.property('_id.constructor.name', 'ObjectID');
          expect(_.omit(item.value, '_id'))
            .to.deep.equal({b: 3, a: 'new value'});
          done();
      });
    });

    it('returns requested projection of new document with new set',
      function(done) {
        harness.items.findAndModify(
          {b: 3},
          null,
          {'$set': {a: 'new value'}},
          {fields: {a: 1}, upsert: true, 'new': true},
          function(error, item) {
            if (error) return done(error);
            expect(item).to.have.property('value');
            expect(item.value)
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            expect(_.omit(item.value, '_id')).to.deep.equal({a: 'new value'});
            done();
        });
    });

    it('creates new collection if it does not exist', function(done) {
      delete fakeDatabase.items;
      harness.items.findAndModify(
        {b: 3},
        null,
        {'$set': {a: 'new value'}},
        {upsert: true},
        function(error) {
          if (error) return done(error);
          expect(fakeDatabase.items).to.have.length(1);
          expect(_.omit(fakeDatabase.items[0], '_id'))
            .to.deep.equal({b: 3, a: 'new value'});
          done();
      });
    });
  });
});
