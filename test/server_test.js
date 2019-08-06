'use strict';

var _ = require('lodash');
var chai = require('chai');
var mongodb = require('mongodb');

var TestHarness = require('./test_harness');

var ObjectId = mongodb.ObjectId;

var fakeDatabase = {
  items: [
    {key: 1, _id: new ObjectId()},
    {key: 2, compound: {subkey: 21}, _id: new ObjectId()},
    {key: 3, compound: {subkey: 31}, _id: new ObjectId()}
  ]
};

// Chai uses properties rather than methods for assertions.
/* eslint-disable no-unused-expressions */

describe('Works in forked mode', function() {
  var expect = chai.expect;
  var harness = new TestHarness({fakedb: fakeDatabase});

  before(function(done) {
    harness.config.fork = true;
    harness.setUp(function(error) {
      if (error) return done(error);
      harness.items = harness.dbClient.db('fakedb').collection('items');
      done();
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  beforeEach(function(done) {
    // Restore the database to the original state to make tests
    // order-independent.
    harness.items.remove({}, function(error) {
      if (error) return done(error);
      // Use copies of the original mock objects to avoid one test affecting
      // others by modifying objects in the database.
      harness.items.insert(fakeDatabase.items, done);
    });
  });

  describe('find', function() {
    it('finds all documents', function(done) {
      harness.items.find().toArray(function(error, items) {
        if (error) return done(error);
        expect(_.pluck(items, 'key')).to.deep.equal([1, 2, 3]);
        done();
      });
    });

    it('finds documents by query', function(done) {
      harness.items.find({key: {'$gt': 1}}).toArray(function(error, items) {
        if (error) return done(error);
        expect(_.pluck(items, 'key')).to.deep.equal([2, 3]);
        done();
      });
    });
  });

  describe('findAndModify', function() {
    it('basic', function(done) {
      harness.items.findAndModify(
        {_id: fakeDatabase.items[1]._id},
        {},
        {'$set': {key: 18}},
        {'new': true},
        function(error, item) {
          if (error) return done(error);
          expect(item).to.have.property('value');
          expect(item.value).to.have.property('key', 18);
          done();
      });
    });
  });

  describe('insert', function() {
    it('inserts document into collection', function(done) {
      var item = {key: 4};
      harness.items.insert(item, function(error, result) {
        if (error) return done(error);
        expect(result).to.have.deep.property('result.n', 1);
        harness.items.findOne({_id: result.ops[0]._id}, function(error, newItem) {
        if (error) return done(error);
          expect(newItem).to.exist;
          expect(newItem).to.deep.equal(result.ops[0]);
          done();
        });
      });
    });
  });

  describe('remove', function() {
    it('removes document from collection', function(done) {
      harness.items.findOne({'key': 2}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist;
        harness.items.remove(function(error) {
          if (error) return done(error);
          harness.items.findOne({_id: item._id}, function(error, noItem) {
            if (error) return done(error);
            expect(noItem).to.not.exist;
            done();
          });
        });
      });
    });
  });

  describe('update', function() {
    it('updates documents', function(done) {
      harness.items.findOne({key: 1}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist;
        item.key = 42;
        harness.items.update({_id: item._id}, item, function(error) {
          if (error) return done(error);
          harness.items.findOne({_id: item._id}, function(error, newItem) {
            if (error) return done(error);
            expect(newItem).to.exist;
            expect(newItem).to.deep.equal(item);
            done();
          });
        });
      });
    });
  });
});
