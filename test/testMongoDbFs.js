'use strict';

var _ = require('lodash');
var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('./test_harness');

var ObjectId = mongoose.Types.ObjectId;

var Item;

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
      delete mongoose.connection.models.Item;
      Item = mongoose.model('Item', {key: Number, compound: {subkey: Number}});
      done();
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  beforeEach(function(done) {
    // Restore the database to the original state to make tests
    // order-independent.
    Item.remove({}, function(error) {
      if (error) return done(error);
      // Use copies of the original mock objects to avoid one test affecting
      // others by modifying objects in the database.
      Item.collection.insert(fakeDatabase.items, done);
    });
  });

  describe('find', function() {
    it('finds all documents', function(done) {
      Item.find(function(error, items) {
        if (error) return done(error);
        expect(_.pluck(items, 'key')).to.deep.equal([1, 2, 3]);
        done();
      });
    });

    it('finds documents by query', function(done) {
      Item.find({key: {'$gt': 1}}, function(error, items) {
        if (error) return done(error);
        expect(_.pluck(items, 'key')).to.deep.equal([2, 3]);
        done();
      });
    });
  });

  describe('findAndUpdate', function() {
    it('basic', function(done) {
      Item.findByIdAndUpdate(
        {_id: fakeDatabase.items[1]._id},
        {'$set': {key: 18}},
        function(error, item) {
          if (error) return done(error);
          expect(item).to.have.property('key', 18);
          done();
      });
    });
  });

  describe('insert', function() {
    it('saves document to collection', function(done) {
      var item = new Item({key: 4});
      item.save(function(error, savedItem) {
        if (error) return done(error);
        expect(savedItem).to.exist();
        Item.findById(savedItem._id, function(error, newItem) {
        if (error) return done(error);
          expect(newItem).to.exist;
          expect(newItem.toObject()).to.deep.equal(savedItem.toObject());
          done();
        });
      });
    });
  });

  describe('remove', function() {
    it('removes document from collection', function(done) {
      Item.findOne({'key': 2}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist();
        item.remove(function(error) {
          if (error) return done(error);
          Item.findById(item._id, function(error, noItem) {
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
      Item.findOne({key: 1}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist();
        item.key = 42;
        item.save(function(error) {
          if (error) return done(error);
          Item.findById(item._id, function(error, newItem) {
            if (error) return done(error);
            expect(newItem).to.exist();
            expect(newItem.toObject()).to.deep.equal(item.toObject());
            done();
          });
        });
      });
    });
  });
});
