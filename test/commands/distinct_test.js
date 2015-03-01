var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('../test_harness');

describe('distinct', function() {
  var expect = chai.expect;

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

  it('finds distinct field values', function(done) {
    fakeDatabase.items = [{key: 1}, {key: 2}, {key: 3}, {key2: 4}];
    Item.collection.distinct('key', function(error, values) {
      if (error) return done(error);
      expect(values).to.deep.equal([1, 2, 3]);
      done();
    });
  });

  it('finds distinct subfiled values', function(done) {
    fakeDatabase.items = [
      {a: {b: 'x'}, c: 1},
      {a: {b: 'y'}},
      {a: {c: 'z'}}
    ];
    Item.collection.distinct('a.b', function(error, values) {
      if (error) return done(error);
      expect(values).to.deep.equal(['x', 'y']);
      done();
    });
  });

  it('finds distinct compound values', function(done) {
    fakeDatabase.items = [
      {a: {b: 'x'}, c: 1},
      {a: {b: 'y'}},
      {a: ['x', 42]}
    ];
    Item.collection.distinct('a', function(error, values) {
      if (error) return done(error);
      expect(values).to.deep.equal([{b: 'x'}, {b: 'y'}, ['x', 42]]);
      done();
    });
  });

  it('supports filtering', function(done) {
    fakeDatabase.items = [{key: 1}, {key: 2}, {key: 3}];
    Item.collection.distinct(
      'key',
      {key: {'$gt': 1}},
      function(error, values) {
        if (error) return done(error);
        expect(values).to.deep.equal([2, 3]);
        done();
      });
  });

  it('handles empty collection', function(done) {
    fakeDatabase.items = [];
    Item.collection.distinct('key', function(error, values) {
      if (error) return done(error);
      expect(values).to.deep.equal([]);
      done();
    });
  });

  it('handles non-existent collection', function(done) {
    delete fakeDatabase.items;
    Item.collection.distinct('key', function(error, values) {
      if (error) return done(error);
      expect(values).to.deep.equal([]);
      done();
    });
  });

  it('does not create non-existent collection', function(done) {
    delete fakeDatabase.items;
    Item.collection.distinct('key', function(error, values) {
      if (error) return done(error);
      expect(fakeDatabase).to.not.have.property('items');
      done();
    });
  });

  it('returns empty array if field parameter is invalid', function(done) {
    fakeDatabase.items = [{key: 1}, {key: 2}, {key: 3}];
    Item.collection.distinct(42, function(error, values) {
      if (error) return done(error);
      expect(values).to.deep.equal([]);
      done();
    });
  });

  it('ignores invalid query parameter', function(done) {
    fakeDatabase.items = [{key: 1}, {key: 2}, {key: 3}];
    Item.collection.distinct('key', 3, function(error, values) {
      if (error) return done(error);
      expect(values).to.deep.equal([1, 2, 3]);
      done();
    });
  });
});
