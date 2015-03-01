var _ = require('lodash');
var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('../test_harness');

describe('find', function() {
  var expect = chai.expect;

  var id1 = new mongoose.Types.ObjectId();
  var id2 = new mongoose.Types.ObjectId();

  var fakeDatabase = {};
  var Item;

  before(function(done) {
    var harness = new TestHarness({fakedb: fakeDatabase});
    harness.setUp(function(error) {
      Item = mongoose.connection.models.Item;
      done(error);
    });
  });

  after(function(done) {
    TestHarness.tearDown(done);
  });

  // Verify that the find doesn't hang when issueing the command second time
  // without waiting for results from the first .
  it('run twice', function(done) {
    fakeDatabase.items = [{key: 'value1'}, {key: 'value2'}];
    Item.find(function(error, items) {});
    Item.find(function(error, items) {
      if (error) return done(error);
      expect(items).to.have.length(2);
      expect(items[0].toObject()).to.have.property('key', 'value1');
      expect(items[1].toObject()).to.have.property('key', 'value2');
      done();
    });
  });

  it('supports $query', function(done) {
    fakeDatabase.items = [
      {key: 'value', key2: 2, _id: id2},
      {key: 'value', key2: 1, _id: id1}];
    Item.collection.find({key: 'value'})
      .sort({key2: 1})  // Calling sort causes MongoDB client to send $query.
      .toArray(function(error, results) {
        if (error) return done(error);
        expect(results).to.have.length(2);
        done();
    });
  });

  it('returns requested projection', function(done) {
    fakeDatabase.items = [
      {key: 'value', key2: {a: 'b'}, _id: id2},
      {key: 'value', key2: {a: 'c'}, _id: id1}
    ];
    Item.collection.find({key: 'value'}, {'key2.a': 1}).toArray(
      function(error, results) {
        if (error) return done(error);

        expect(results).to.have.length(2);
        expect(_.omit(results[0], '_id')).to.deep.equal({key2: {a: 'b'}});
        expect(_.omit(results[1], '_id')).to.deep.equal({key2: {a: 'c'}});
        done();
    });
  });

  it('rejects invalid requested projections', function(done) {
    Item.collection.find({b: 1}, {a: 1, b: 0}).toArray(function(error) {
      expect(error).to.have.property('name', 'MongoError');
      expect(error)
        .to.have.property('message')
        .to.have.string(
          'BadValue Projection cannot have ' +
          'a mix of inclusion and exclusion.');
      done();
    });
  });
});
