'use strict';

var _ = require('lodash');
var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('../test_harness');

// Chai uses properties rather than methods for assertions.
/* eslint-disable no-unused-expressions */

function getProjectionOption(projection) {
  if (parseInt(mongoose.version.split('.')[0]) <= 4) {
    return projection;
  } else {
    return {projection: projection};
  }
}

describe('find', function() {
  var expect = chai.expect;

  var id1 = new mongoose.Types.ObjectId();
  var id2 = new mongoose.Types.ObjectId();
  var id3 = new mongoose.Types.ObjectId();

  var fakeDatabase = {};
  var harness = new TestHarness({fakedb: fakeDatabase});

  before(function(done) {
    harness.setUpWithMongoClient(function(error) {
      if (error) return done(error);
      harness.items = harness.dbClient.db('fakedb').collection('items');
      done();
    });
  });

  after(function(done) {
    harness.tearDownWithMongoClient(done);
  });

  it('returns all documents', function(done) {
    fakeDatabase.items = [{key: 'value1'}, {key: 'value2'}];
    harness.items.find().toArray(function(error, items) {
      if (error) return done(error);
      expect(items).to.have.length(2);
      expect(items[0]).to.have.property('key', 'value1');
      expect(items[1]).to.have.property('key', 'value2');
      done();
    });
  });

  it('returns documents by query', function(done) {
    fakeDatabase.items = [{key: 'value1'}, {key: 'value2'}];
    harness.items.find({key: 'value1'}).toArray(function(error, items) {
      if (error) return done(error);
      expect(items).to.have.length(1);
      expect(items[0]).to.have.property('key', 'value1');
      done();
    });
  });

  it('returns no documents from non-existent collections', function(done) {
    delete fakeDatabase.items;
    harness.items.find({key: 'value1'}).toArray(function(error, items) {
      if (error) return done(error);
      expect(items).to.have.length(0);
      expect(fakeDatabase).to.not.have.property('items');
      done();
    });
  });

  it('does not create non-existent collections', function(done) {
    delete fakeDatabase.items;
    harness.items.find({key: 'value1'}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase).to.not.have.property('items');
      done();
    });
  });

  // Verify that the find doesn't hang when issueing the command second time
  // without waiting for results from the first .
  it('run twice', function(done) {
    fakeDatabase.items = [{key: 'value1'}, {key: 'value2'}];
    harness.items.find().toArray(function() {});
    harness.items.find().toArray(function(error, items) {
      if (error) return done(error);
      expect(items).to.have.length(2);
      expect(items[0]).to.have.property('key', 'value1');
      expect(items[1]).to.have.property('key', 'value2');
      done();
    });
  });

  it('supports $query', function(done) {
    fakeDatabase.items = [
      {key: 'value', key2: 2, _id: id2},
      {key: 'value', key2: 1, _id: id1}
    ];
    harness.items.find({key: 'value'})
      .sort({key2: 1})  // Calling sort causes MongoDB client to send $query.
      .toArray(function(error, results) {
        if (error) return done(error);
        expect(results).to.have.length(2);
        done();
    });
  });

  it('returns requested projection', function(done) {
    fakeDatabase.items = [
      {key: 'value', key2: {a: 'b', x: 'y'}, _id: id2},
      {key: 'value', key2: {a: 'c', z: 'x'}, _id: id1}
    ];
    harness.items.find(
      {key: 'value'},
      getProjectionOption({'key2.a': 1})
    ).toArray(function(error, results) {
      if (error) return done(error);

      expect(results).to.have.length(2);
      expect(_.omit(results[0], '_id')).to.deep.equal({key2: {a: 'b'}});
      expect(_.omit(results[1], '_id')).to.deep.equal({key2: {a: 'c'}});
      done();
    });
  });

  it('supports skip', function(done) {
    fakeDatabase.items = [
      {key: 'value1', key2: 1, _id: id1},
      {key: 'value2', key2: 2, _id: id2},
      {key: 'value3', key2: 3, _id: id3}
    ];
    harness.items.find({}).skip(1).toArray(function(error, items) {
      if (error) return done(error);
      expect(items).to.deep.equal(fakeDatabase.items.slice(1));
      done();
    });
  });

  it('supports limit', function(done) {
    fakeDatabase.items = [
      {key: 'value1', key2: 1, _id: id1},
      {key: 'value2', key2: 2, _id: id2},
      {key: 'value3', key2: 3, _id: id3}
    ];
    harness.items.find({}).limit(2).toArray(function(error, items) {
      if (error) return done(error);
      expect(items).to.deep.equal(fakeDatabase.items.slice(0, 2));
      done();
    });
  });

  it('supports skip together with limit', function(done) {
    fakeDatabase.items = [
      {key: 'value1', key2: 1, _id: id1},
      {key: 'value2', key2: 2, _id: id2},
      {key: 'value3', key2: 3, _id: id3}
    ];
    harness.items.find({}).skip(1).limit(1).toArray(function(error, items) {
      if (error) return done(error);
      expect(items).to.deep.equal(fakeDatabase.items.slice(1, 2));
      done();
    });
  });

  it('supports findOne', function(done) {
    fakeDatabase.items = [
      {key: 'value1', key2: 1, _id: id1},
      {key: 'value2', key2: 2, _id: id2},
      {key: 'value3', key2: 3, _id: id3}
    ];
    harness.items.findOne(
      {_id: new mongoose.Types.ObjectId(id3)},
      function(error, item) {
        if (error) return done(error);
        expect(item).to.exist;
        expect(item).to.deep.equal(fakeDatabase.items[2]);
        done();
      });
  });

  it('rejects invalid queries', function(done) {
    harness.items.find({'$eq': 'value24'}).toArray(function(error) {
      expect(error)
        .to.have.property('message')
        .to.match(/BadValue unknown top level operator: [$]eq/);
      done();
    });
  });

  it('rejects invalid requested projections', function(done) {
    harness.items.find({b: 1}, getProjectionOption({a: 1, b: 0}))
      .toArray(function(error) {
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
