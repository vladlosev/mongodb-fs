'use strict';

var chai = require('chai');

var TestHarness = require('../test_harness');

describe('count', function() {
  var expect = chai.expect;

  var fakeDatabase = {};
  var harness = new TestHarness({fakedb: fakeDatabase});

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

  it('returns the number of queried documents', function(done) {
    fakeDatabase.items = [{key: 1}, {key: 2}, {key: 3}];
    harness.items.count({key: {$gt: 1}}, function(error, n) {
      if (error) return done(error);
      expect(n).to.equal(2);
      done();
    });
  });

  it('reports count of non-existent collection as zero', function(done) {
    delete fakeDatabase.items;
    harness.items.count({}, function(error, n) {
      if (error) return done(error);
      expect(n).to.equal(0);
      done();
    });
  });

  it('does not create a collection if non-existent', function(done) {
    delete fakeDatabase.items;
    harness.items.count({}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase).to.not.have.property('items');
      done();
    });
  });
});
