var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('../test_harness');

describe('count', function() {
  var expect = chai.expect;

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

  it('returns the number of queried documents', function(done) {
    fakeDatabase.items = [{key: 1}, {key: 2}, {key: 3}];
    Item.count({key: {$gt: 1}}, function(error, n) {
      if (error) return done(error);
      expect(n).to.equal(2);
      done();
    });
  });
});
