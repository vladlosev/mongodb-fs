var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('../test_harness');

describe('insert', function() {
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

  it('basic', function(done) {
    fakeDatabase.items = [];
    Item.collection.insert({key: 'value'}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.have.property('key', 'value');
      done();
    });
  });
});
