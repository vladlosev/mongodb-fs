'use strict';

var chai = require('chai');
var mongoose = require('mongoose');

var TestHarness = require('../test_harness');

describe('remove', function() {
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
    fakeDatabase.items = [{key: 'value1'}, {key: 'value2'}];
    Item.remove({key: 'value1'}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.have.property('key', 'value2');
      done();
    });
  });

  it('removes documents by query', function(done) {
    fakeDatabase.items = [{key: 'value1'}, {key: 'value2'}];
    Item.remove({key: {$ne: 'value1'}}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.have.property('key', 'value1');
      done();
    });
  });

  it('does not create non-existent collection', function(done) {
    delete fakeDatabase.items;
    Item.remove({key: 'value1'}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase).to.not.have.property('items');
      done();
    });
  });
});
