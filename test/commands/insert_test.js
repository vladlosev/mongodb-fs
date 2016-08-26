'use strict';

var chai = require('chai');
var mongoose = require('mongoose');

chai.use(require('chai-properties'));

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

  it('adds documents to collection', function(done) {
    fakeDatabase.items = [];
    Item.collection.insert({key: 'value'}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.have.property('key', 'value');
      done();
    });
  });

  it('creates collection if it does not exist', function(done) {
    delete fakeDatabase.items;
    Item.collection.insert({key: 'value'}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.have.property('key', 'value');
      done();
    });
  });

  it('runs CreateIndexes if asked to insert into system.indexes',
    function(done) {
      fakeDatabase['system.indexes'] = [];

      var Index = mongoose.model(
        'Index',
        new mongoose.Schema(
          {any: mongoose.Schema.Types.Mixed},
          {collection: 'system.indexes', versionKey: false, _id: false}));
      Index.collection.insert(
        [{key: {a: 1}, name: 'a_1', v: 1, ns: 'fakedb.items'}],
        function(error, result) {
          delete mongoose.connection.models.Index;
          if (error) return done(error);

          expect(result).to.have.deep.property('result.ok').to.be.ok;

          chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
          chai.expect(fakeDatabase['system.indexes'][0])
            .to.have.properties({
              key: {_id: 1},
              name: '_id_',
              ns: 'fakedb.items'
            });
          chai.expect(fakeDatabase['system.indexes'][1])
            .to.have.properties({
              v: 1,
              key: {a: 1},
              name: 'a_1',
              ns: 'fakedb.items'
            });
          done();
        });
  });
});
