'use strict';

var chai = require('chai');

chai.use(require('chai-properties'));

var TestHarness = require('../test_harness');

// Chai uses properties rather than methods for assertions.
/* eslint-disable no-unused-expressions */

describe('insert', function() {
  var expect = chai.expect;

  var fakeDatabase = {};
  var harness = new TestHarness({fakedb: fakeDatabase});

  before(function(done) {
    harness.setUp(function(error) {
      if (error) return done(error);
      harness.items = harness.dbClient.db('fakedb').collection('items');
      harness.systemIndexes = harness.dbClient.db('fakedb').collection('system.indexes');
      done();
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  it('adds documents to collection', function(done) {
    fakeDatabase.items = [];
    harness.items.insert({key: 'value'}, function(error) {
      if (error) return done(error);
      expect(fakeDatabase.items).to.have.length(1);
      expect(fakeDatabase.items[0])
        .to.have.property('key', 'value');
      done();
    });
  });

  it('creates collection if it does not exist', function(done) {
    delete fakeDatabase.items;
    harness.items.insert({key: 'value'}, function(error) {
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

      harness.systemIndexes.insert(
        [{key: {a: 1}, name: 'a_1', v: 1, ns: 'fakedb.items'}],
        function(error, result) {
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
