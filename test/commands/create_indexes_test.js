'use strict';

var chai = require('chai');

chai.use(require('chai-properties'));

var TestHarness = require('../test_harness');

describe('createIndex', function() {
  var fakeDatabase = {};
  var harness = new TestHarness({fakedb: fakeDatabase});

  before(function(done) {
    harness.setUp(function(error) {
      if (error) return done(error);
      harness.db = harness.dbClient.db('fakedb');
      harness.items = harness.db.collection('items');
      done();
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  beforeEach(function() {
    fakeDatabase.items = [{a: 'x', b: 'y'}];
    fakeDatabase['system.indexes'] = [{
      v: 1,
      key: {_id: 1},
      name: '_id_',
      ns: 'fakedb.items'
    }];
  });

  it('creates an index', function(done) {
    harness.items.createIndex(
      {a: 1},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.deep.equal({
            v: 1,
            key: {a: 1},
            name: 'a_1',
            ns: 'fakedb.items'
          });
        done();
    });
  });

  // We need this to verify the full output of the createIndexes command.
  // Mongoose only returns the index name.
  it('creates an index using raw command', function(done) {
    var util = require('util');
    harness.dbClient.db('fakedb').command(
      {createIndexes: 'items', indexes: [{key: {a: 1}}]},
      function(error, results) {
        if (error) return done(error);

        chai.expect(results).to.deep.equal({
          createdCollectionAutomatically: false,
          numIndexesBefore: 1,
          numIndexesAfter: 2,
          ok: 1
        });
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.deep.equal({
            v: 1,
            key: {a: 1},
            name: 'a_1',
            ns: 'fakedb.items'
          });
        done();
    });
  });

  it('creates multiple indices using raw command', function(done) {
    harness.dbClient.db('fakedb').command(
      {createIndexes: 'items', indexes: [{key: {a: 1}}, {key: {b: 1}}]},
      function(error, results) {
        if (error) return done(error);

        chai.expect(results).to.deep.equal({
          createdCollectionAutomatically: false,
          numIndexesBefore: 1,
          numIndexesAfter: 3,
          ok: 1
        });
        chai.expect(fakeDatabase['system.indexes']).to.have.length(3);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.deep.equal({v: 1, key: {a: 1}, name: 'a_1', ns: 'fakedb.items'});
        chai.expect(fakeDatabase['system.indexes'][2])
          .to.deep.equal({v: 1, key: {b: 1}, name: 'b_1', ns: 'fakedb.items'});
        done();
    });
  });

  it('creates index if there are other indexes already', function(done) {
    fakeDatabase['system.indexes'].push({
      v: 1,
      key: {a: 1},
      name: 'a_1',
      ns: 'fakedb.items'
    });
    harness.items.createIndex(
      {b: 1},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('b_1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(3);
        chai.expect(fakeDatabase['system.indexes'][2])
          .to.have.property('key')
          .to.deep.equal({b: 1});
        done();
    });
  });

  it('reports success if index already exists', function(done) {
    fakeDatabase['system.indexes'].push({
      v: 1,
      key: {a: 1},
      name: 'a_1',
      ns: 'fakedb.items'
    });
    harness.items.createIndex(
      {a: 1},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        done();
    });
  });

  it('creates a collection if it did not exist', function(done) {
    delete fakeDatabase.items;
    fakeDatabase['system.indexes'] = [];

    harness.dbClient.db('fakedb').command(
      {createIndexes: 'items', indexes: [{key: {a: 1}}]},
      function(error, results) {
        if (error) return done(error);

        chai.expect(results).to.deep.equal({
          createdCollectionAutomatically: true,
          numIndexesBefore: 1,
          numIndexesAfter: 2,
          ok: 1
        });
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        // _id index is ctreated automatically.
        chai.expect(fakeDatabase['system.indexes'][0])
          .to.deep.equal({
            v: 1,
            key: {'_id': 1},
            name: '_id_',
            ns: 'fakedb.items'
          });
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('key')
          .to.deep.equal({a: 1});
        done();
    });
  });

  it('creates a compound index', function(done) {
    harness.items.createIndex(
      {a: 1, b: -1},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_1_b_-1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.deep.equal({
            v: 1,
            key: {a: 1, b: -1},
            name: 'a_1_b_-1',
            ns: 'fakedb.items'
          });
        done();
    });
  });

  it('creates index on embedded field', function(done) {
    harness.items.createIndex(
      {a: 1, 'b.c': -1},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_1_b.c_-1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('key')
          .to.deep.equal({a: 1, 'b.c': -1});
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('name', 'a_1_b.c_-1');
        done();
    });
  });

  it('creates a background index', function(done) {
    harness.items.createIndex(
      {a: 1},
      {background: true},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('key')
          .to.deep.equal({a: 1});
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('background', true);
        done();
    });
  });

  it('creates a unique index', function(done) {
    harness.items.createIndex(
      {a: 1},
      {unique: true},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('key')
          .to.deep.equal({a: 1});
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('unique', true);
        done();
    });
  });

  it('creates a sparse index', function(done) {
    harness.items.createIndex(
      {a: 1},
      {sparse: true},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_1');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('key')
          .to.deep.equal({a: 1});
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('sparse', true);
        done();
    });
  });

  it('creates a hashed index', function(done) {
    harness.items.createIndex(
      {a: 'hashed'},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_hashed');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('key')
          .to.deep.equal({a: 'hashed'});
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('name', 'a_hashed');
        done();
    });
  });

  it('creates separate hashed index on already indexed field', function(done) {
    fakeDatabase['system.indexes'].push({
      v: 1,
      key: {a: 1},
      name: 'a_1',
      ns: 'fakedb.items'
    });
    harness.items.createIndex(
      {a: 'hashed'},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('a_hashed');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(3);
        chai.expect(fakeDatabase['system.indexes'][2])
          .to.have.property('key')
          .to.deep.equal({a: 'hashed'});
        chai.expect(fakeDatabase['system.indexes'][2])
          .to.have.property('name', 'a_hashed');
        done();
    });
  });

  it('reports success if hashed index already exists', function(done) {
    fakeDatabase['system.indexes'].push({
      v: 1,
      key: {a: 'hashed'},
      name: 'a_hashed',
      ns: 'fakedb.items'
    });
    harness.dbClient.db('fakedb').command(
      {createIndexes: 'items', indexes: [{key: {a: 'hashed'}}]},
      function(error, results) {
        if (error) return done(error);

        chai.expect(results).to.deep.equal({
          numIndexesBefore: 2,
          note: 'all indexes already exist',
          ok: 1
        });
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        done();
    });
  });

  it('accepts custom index name', function(done) {
    harness.items.createIndex(
      {a: 1},
      {name: 'totally custom name'},
      function(error, result) {
        if (error) return done(error);

        chai.expect(result).to.equal('totally custom name');
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('key').to.deep.equal({a: 1});
        chai.expect(fakeDatabase['system.indexes'][1])
          .to.have.property('name', 'totally custom name');
        done();
    });
  });

  it('rejects index on same fields with different options', function(done) {
    fakeDatabase['system.indexes'].push({
      v: 1,
      key: {a: 1},
      name: 'totally custom name',
      ns: 'fakedb.items'
    });
    harness.items.createIndex(
      {a: 1},
      {unique: true},
      function(error) {
        chai.expect(error).to.have.properties({
          ok: 0,
          errmsg: 'Index with pattern: { a: 1 } ' +
                  'already exists with different options',
          code: 85
        });
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        done();
    });
  });

  it('rejects index on different fields with the same name', function(done) {
    fakeDatabase['system.indexes'].push({
      v: 1,
      key: {a: 1},
      name: 'totally custom name',
      ns: 'fakedb.items'
    });
    harness.items.createIndex(
      {b: 1},
      {name: 'totally custom name'},
      function(error) {
        chai.expect(error)
          .to.be.instanceof(Error)
          .and.to.have.properties({
            ok: 0,
            errmsg: 'Trying to create an index with same name ' +
                    'totally custom name with different key spec { b: 1 } ' +
                    'vs existing spec { a: 1 }',
            code: 86
          });
        chai.expect(fakeDatabase['system.indexes']).to.have.length(2);
        done();
    });
  });

  it('rejects compound index with a hashed field', function(done) {
    harness.items.createIndex(
      {a: 1, b: 'hashed'},
      function(error) {
        chai.expect(error)
          .to.be.instanceof(Error)
          .and.to.have.properties({
            createdCollectionAutomatically: false,
            numIndexesBefore: 1,
            errmsg: 'exception: Currently only single field hashed index ' +
                    'supported.',
            ok: 0
          });
        chai.expect(fakeDatabase['system.indexes']).to.have.length(1);
        done();
    });
  });
});
