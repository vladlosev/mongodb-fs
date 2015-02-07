var _ = require('lodash');
var util = require('util')
  , chai = require('chai')
  , path = require('path')
  , mongodbFs = require('../lib/mongodb-fs')
  , mongoose = require('mongoose')
  , log = require('../lib/log')
  , config, logger, schema, dbConfig, dbOptions, SimpleItem, Unknown;

var logLevel = process.env.LOG_LEVEL || 'WARN';

config = {
  port: 27027,
  mocks: {
    fakedb: {
    }
  },
  fork: false,
  log: {
    log4js: {
      appenders: [
        {
          type: 'console',
          category: path.basename(__filename)
        }
      ]
    },
    category: path.basename(__filename),
    level: logLevel
  }
};

log.init(config.log);
logger = log.getLogger();

dbConfig = {
  name: 'fakedb'
};
dbConfig.url = util.format('mongodb://localhost:%d/%s', config.port, dbConfig.name);

dbOptions = {
  server: { poolSize: 1 }
};

mongoose.model('FreeItem', new mongoose.Schema({any: mongoose.Schema.Types.Mixed}));
mongoose.model('SimpleItem', new mongoose.Schema({key: String}));
mongoose.model('ArrayItem', new mongoose.Schema({key: [String], key2: [String]}));
mongoose.model('DateItem', new mongoose.Schema({date: Date}));
mongoose.model('DateArrayItem', new mongoose.Schema({date: [Date]}));
mongoose.model('NumberItem', new mongoose.Schema({key: Number}));
mongoose.model('ArrayObjectIdItem', new mongoose.Schema({key: [mongoose.Types.ObjectId]}));

var FreeItem, SimpleItem, ArrayItem;

describe('MongoDb-Fs in-process operations do not hang', function() {
  var expect = chai.expect;

  before(function(done) {
    mongodbFs.init(config);
    logger.trace('init');
    mongodbFs.start(function(err) {
      if (err) return done(err);
      logger.trace('connect to db');
      mongoose.set('debug', logLevel === 'TRACE');
      mongoose.connect(dbConfig.url, dbOptions, function(err) {
        if (err) {
          mongodbFs.stop(function() { done(err); });
          return;
        }
        FreeItem = mongoose.connection.model('FreeItem');
        SimpleItem = mongoose.connection.model('SimpleItem');
        ArrayItem = mongoose.connection.model('ArrayItem');
        done();
      });
    });
  });

  after(function(done) {
    logger.trace('disconnect');
    mongoose.disconnect(function() {
      mongodbFs.stop(done);
    });
  });

  beforeEach(function() {
    delete config.mocks.fakedb.simpleitems;
    delete config.mocks.fakedb.arrayitems;
    delete config.mocks.fakedb.dateitems;
    delete config.mocks.fakedb.datearrayitems;
    delete config.mocks.fakedb.numberitems;
    delete config.mocks.fakedb.arrayobjectiditems;
  });

  describe('find', function() {
    var id1 = new mongoose.Types.ObjectId();
    var id2 = new mongoose.Types.ObjectId();

    it('run twice', function(done) {
      config.mocks.fakedb.simpleitems = [{key: 'value1'}, {key: 'value2'}];
      SimpleItem.find(function (error, items) {});
      SimpleItem.find(function (error, items) {
        if (error) return done(error);
        expect(items).to.have.length(2);
        expect(items[0]).to.have.property('key', 'value1');
        expect(items[1]).to.have.property('key', 'value2');
        done();
      });
    });

    it('supports $query', function(done) {
      config.mocks.fakedb.freeitems = [
        {key: 'value', key2: 2, _id: id2},
        {key: 'value', key2: 1, _id: id1}];
      FreeItem.collection.find({key: 'value'})
        .sort({key2: 1})  // Calling sort causes MongoDB client to send $query.
        .toArray(function(error, results) {
          if (error) return done(error);
          expect(results).to.have.length(2);
          done();
      });
    });

    it('returns requested projection', function(done) {
      config.mocks.fakedb.freeitems = [
        {key: 'value', key2: {a: 'b'}, _id: id2},
        {key: 'value', key2: {a: 'c'}, _id: id1}
      ];
      FreeItem.collection.find({key: 'value'}, {'key2.a': 1}).toArray(
        function(error, results) {
          if (error) return done(error);

          expect(results).to.have.length(2);
          expect(_.omit(results[0], '_id')).to.deep.equal({key2: {a: 'b'}});
          expect(_.omit(results[1], '_id')).to.deep.equal({key2: {a: 'c'}});
          done();
      });
    });

    it('rejects invalid requested projections', function(done) {
      FreeItem.collection.find({b: 1}, {a: 1, b: 0}).toArray(function(error) {
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

  describe('remove', function() {
    it('basic', function(done) {
      config.mocks.fakedb.simpleitems = [{key: 'value1'}, {key: 'value2'}];
      SimpleItem.remove({key: 'value1'}, function(error) {
        if (error) return done(error);
        expect(config.mocks.fakedb.simpleitems).to.have.length(1);
        expect(config.mocks.fakedb.simpleitems[0])
          .to.have.property('key', 'value2');
        done();
      });
    });

    it('by query', function(done) {
      config.mocks.fakedb.simpleitems = [{key: 'value1'}, {key: 'value2'}];
      SimpleItem.remove({key: {$ne: 'value1'}}, function(error) {
        if (error) return done(error);
        expect(config.mocks.fakedb.simpleitems).to.have.length(1);
        expect(config.mocks.fakedb.simpleitems[0])
          .to.have.property('key', 'value1');
        done();
      });
    });

  });

  describe('insert', function() {
    it('basic', function(done) {
      config.mocks.fakedb.simpleitems = [];
      var item = new SimpleItem({key: 'value'});
      item.save(function(error) {
        if (error) return done(error);
        expect(config.mocks.fakedb.simpleitems).to.have.length(1);
        expect(config.mocks.fakedb.simpleitems[0])
          .to.have.property('key', 'value');
        done();
      });
    });
  });

  describe('update', function() {
    it('basic', function(done) {
      config.mocks.fakedb.freeitems = [
      {key: 'value1', _id: new mongoose.Types.ObjectId},
      {key: 'value2', _id: new mongoose.Types.ObjectId}];
      FreeItem.collection.update(
        {key: 'value1'},
        {'$set': {key: 'new value'}},
        function(error) {
          if (error) return done(error);
          expect(config.mocks.fakedb.freeitems).to.have.length(2);
          expect(config.mocks.fakedb.freeitems[0])
            .to.have.property('key', 'new value');
          done();
      });
    });

    it('replaces fields', function(done) {
      var id = new mongoose.Types.ObjectId();
      config.mocks.fakedb.freeitems = [{a: 'value', b: 1, c: 'there', _id: id}];
      FreeItem.collection.update(
        {a: 'value'},
        {a: 'new value', '$inc': {b: 1}},
        function(err) {
          if (err) return done(err);
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal([{a: 'new value', b: 2, c: 'there', _id: id}]);
          done();
      });
    });

    it('replaces subfields', function(done) {
      var id = new mongoose.Types.ObjectId();
      config.mocks.fakedb.freeitems = [{a: 'value1', b: {c: 1}, _id: id}];
      FreeItem.collection.update(
        {a: 'value1'},
        {'$set': {'b.c': 42}},
        function(err) {
          if (err) return done(err);
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal([{a: 'value1', b: {c: 42}, _id: id}]);
          done();
      });
    });

    it('rejects subfield literals', function(done) {
      var id = new mongoose.Types.ObjectId();
      config.mocks.fakedb.freeitems = [{a: 'value1', b: {c: 1}, _id: id}];
      FreeItem.collection.update(
        {a: 'value1'},
        {'b.c': 42},
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('err')
            .to.have.string("can't have . in field names [b.c]");
          done();
      });
    });

    it('replaces entire documents', function(done) {
      var id = new mongoose.Types.ObjectId();
      config.mocks.fakedb.freeitems = [{a: 'value1', b: 1, _id: id}];
      FreeItem.collection.update({a: 'value1'}, {b: 42}, function(err) {
        if (err) return done(err);
        // The subfield a.c should be gone as the update document does not
        // specify any operators.
        expect(config.mocks.fakedb.freeitems)
          .to.deep.equal([{b: 42, _id: id}]);
        done();
      });
    });

    it('push to array', function(done) {
      var id = new mongoose.Types.ObjectId;
      config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1']}];
      ArrayItem.findOne({_id: id}, function (error, item) {
        if (error) return done(error);
        expect(item).to.have.property('key');
        item.key.push('value2');
        item.save(function(error) {
          if (error) return done(error);
          expect(config.mocks.fakedb.arrayitems).to.have.length(1);
          expect(config.mocks.fakedb.arrayitems[0])
            .to.have.property('key')
            .deep.equal(['value1', 'value2']);
          done();
        });
      });
    });

    it('$pushAll to non-existent field creates array', function(done) {
      config.mocks.fakedb.arrayitems = [{}];
      ArrayItem.update({}, {'$pushAll': {key: ['a', 'b']}}, function(err) {
        if (err) return done(err);
        expect(config.mocks.fakedb.arrayitems).to.have.length(1);
        expect(config.mocks.fakedb.arrayitems[0])
          .to.deep.equal({key: ['a', 'b']});
        done();
      });
    });

    it('$pushAll to non-array fails', function(done) {
      config.mocks.fakedb.freeitems = [{key: {a: 1}}];
      FreeItem.collection.update(
        {},
        {'$pushAll': {'key': ['a']}},
        function(err) {
          expect(err).to.exist;
          expect(err.ok).to.be.false;
          expect(err)
            .to.have.property('err', "The field 'key' must be an array.");
          expect(config.mocks.fakedb.freeitems).to.have.length(1);
          expect(config.mocks.fakedb.freeitems[0])
            .to.deep.equal({key: {a: 1}});
          done();
      });
    });

    it('$pushAll with non-array argument fails', function(done) {
      config.mocks.fakedb.arrayitems = [{key: ['value1', 'value2']}];
      ArrayItem.update({}, {'$pushAll': {key: 36}}, function(err) {
        expect(err).to.exist;
        expect(err.ok).to.be.false;
        expect(err)
          .to.have.property('err')
          .to.contain('$pushAll requires an array of values but was given');
        expect(config.mocks.fakedb.arrayitems).to.have.length(1);
        expect(config.mocks.fakedb.arrayitems[0])
          .to.deep.equal({key: ['value1', 'value2']});
        done();
      });
    });

    it('array shift', function(done) {
      var id = new mongoose.Types.ObjectId;
      config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1', 'value2']}];
      ArrayItem.findOne({_id: id}, function (error, item) {
        if (error) return done(error);
        expect(item).to.have.property('key');
        item.key.shift();
        item.save(function(error) {
          if (error) return done(error);
          expect(config.mocks.fakedb.arrayitems).to.have.length(1);
          expect(config.mocks.fakedb.arrayitems[0])
            .to.have.property('key')
            .deep.equal(['value2']);
          done();
        });
      });
    });

    it('set array value', function(done) {
      var id = new mongoose.Types.ObjectId;
      config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1', 'value2']}];
      ArrayItem.findOne({_id: id}, function (error, item) {
        if (error) return done(error);
        expect(item).to.have.property('key');
        item.key = ['one', 'two'];
        item.save(function(error) {
          if (error) return done(error);
          expect(config.mocks.fakedb.arrayitems).to.have.length(1);
          expect(config.mocks.fakedb.arrayitems[0])
            .to.have.property('key')
            .deep.equal(['one', 'two']);
          done();
        });
      });
    });

    it('set Date field', function(done) {
      var id = new mongoose.Types.ObjectId;
      var tenSecondsAgo = new Date(Date.now() - 10 * 1000);
      var now = new Date();
      config.mocks.fakedb.dateitems = [
        {_id: id, date: tenSecondsAgo}];
      var DateItem = mongoose.connection.model('DateItem');
      DateItem.findOne({_id: id}, function (error, item) {
        if (error) return done(error);
        expect(item).to.have.property('date');
        item.date = now;
        item.save(function(error) {
          if (error) return done(error);
          expect(config.mocks.fakedb.dateitems).to.have.length(1);
          expect(config.mocks.fakedb.dateitems[0].date.toString())
            .equal(now.toString());
          done();
        });
      });
    });

    it('set array of dates field', function(done) {
      var id = new mongoose.Types.ObjectId;
      var tenSecondsAgo = new Date(Date.now() - 10 * 1000);
      var now = new Date();
      config.mocks.fakedb.datearrayitems = [
        {_id: id, date: tenSecondsAgo}];
      var DateArrayItem = mongoose.connection.model('DateArrayItem');
      DateArrayItem.findOne({_id: id}, function (error, item) {
        if (error) return done(error);
        expect(item).to.have.property('date');
        item.date = [now];
        item.save(function(error) {
          if (error) return done(error);
          expect(config.mocks.fakedb.datearrayitems).to.have.length(1);
          expect(config.mocks.fakedb.datearrayitems[0])
            .to.have.deep.property('date[0]');
          expect(config.mocks.fakedb.datearrayitems[0].date[0].toString())
            .equal(now.toString());
          done();
        });
      });
    });

    it('$pull', function(done) {
      config.mocks.fakedb.arrayitems = [{key: ['value1', 'value2']}];
      ArrayItem.update({}, {$pull: {key: 'value1'}}, function(error) {
        if (error) return done(error);
        expect(config.mocks.fakedb.arrayitems).to.have.length(1);
        expect(config.mocks.fakedb.arrayitems[0])
          .to.deep.equal({key: ['value2']});
        done();
      });
    });

    it('$pull ObjectIds', function(done) {
      var id = new mongoose.Types.ObjectId();
      var idCopy = new mongoose.Types.ObjectId(id.toString());
      config.mocks.fakedb.arrayobjectiditems = [{key: [id]}];
      ArrayObjectIdItem = mongoose.connection.model('ArrayObjectIdItem');
      ArrayObjectIdItem.update({}, {$pull: {key: idCopy}}, function(error) {
        if (error) return done(error);
        expect(config.mocks.fakedb.arrayobjectiditems).to.have.length(1);
        expect(config.mocks.fakedb.arrayobjectiditems[0])
          .to.deep.equal({key: []});
        done();
      });
    });

    it('$pull multiple fields', function(done) {
      config.mocks.fakedb.arrayitems = [{key: ['a', 'b'], key2: ['c', 'd']}];
      ArrayItem.update({}, {$pull: {key: 'a', key2: 'd'}}, function(error) {
        if (error) return done(error);
        expect(config.mocks.fakedb.arrayitems).to.have.length(1);
        expect(config.mocks.fakedb.arrayitems[0])
          .to.deep.equal({key: ['b'], key2: ['c']});
        done();
      });
    });

    it('$pull from non-array fails', function(done) {
      config.mocks.fakedb.arrayitems = [{key: ['value1', 'value2']}];
      ArrayItem.update({}, {$pull: {'key.1': 'a'}}, function(err) {
        expect(err).to.exist;
        expect(err.ok).to.be.false;
        expect(err)
          .to.have.property('err', 'Cannot apply $pull to a non-array value');
        expect(config.mocks.fakedb.arrayitems).to.have.length(1);
        expect(config.mocks.fakedb.arrayitems[0])
          .to.deep.equal({key: ['value1', 'value2']});
        done();
      });
    });

    it('with non-container in path fails', function(done) {
      config.mocks.fakedb.simpleitems = [{key: 'value1'}];
      SimpleItem.update(
        {key: 'value1'},
        {$set: {'key.k2.k3': 5 }}, function (err, item) {
        expect(err).to.exist;
        expect(err.ok).to.be.false;
        expect(err)
          .to.have.property(
            'err',
            'cannot use the part (k2 of key.k2.k3)' +
            " to traverse the element ({ key: 'value1' })");
        done();
      });
    });

    describe('$unset', function() {
      var id1 = new mongoose.Types.ObjectId();

      it('deletes fields', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', b: 33, _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {b: 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', _id: id1});
            done();
        });
      });

      it('deletes compound fields', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', b: {c: 1}, _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {b: 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', _id: id1});
            done();
        });
      });

      it('deletes subfields', function(done) {
        config.mocks.fakedb.freeitems = [{
          a: 'value1',
          b: {c: 1, d: 2},
          _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {'b.c': 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', b: {d: 2}, _id: id1});
            done();
        });
      });

      it('deletes multiple fields', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', b: 33, _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {a: 'value ignored', b: 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({_id: id1});
            done();
        });
      });

      it('ignores non-existing fields', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', b: 33, _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {c: 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', b: 33, _id: id1});
            done();
        });
      });

      it('ignores non-existing subfields', function(done) {
        config.mocks.fakedb.freeitems = [{
          a: 'value1',
          b: {c: 1, d: {x: 2}},
          _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {'b.h': 0, 'b.d.y': 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', b: {c: 1, d: {x: 2}}, _id: id1});
            done();
        });
      });

      it('ignores subfields of non-objects', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {'a.f': 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', _id: id1});
            done();
        });
      });

      it('nulls out elements of arrays', function(done) {
        config.mocks.fakedb.freeitems = [{
          a: 'value1',
          b: [1, 2, 3],
          _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {'b.1': 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', b: [1, null, 3], _id: id1});
            done();
        });
      });

      it('ignores out-of-index elements of arrays', function(done) {
        config.mocks.fakedb.freeitems = [{
          a: 'value1',
          b: [1, 2, 3],
          _id: id1}];
        FreeItem.collection.update(
          {_id: id1},
          {'$unset': {'b.8': 0}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', b: [1, 2, 3], _id: id1});
            done();
        });
      });
    });

    describe('upsert', function() {
      var id1 = new mongoose.Types.ObjectId();
      var id2 = new mongoose.Types.ObjectId();

      it('updates existing documents', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value', b: 1, _id: id1}];
        FreeItem.collection.update(
          {a: 'value'},
          {'$set': {a: 'new value'}, '$inc': {b: 10}},
          {upsert: true},
          function(err) {
            if (err) return done(err);
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal([{a: 'new value', b: 11, _id: id1}]);
            done();
        });
      });

      it('inserts new document when no matches', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', b: 1, _id: id1}];
        FreeItem.collection.update(
          {a: 'value2'},
          {b: 10},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(2);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', b: 1, _id: id1});
            var newDocument = config.mocks.fakedb.freeitems[1];
            expect(newDocument)
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            expect(newDocument).to.have.property('b', 10);
            done();
        });
      });

      it('uses only update document values if update contains no operators',
        function(done) {
          config.mocks.fakedb.freeitems = [{a: 'value1', b: 1, _id: id1}];
          FreeItem.collection.update(
            {a: 'value2'},
            {b: 10, c: 'whatever'},
            {upsert: true},
            function(error) {
              if (error) return done(error);
              expect(config.mocks.fakedb.freeitems).to.have.length(2);
              expect(config.mocks.fakedb.freeitems[0])
                .to.deep.equal({a: 'value1', b: 1, _id: id1});
              var newDocument = config.mocks.fakedb.freeitems[1];
              expect(newDocument)
                .to.have.deep.property('_id.constructor.name', 'ObjectID');
              expect(_.omit(newDocument, '_id'))
                .to.deep.equal({b: 10, c: 'whatever'});
              done();
          });
      });

      it('uses update and find document values if update contains operators',
        function(done) {
          config.mocks.fakedb.freeitems = [{a: 'value1', b: 1, _id: id1}];
          FreeItem.collection.update(
            {a: 'value2', b: {'$gt': 5}},
            {'$set': {c: 10}, d: 'whatever'},
            {upsert: true},
            function(error) {
              if (error) return done(error);
              expect(config.mocks.fakedb.freeitems).to.have.length(2);
              expect(config.mocks.fakedb.freeitems[0])
                .to.deep.equal({a: 'value1', b: 1, _id: id1});
              var newDocument = config.mocks.fakedb.freeitems[1];
              expect(newDocument)
                .to.have.deep.property('_id.constructor.name', 'ObjectID');
              // Non-equality comparison should not be a basis for the new
              // document.
              expect(_.omit(newDocument, '_id'))
                .to.deep.equal({a: 'value2', c: 10, d: 'whatever'});
              done();
          });
      });

      it('pulls values from $and conjunctions if update contains operators',
        function(done) {
          config.mocks.fakedb.freeitems = [{a: 'value1', b: 1, _id: id1}];
          FreeItem.collection.update(
            {'$and': [
              {a: 'value2', b: 18},
              {'$and': [{c: 42}, {'d.e': 36}]},
              {'$or': [{m: 4458}, {n: 5577}]}]},
            {'$set': {z: 'whatever'}},
            {upsert: true},
            function(error) {
              if (error) return done(error);
              expect(config.mocks.fakedb.freeitems).to.have.length(2);
              expect(config.mocks.fakedb.freeitems[0])
                .to.deep.equal({a: 'value1', b: 1, _id: id1});
              var newDocument = config.mocks.fakedb.freeitems[1];
              expect(newDocument)
                .to.have.deep.property('_id.constructor.name', 'ObjectID');
              // Equalities under $or should not get transplanted to a new
              // document, so the result should not contain m or n.
              expect(_.omit(newDocument, '_id'))
                .to.deep.equal({
                  a: 'value2',
                  b: 18,
                  c: 42,
                  d: {e: 36},
                  z: 'whatever'});
              done();
          });
      });

      it('does not unpack ObjectIDs when copying from query', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', _id: id1}];
        FreeItem.collection.update(
          {a: 'value2', b: id2},
          {'$set': {c: 10}},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(2);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', _id: id1});
            var newDocument = config.mocks.fakedb.freeitems[1];
            expect(newDocument)
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            // An ObjectID must be pulled in its entirety and no pulled apart.
            expect(newDocument).to.have.property('b');
            expect(newDocument.b.equals(id2)).to.be.true;
            done();
        });
      });

      it('$setOnInsert modifies inserted records', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', _id: id1}];
        FreeItem.collection.update(
          {a: 'value2'},
          {'$setOnInsert': {c: 10}},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(2);
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', _id: id1});
            var newDocument = config.mocks.fakedb.freeitems[1];
            // $setOnInsert puts its arguments into the new document.
            expect(_.omit(newDocument, '_id'))
              .to.deep.equal({a: 'value2', c: 10});
            done();
        });
      });

      it('$setOnInsert does not touch updated records', function(done) {
        config.mocks.fakedb.freeitems = [{a: 'value1', _id: id1}];
        FreeItem.collection.update(
          {a: 'value1'},
          {'$setOnInsert': {c: 10}, '$set': {b: 5}},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(1);
            // $setOnInsert's arguments do not apply to existing documents.
            expect(config.mocks.fakedb.freeitems[0])
              .to.deep.equal({a: 'value1', b: 5, _id: id1});
            done();
        });
      });
    });

    describe('multi', function() {
      var id1 = new mongoose.Types.ObjectId();
      var id2 = new mongoose.Types.ObjectId();

      it('updates single document by default', function(done) {
        config.mocks.fakedb.freeitems = [
          {a: 'value', b: 1, _id: id1},
          {a: 'value', b: 2, _id: id2}];
        FreeItem.collection.update(
          {a: 'value'},
          {'$set': {a: 'new value'}, '$inc': {b: 10}},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal([
                {a: 'new value', b: 11, _id: id1},
                {a: 'value', b: 2, _id: id2}]);
            done();
        });
      });

      it('updates single document when set to false', function(done) {
        config.mocks.fakedb.freeitems = [
          {a: 'value', b: 1, _id: id1},
          {a: 'value', b: 2, _id: id2}];
        FreeItem.collection.update(
          {a: 'value'},
          {'$set': {a: 'new value'}, '$inc': {b: 10}},
          {multi: false},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal([
                {a: 'new value', b: 11, _id: id1},
                {a: 'value', b: 2, _id: id2}]);
            done();
        });
      });

      it('updates multiple documents when set to true', function(done) {
        config.mocks.fakedb.freeitems = [
          {a: 'value', b: 1, _id: id1},
          {a: 'value', b: 2, _id: id2}];
        FreeItem.collection.update(
          {a: 'value'},
          {'$set': {a: 'new value'}, '$inc': {b: 10}},
          {multi: true},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal([
                {a: 'new value', b: 11, _id: id1},
                {a: 'new value', b: 12, _id: id2}]);
            done();
        });
      });

      it('rejects update documents with literal fields', function(done) {
        config.mocks.fakedb.freeitems = [
          {a: 'value', b: 1, _id: id1},
          {a: 'value', b: 2, _id: id2}];
        FreeItem.collection.update(
          {a: 'value'},
          {a: 'new value', '$inc': {b: 10}},
          {multi: true},
          function(error) {
            expect(error).to.have.property('ok', false);
            expect(error).to.have.property('name', 'MongoError');
            expect(error)
              .to.have.property('err')
              .to.have.string("multi update only works with $ operators");
            done();
        });
      });
    });
  });

  describe('findAndModify', function() {
    var id1 = new mongoose.Types.ObjectId();
    var id2 = new mongoose.Types.ObjectId();
    var originalFreeItems = [
      {a: 'value', b: 1, _id: id1},
      {a: 'value', b: 2, _id: id2}];

    beforeEach(function() {
      config.mocks.fakedb.freeitems = _.cloneDeep(
        originalFreeItems,
        function(value) {
          return value instanceof mongoose.Types.ObjectId ?
            new mongoose.Types.ObjectId(value) :
            undefined;
        });
    });

    it('finds and updates a document', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        {},
        {'$set': {a: 'new value'}},
        function(error) {
          if (error) return done(error);
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal([
              {a: 'new value', b: 1, _id: id1},  // Has new value.
              originalFreeItems[1]]);
          done();
      });
    });

    it('returns original document', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        {},
        {'$set': {a: 'new value'}},
        function(error, item) {
          if (error) return done(error);
          expect(item).to.deep.equal(originalFreeItems[0]);
          done();
      });
    });

    it('returns requested projection of original document', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        null,
        {'$set': {a: 'new value'}},
        {fields: {a: 1}},
        function(error, item) {
          if (error) return done(error);
          expect(item).to.deep.equal({a: 'value', _id: id1});
          done();
      });
    });

    it('returns new document when new is set', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        {},
        {'$set': {a: 'new value'}},
        {'new': true},
        function(error, item) {
          if (error) return done(error);
          expect(item).to.deep.equal({a: 'new value', b: 1, _id: id1});
          done();
      });
    });

    it('returns requested projection of updated document when new is set',
      function(done) {
        FreeItem.collection.findAndModify(
          {b: 1},
          null,
          {'$set': {a: 'new value'}},
          {fields: {a: 1}, 'new': 1},
          function(error, item) {
            if (error) return done(error);
            expect(item).to.deep.equal({a: 'new value', _id: id1});
            done();
        });
    });

    it('rejects invalid requested projections', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        null,
        {'$set': {a: 'new value'}},
        {fields: {a: 1, b: 0}},
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('message')
            .to.have.string(
              'You cannot currently mix including and excluding fields');

          // The collection must remain unchanged.
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal(originalFreeItems);
          done();
      });
    });

    it('returns null when document is not found', function(done) {
      FreeItem.collection.findAndModify(
        {b: 'non-existent'},
        {},
        {'$set': {a: 'new value'}},
        function(error, item) {
          if (error) return done(error);
          expect(item).to.equal(null);
          done();
      });
    });

    it('fails when update document is not specified', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        function(error, item) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('message')
            .to.have.string('need remove or update');

          // The collection must remain unchanged.
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal(originalFreeItems);
          done();
      });
    });

    it('fails when operators follow fields in update', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        {},
        {a: 'new value', $set: {b: 5}},
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('message')
            .to.have.string(
              "exception: The dollar ($) prefixed field '$set' " +
              "in '$set' is not valid for storage.");

          // The collection must remain unchanged.
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal(originalFreeItems);
          done();
      });
    });

    it('fails when fields follow operators in update', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        {},
        {$set: {b: 5}, a: 'new value'},
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('message')
            .to.have.string('exception: Unknown modifier: a');

          // The collection must remain unchanged.
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal(originalFreeItems);
          done();
      });
    });

    it('fails when direct field asignments use dot notation', function(done) {
      FreeItem.collection.findAndModify(
        {b: 1},
        {},
        {'y.z': 5},
        function(error) {
          expect(error).to.have.property('ok', false);
          expect(error).to.have.property('name', 'MongoError');
          expect(error)
            .to.have.property('message')
            .to.have.string(
              "exception: The dotted field 'y.z' in 'y.z' " +
              'is not valid for storage.');

          // The collection must remain unchanged.
          expect(config.mocks.fakedb.freeitems)
            .to.deep.equal(originalFreeItems);
          done();
      });
    });

    xit('updates the first document in specified sort order', function(done) {
      done();
    });

    xit('updates the first document in the default sort order', function(done) {
      done();
    });

    describe('with remove option set', function() {
      it('deletes the found document', function(done) {
        FreeItem.collection.findAndModify(
          {b: 1},
          null,
          null,
          {remove: true},
          function(error, item) {
            if (error) return done(error);
            // The first record is deleted.
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal(originalFreeItems.slice(1, 2));
            done();
        });
      });

      it('returns the deleted document', function(done) {
        FreeItem.collection.findAndModify(
          {b: 1},
          null,
          null,
          {remove: true},
          function(error, item) {
            if (error) return done(error);
            // The first record is deleted.
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal(originalFreeItems.slice(1, 2));
            done();
        });
      });

      it('ignores update document', function(done) {
        FreeItem.collection.findAndModify(
          {b: 1},
          null,
          {a: 'new value'},
          {remove: true},
          function(error) {
            if (error) return done(error);

            // The first record is deleted even though update is specified.
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal(originalFreeItems.slice(1, 2));
            done();
        });
      });

      it('fails when new is specified', function(done) {
        FreeItem.collection.findAndModify(
          {b: 1},
          null,
          null,
          {remove: true, 'new': true},
          function(error) {
            expect(error).to.have.property('ok', false);
            expect(error).to.have.property('name', 'MongoError');
            expect(error)
              .to.have.property('message')
              .to.have.string("remove and returnNew can't co-exist");

            // The collection must remain unchanged.
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal(originalFreeItems);
            done();
        });
      });

      xit('removes first document in specified sort order', function(done) {
        done();
      });

      xit('removes first document in default sort order', function(done) {
        done();
      });
    });

    describe('with upsert options set', function() {
      it('updates existing document', function(done) {
        FreeItem.collection.findAndModify(
          {b: 1},
          null,
          {'$set': {a: 'new value'}},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems)
              .to.deep.equal([
                {a: 'new value', b: 1, _id: id1},  // Has new value.
                originalFreeItems[1]]);
            done();
        });
      });

      it('inserts new document when no matches', function(done) {
        FreeItem.collection.findAndModify(
          {b: 3},
          null,
          {'$set': {a: 'new value'}},
          {upsert: true},
          function(error) {
            if (error) return done(error);
            expect(config.mocks.fakedb.freeitems).to.have.length(3);
            expect(config.mocks.fakedb.freeitems.slice(0,2))
              .to.deep.equal(originalFreeItems);
            expect(config.mocks.fakedb.freeitems[2])
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            expect(_.omit(config.mocks.fakedb.freeitems[2], '_id'))
              .to.deep.equal({b: 3, a: 'new value'});
            done();
        });
      });

      it('returns null when upserting', function(done) {
        FreeItem.collection.findAndModify(
          {b: 3},
          null,
          {'$set': {a: 'new value'}},
          {upsert: true},
          function(error, item) {
            if (error) return done(error);
            expect(item).to.equal(null);
            done();
        });
      });

      it('returns new document when upserting and new is set', function(done) {
        FreeItem.collection.findAndModify(
          {b: 3},
          null,
          {'$set': {a: 'new value'}},
          {upsert: true, 'new': true},
          function(error, item) {
            if (error) return done(error);
            expect(item)
              .to.have.deep.property('_id.constructor.name', 'ObjectID');
            expect(_.omit(item, '_id')).to.deep.equal({b: 3, a: 'new value'});
            done();
        });
      });

      it('returns requested projection of new document with new set',
        function(done) {
          FreeItem.collection.findAndModify(
            {b: 3},
            null,
            {'$set': {a: 'new value'}},
            {fields: {a: 1}, upsert: true, 'new': true},
            function(error, item) {
              if (error) return done(error);
              expect(item)
                .to.have.deep.property('_id.constructor.name', 'ObjectID');
              expect(_.omit(item, '_id')).to.deep.equal({a: 'new value'});
              done();
          });
      });
    });
  });

  describe('count', function() {
    it('returns the number of queried documents', function(done) {
      config.mocks.fakedb.numberitems = [{key: 1}, {key: 2}, {key: 3}];
      NumberItem = mongoose.connection.model('NumberItem');
      NumberItem.count({key: {$gt: 1}}, function(error, n) {
        if (error) return done(error);
        expect(n).to.equal(2);
        done();
      });
    });
  });

  describe('distinct', function() {
    it('finds distinct field values', function(done) {
      config.mocks.fakedb.freeitems = [{key: 1}, {key: 2}, {key: 3}, {key2: 4}];
      FreeItem.collection.distinct('key', function(error, values) {
        if (error) return done(error);
        expect(values).to.deep.equal([1, 2, 3]);
        done();
      });
    });

    it('finds distinct subfiled values', function(done) {
      config.mocks.fakedb.freeitems = [
        {a: {b: 'x'}, c: 1},
        {a: {b: 'y'}},
        {a: {c: 'z'}}
      ];
      FreeItem.collection.distinct('a.b', function(error, values) {
        if (error) return done(error);
        expect(values).to.deep.equal(['x', 'y']);
        done();
      });
    });

    it('finds distinct compound values', function(done) {
      config.mocks.fakedb.freeitems = [
        {a: {b: 'x'}, c: 1},
        {a: {b: 'y'}},
        {a: ['x', 42]}
      ];
      FreeItem.collection.distinct('a', function(error, values) {
        if (error) return done(error);
        expect(values).to.deep.equal([{b: 'x'}, {b: 'y'}, ['x', 42]]);
        done();
      });
    });

    it('supports filtering', function(done) {
      config.mocks.fakedb.freeitems = [{key: 1}, {key: 2}, {key: 3}];
      FreeItem.collection.distinct(
        'key',
        {key: {'$gt': 1}},
        function(error, values) {
          if (error) return done(error);
          expect(values).to.deep.equal([2, 3]);
          done();
        });
    });

    it('handles empty collection', function(done) {
      config.mocks.fakedb.freeitems = [];
      FreeItem.collection.distinct('key', function(error, values) {
        if (error) return done(error);
        expect(values).to.deep.equal([]);
        done();
      });
    });

    it('returns empty array if field parameter is invalid', function(done) {
      config.mocks.fakedb.freeitems = [{key: 1}, {key: 2}, {key: 3}];
      FreeItem.collection.distinct(42, function(error, values) {
        if (error) return done(error);
        expect(values).to.deep.equal([]);
        done();
      });
    });

    it('ignores invalid query parameter', function(done) {
      config.mocks.fakedb.freeitems = [{key: 1}, {key: 2}, {key: 3}];
      FreeItem.collection.distinct('key', 3, function(error, values) {
        if (error) return done(error);
        expect(values).to.deep.equal([1, 2, 3]);
        done();
      });
    });
  });
});

