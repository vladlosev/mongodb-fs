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

mongoose.model('SimpleItem', new mongoose.Schema({key: String}));
mongoose.model('ArrayItem', new mongoose.Schema({key: [String], key2: [String]}));
mongoose.model('DateItem', new mongoose.Schema({date: Date}));
mongoose.model('DateArrayItem', new mongoose.Schema({date: [Date]}));
mongoose.model('NumberItem', new mongoose.Schema({key: Number}));
mongoose.model('ArrayObjectIdItem', new mongoose.Schema({key: [mongoose.Types.ObjectId]}));

var SimpleItem, ArrayItem;

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
    it('run twice', function(done) {
      config.mocks.fakedb.simpleitems = [{key: 'value1'}, {key: 'value2'}];
      SimpleItem.find(function (err, items) {});
      SimpleItem.find(function (err, items) {
        expect(err).to.not.exist;
        expect(items).to.have.length(2);
        expect(items[0]).to.have.property('key', 'value1');
        expect(items[1]).to.have.property('key', 'value2');
        done();
      });
    });
  });

  describe('delete', function() {
    it('basic', function(done) {
      config.mocks.fakedb.simpleitems = [{key: 'value1'}, {key: 'value2'}];
      SimpleItem.remove({key: 'value1'}, function(err) {
        expect(err).to.not.exist;
        expect(config.mocks.fakedb.simpleitems).to.have.length(1);
        expect(config.mocks.fakedb.simpleitems[0])
          .to.have.property('key', 'value2');
        done();
      });
    });

    it('by query', function(done) {
      config.mocks.fakedb.simpleitems = [{key: 'value1'}, {key: 'value2'}];
      SimpleItem.remove({key: {$ne: 'value1'}}, function(err) {
        expect(err).to.not.exist;
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
      item.save(function(err) {
        expect(err).to.not.exist;
        expect(config.mocks.fakedb.simpleitems).to.have.length(1);
        expect(config.mocks.fakedb.simpleitems[0])
          .to.have.property('key', 'value');
        done();
      });
    });
  });

  describe('update', function() {
    it('basic', function(done) {
      config.mocks.fakedb.simpleitems = [
        {key: 'value1', _id: new mongoose.Types.ObjectId},
        {key: 'value2', _id: new mongoose.Types.ObjectId}];
      SimpleItem.findOne({key: 'value1'}, function (err, item) {
        expect(err).to.not.exist;
        expect(item).to.exist;
        item.key = 'new value';
        item.save(function(err) {
          expect(err).to.not.exist;
          expect(config.mocks.fakedb.simpleitems).to.have.length(2);
          expect(config.mocks.fakedb.simpleitems[0])
            .to.have.property('key', 'new value');
          done();
        });
      });
    });

    it('push to array', function(done) {
      var id = new mongoose.Types.ObjectId;
      config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1']}];
      ArrayItem.findOne({_id: id}, function (err, item) {
        expect(err).to.not.exist;
        expect(item).to.have.property('key');
        item.key.push('value2');
        item.save(function(err) {
          expect(err).to.not.exist;
          expect(config.mocks.fakedb.arrayitems).to.have.length(1);
          expect(config.mocks.fakedb.arrayitems[0])
            .to.have.property('key')
            .deep.equal(['value1', 'value2']);
          done();
        });
      });
    });

    it('$pushAll to no-array fails', function(done) {
      config.mocks.fakedb.arrayitems = [{key: ['value1', 'value2']}];
      ArrayItem.update({}, {$pushAll: {'key.1': ['a']}}, function(err) {
        expect(err).to.exist;
        expect(err.ok).to.be.false;
        expect(err)
          .to.have.property('err', "The field 'key.1' must be an array.");
        expect(config.mocks.fakedb.arrayitems).to.have.length(1);
        expect(config.mocks.fakedb.arrayitems[0])
          .to.deep.equal({key: ['value1', 'value2']});
        done();
      });
    });

    it('array shift', function(done) {
      var id = new mongoose.Types.ObjectId;
      config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1', 'value2']}];
      ArrayItem.findOne({_id: id}, function (err, item) {
        expect(err).to.not.exist;
        expect(item).to.have.property('key');
        item.key.shift();
        item.save(function(err) {
          expect(err).to.not.exist;
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
      ArrayItem.findOne({_id: id}, function (err, item) {
        expect(err).to.not.exist;
        expect(item).to.have.property('key');
        item.key = ['one', 'two'];
        item.save(function(err) {
          expect(err).to.not.exist;
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
      DateItem.findOne({_id: id}, function (err, item) {
        expect(err).to.not.exist;
        expect(item).to.have.property('date');
        item.date = now;
        item.save(function(err) {
          expect(err).to.not.exist;
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
      DateArrayItem.findOne({_id: id}, function (err, item) {
        expect(err).to.not.exist;
        expect(item).to.have.property('date');
        item.date = [now];
        item.save(function(err) {
          expect(err).to.not.exist;
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
      ArrayItem.update({}, {$pull: {key: 'value1'}}, function(err) {
        expect(err).to.not.exist;
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
      ArrayObjectIdItem.update({}, {$pull: {key: idCopy}}, function(err) {
        expect(err).to.not.exist;
        expect(config.mocks.fakedb.arrayobjectiditems).to.have.length(1);
        expect(config.mocks.fakedb.arrayobjectiditems[0])
          .to.deep.equal({key: []});
        done();
      });
    });

    it('$pull multiple fields', function(done) {
      config.mocks.fakedb.arrayitems = [{key: ['a', 'b'], key2: ['c', 'd']}];
      ArrayItem.update({}, {$pull: {key: 'a', key2: 'd'}}, function(err) {
        expect(err).to.not.exist;
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
  });

});

module.exports.testCount = function(test) {
  logger.trace('testCount');
  config.mocks.fakedb.numberitems = [{key: 1}, {key: 2}, {key: 3}];
  NumberItem = mongoose.connection.model('NumberItem');
  NumberItem.count({key: {$gt: 1}}, function(err, n) {
    test.ifError(err);
    test.equal(n, 2);
    test.done();
  });
};
