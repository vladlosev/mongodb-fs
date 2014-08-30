var util = require('util')
  , path = require('path')
  , nodeunit = require('nodeunit')
  , mongodbFs = require('../lib/mongodb-fs')
  , mongoose = require('mongoose')
  , Profess = require('profess')
  , log = require('../lib/log')
  , helper = require('../lib/helper')
  , config, logger, schema, dbConfig, dbOptions, Item, Unknown;

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
    level: 'TRACE'
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

mongoose.model('Item', new mongoose.Schema({key: String}));
mongoose.model('ArrayItem', new mongoose.Schema({key: [String]}));

var Item, ArrayItem;

module.exports.setUp = function (callback) {
  var profess;
  profess = new Profess();
  profess.
    do(function () {
      //return profess.next();
      if (!mongodbFs.isRunning()) {
        mongodbFs.init(config);
        logger.trace('init');
        mongodbFs.start(profess.next);
        nodeunit.on('complete', function () {
          mongodbFs.stop();
        });
      } else {
        profess.next();
      }
    }).
    then(function () {
      logger.trace('connect to db');
      mongoose.set('debug', true);
      mongoose.connect(dbConfig.url, dbOptions, profess.next);
    }).
    then(function () {
      Item = mongoose.connection.model('Item');
      ArrayItem = mongoose.connection.model('ArrayItem');
      profess.next();
    }).
    then(callback);
};

module.exports.tearDown = function (callback) {
  logger.trace('disconnect');
  mongoose.disconnect(callback);
};

module.exports.testFindTwice = function (test) {
  logger.trace('testFind');
  config.mocks.fakedb.items = [{key: 'value1'}, {key: 'value2'}];
  Item.find(function (err, items) {});
  Item.find(function (err, items) {
    test.ifError(err);
    test.ok(items);
    test.equal(items.length, 2);
    test.equal(items[0].key, 'value1');
    test.equal(items[1].key, 'value2');
    test.done();
  });
};

exports.testDelete = function (test) {
  logger.trace('testDelete');
  config.mocks.fakedb.items = [{key: 'value1'}, {key: 'value2'}];
  Item.remove({key: 'value1'}, function(err) {
    test.ifError(err);
    test.equal(config.mocks.fakedb.items.length, 1);
    test.equal(config.mocks.fakedb.items[0].key, 'value2');
    test.done();
  });
};

exports.testInsert = function (test) {
  logger.trace('testInsert');
  config.mocks.fakedb.items = [];
  var item = new Item({key: 'value'});
  item.save(function(err) {
    test.ifError(err);
    test.ok(item);
    test.equal(config.mocks.fakedb.items.length, 1);
    test.equal(config.mocks.fakedb.items[0].key, 'value');
    test.done();
  });
};

exports.testUpdate = function (test) {
  logger.trace('testUpdate');
  config.mocks.fakedb.items = [
    {key: 'value1', _id: new mongoose.Types.ObjectId},
    {key: 'value2', _id: new mongoose.Types.ObjectId}];
  Item.findOne({key: 'value1'}, function (err, item) {
    test.ifError(err);
    test.ok(item);
    item.key = 'new value';
    item.save(function(err) {
      test.ifError(err);
      test.equal(config.mocks.fakedb.items.length, 2);
      test.equal(config.mocks.fakedb.items[0].key, 'new value');
      test.done();
    });
  });
};

exports.testUpdateArrayPush = function (test) {
  logger.trace('testUpdateArrayPush');
  var id = new mongoose.Types.ObjectId;
  config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1']}];
  ArrayItem.findOne({_id: id}, function (err, item) {
    test.ifError(err);
    test.ok(item);
    item.key.push('value2');
    item.save(function(err) {
      test.ifError(err);
      test.equal(config.mocks.fakedb.arrayitems.length, 1);
      test.deepEqual(config.mocks.fakedb.arrayitems[0].key, ['value1', 'value2']);
      test.done();
    });
  });
};

exports.testUpdateArrayShift = function (test) {
  logger.trace('testUpdateArrayShift');
  var id = new mongoose.Types.ObjectId;
  config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1', 'value2']}];
  ArrayItem.findOne({_id: id}, function (err, item) {
    test.ifError(err);
    test.ok(item);
    item.key.shift();
    item.save(function(err) {
      test.ifError(err);
      test.equal(config.mocks.fakedb.arrayitems.length, 1);
      test.deepEqual(
        config.mocks.fakedb.arrayitems[0].key, ['value2']);
      test.done();
    });
  });
};

exports.testUpdateArraySetArray = function (test) {
  logger.trace('testUpdate');
  var id = new mongoose.Types.ObjectId;
  config.mocks.fakedb.arrayitems = [{_id: id, __v: 0, key: ['value1', 'value2']}];
  ArrayItem.findOne({_id: id}, function (err, item) {
    test.ifError(err);
    test.ok(item);
    item.key = ['one', 'two'];
    item.save(function(err) {
      test.ifError(err);
      test.equal(config.mocks.fakedb.arrayitems.length, 1);
      test.deepEqual(
        config.mocks.fakedb.arrayitems[0].key, ['one', 'two']);
      test.done();
    });
  });
};
