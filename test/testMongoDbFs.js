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
  mocks: require('./mocks'),
  fork: true,
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

schema = {
  field1: String,
  field2: {
    field3: Number,
    field4: String
  },
  field5: Array
};

dbConfig = {
  name: 'fakedb'
};
dbConfig.url = util.format('mongodb://localhost:%d/%s', config.port, dbConfig.name);

dbOptions = {
  server: { poolSize: 1 }
};

mongoose.model('Item', schema);
mongoose.model('Unknown', { name: String });

exports.setUp = function (callback) {
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
      mongoose.connect(dbConfig.url, dbOptions, profess.next);
      //test.ok(mongoose.connection.readyState);
    }).
    then(function () {
      Item = mongoose.connection.model('Item');
      Unknown = mongoose.connection.model('Unknown');
      profess.next();
    }).
    then(callback);
};

exports.tearDown = function (callback) {
  logger.trace('disconnect');
  mongoose.disconnect(callback);
};

exports.testFindAll = function (test) {
  logger.trace('testFindAll');
  Item.find(function (err, items) {
    test.ifError(err);
    test.ok(items);
    test.equal(items.length, 3);
    test.done();
  });
};

exports.testFindUnknown = function (test) {
  logger.trace('testFindUnknown');
  Unknown.find(function (err, items) {
    test.ifError(err);
    test.equal(items.length, 0);
    test.done();
  });
};

exports.testFindFilters = {
  'test $all': function (test) {
    Item.find({ 'field5': { $all: ['a', 'c'] } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 1);
      if (items.length === 3) {
        test.equal(items[0].field5.indexOf('a'), 0);
        test.equal(items[0].field5.indexOf('b'), 1);
        test.equal(items[0].field5.indexOf('c'), 2);
      }
      test.done();
    });
  },
  'test $gt': function (test) {
    Item.find({ 'field2.field3': { $gt: 32 } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 1);
      if (items.length === 1) {
        test.equal(items[0].field2.field3, 33);
      }
      test.done();
    });
  },
  'test $gte': function (test) {
    Item.find({ 'field2.field3': { $gte: 32 } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 2);
      test.done();
    });
  },
  'test $in': function (test) {
    Item.find({ 'field2.field3': { $in: [32, 33] } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 2);
      if (items.length === 2) {
        test.equal(items[0].field2.field3, 32);
        test.equal(items[1].field2.field3, 33);
      }
      test.done();
    });
  },
  'test $lt': function (test) {
    Item.find({ 'field2.field3': { $lt: 32 } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 1);
      if (items.length === 1) {
        test.equal(items[0].field2.field3, 31);
      }
      test.done();
    });
  },
  'test $lte': function (test) {
    Item.find({ 'field2.field3': { $gte: 32 } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 2);
      test.done();
    });
  },
  'test $ne': function (test) {
    Item.find({ 'field2.field3': { $ne: 32 } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 2);
      if (items.length === 2) {
        test.equal(items[0].field2.field3, 31);
        test.equal(items[1].field2.field3, 33);
      }
      test.done();
    });
  },
  'test $nin': function (test) {
    Item.find({ 'field2.field3': { $nin: [32, 33] } }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 1);
      if (items.length === 1) {
        test.equal(items[0].field2.field3, 31);
      }
      test.done();
    });
  },
   'test $or': function (test) {
    Item.find({ $or: [
      { field1: 'value1' },
      { 'field2.field3': 32 }
    ]}, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 2);
      if (items.length === 2) {
        test.equal(items[0].field1, 'value1');
        test.equal(items[0].field2.field3, 32);
      }
      test.done();
    });
  },
  'test simple filter': function (test) {
    Item.find({ 'field2.field3': 32 }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 1);
      if (items.length) {
        test.equal(items[0].field2.field3, 32);
      }
      test.done();
    });
  },
  'test 2 fields filter': function (test) {
    Item.find({ field1: 'value1', 'field2.field3': 31 }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 1);
      if (items.length) {
        test.equal(items[0].field1, 'value1');
        test.equal(items[0].field2.field3, 31);
      }
      test.done();
    });
  },
  'test string filter': function (test) {
    Item.find({ 'field2.field4': 'value24' }, function (err, items) {
      test.ifError(err);
      test.ok(items);
      test.equal(items.length, 1);
      if (items.length) {
        test.equal(items[0].field2.field4, 'value24');
      }
      test.done();
    });
  }
};

exports.testFindById = function (test) {
  logger.trace('testFindById');
  var itemId;
  Item.findOne({field1: 'value1'}, function (err, item) {
    test.ifError(err);
    test.ok(item);
    logger.trace('item :', item);
    itemId = item.id;
    logger.trace('itemId :', itemId);
    Item.findById(itemId, function (err, item) {
      test.ifError(err);
      test.ok(item);
      test.done();
    });
  });
};

exports.testFindByIdAndUpdate = function (test) {
  var itemId;
  Item.findOne({field1: 'value1'}, function (err, item) {
    test.ifError(err);
    test.ok(item);
    logger.trace('item :', item);
    itemId = item.id;
    logger.trace('itemId :', itemId);
    Item.findByIdAndUpdate(itemId, {field1: 'value1Modified'}, function (err, item) {
      test.ifError(err);
      test.ok(item);
      //test.equal(item.field1, 'value1Modified');
    });
  });
};

exports.testRemove = function (test) {
  logger.trace('testRemove');
  Item.findOne({ 'field1': 'value11' }, function (err, item) {
    logger.info('item :', item);
    test.ifError(err);
    test.ok(item);
    item.remove(function (err) {
      test.ifError(err);
      test.done();
    });
  });
};

exports.testCrud = function (test) {
  var profess, noItems, errorHandler, item;
  profess = new Profess();
  errorHandler = profess.handleError(function (err) {
    test.ifError(err);
    test.done();
  });
  profess.
    do(function () { // load all items
      Item.find(errorHandler);
    }).
    then(function (items) { // check
      test.ok(items);
      noItems = items.length;
      profess.next();
    }).
    then(function () { // insert item
      item = new Item({
        field1: 'value101',
        field2: {
          field3: 1031,
          field4: 'value104'
        }
      });
      item.save(errorHandler);
    }).
    then(function (item) { // check
      test.ok(item);
      profess.next();
    }).
    then(function (item) { // find item
      Item.findOne({ 'field2.field3': 1031 }, errorHandler);
    }).
    then(function (savedItem) { // check saved item
      test.equal(item.field1, savedItem.field1);
      test.equal(item.field2.field3, savedItem.field2.field3);
      test.equal(item.field2.field4, savedItem.field2.field4);
      profess.next();
    }).
    then(function () { // load all items
      Item.find(errorHandler);
    }).
    then(function (items) { // check
      test.ok(items);
      test.equal(items.length, noItems + 1);
      profess.next();
    }).
    then(function () { // update item
      item.field2.field3 = 2031;
      item.save(errorHandler);
    }).
    then(function (item) { // check
      test.ok(item);
      profess.next();
    }).
    then(function () { // remove item
      Item.remove({_id: item._id }, errorHandler);
    }).
    then(function () { // load all items
      Item.find(errorHandler);
    }).
    then(function (items) { // check
      test.ok(items);
      test.equal(items.length, noItems);
      profess.next();
    }).
    then(function () { // end
      test.done();
    });
};

exports.testInsert = function (test) {
  var item;
  logger.trace('testInsert');
  item = new Item({
    field1: 'value101',
    field2: {
      field3: 1031,
      field4: 'value104'
    },
    field5: ['h', 'i', 'j']
  });
  item.save(function (err, savedItem) {
    test.ifError(err);
    test.ok(savedItem);
    item.remove(function(err) {
      test.ifError(err);
      test.done();
    });
  });
};

// disabled tests :
// With current Mongoose (3.8.15), findByIdAndUpdate requires implementing the
// findandmodify command.
delete exports.testFindByIdAndUpdate;

module.exports = exports;
