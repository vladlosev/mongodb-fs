var path = require('path')
  , nodeunit = require('nodeunit')
  , log = require('../lib/log')
  , helper = require('../lib/helper')
  , filter = require('../lib/filter')
  , mocks = require('./mocks')
  , logger;

log.init({
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
});
logger = log.getLogger();
filter.init();

module.exports.test1 = function(test) {
  var items;
  items = filter.filterItems(mocks.fakedb.items, {
    field1: 'value1',
    'field2.field3': 31
  });
  logger.debug('items :', items);
  test.ok(items);
  test.equal(items.length, 1);
  test.done();
}

module.exports.test2 = function(test) {
  var items;
  items = filter.filterItems(mocks.fakedb.items, {
    $or: [
      {field1: 'value1'},
      {'field2.field3': 32}
    ]
  });
  logger.debug('items :', items);
  test.ok(items);
  test.equal(items.length, 2);
  test.done();
}

module.exports.test3 = function(test) {
  var items;
  items = filter.filterItems(mocks.fakedb.items, {
    field1: { '$in': ['value1', 'value21'] },
    'field2.field3': { $ne: 32 }
  });
  logger.debug('items :', items);
  test.ok(items);
  test.equal(items.length, 2);
  test.done();
}

module.exports.test4 = function(test) {
  var items;
  items = filter.filterItems(mocks.fakedb.items, {
    field5: { '$all': ['a', 'b'] },
    'field2.field3': { '$gt': 31 }
  });
  logger.debug('items :', items);
  test.ok(items);
  test.equal(items.length, 1);
  test.done();
}

module.exports.test5 = function(test) {
  var items;
  items = filter.filterItems(mocks.fakedb.items, {
    'field2.field3': { $not: { $gt: 32 } }
  });
  logger.debug('items :', items);
  test.ok(items);
  test.equal(items.length, 2);
  test.done();
}

module.exports.testFindItemsInArray = function(test) {
  var docs = [{key: [1, 2]}];
  var filtered = filter.filterItems(docs, {key: 2});
  test.deepEqual(filtered, docs);

  filtered = filter.filterItems(docs, {key: 3});
  test.deepEqual(filtered, []);
  test.done();
}

module.exports.testEqFindsItemsInArray = function(test) {
  var docs = [{key: [1, 2]}];
  var filtered = filter.filterItems(docs, {key: {$eq: 2}});
  test.deepEqual(filtered, docs);

  filtered = filter.filterItems(docs, {key: {$eq: 3}});
  test.deepEqual(filtered, []);
  test.done();
}

module.exports.testNeFindsItemsInArray = function(test) {
  var docs = [{key: [1, 2]}];
  var filtered = filter.filterItems(docs, {key: {$ne: 2}});
  test.deepEqual(filtered, []);

  filtered = filter.filterItems(docs, {key: {$ne: 3}});
  test.deepEqual(filtered, docs);
  test.done();
}
