var path = require('path')
  , chai = require('chai')
  , log = require('../lib/log')
  , helper = require('../lib/helper')
  , filter = require('../lib/filter')
  , mocks = require('./mocks')
  , logger;

var logLevel = process.env.LOG_LEVEL || 'WARN';

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
  level: logLevel
});
logger = log.getLogger();

describe('filterItems', function() {
  before(function() {
    filter.init();
  });

  it('basic field match', function() {
    var items;
    items = filter.filterItems(mocks.fakedb.items, {
      field1: 'value1',
      'field2.field3': 31
    });
    chai.expect(items).to.have.length(1);
  });

  it('$or', function() {
    var items;
    items = filter.filterItems(mocks.fakedb.items, {
      $or: [
        {field1: 'value1'},
        {'field2.field3': 32}
      ]
    });
    chai.expect(items).to.have.length(2);
  });

  it('$in and $ne', function() {
    var items;
    items = filter.filterItems(mocks.fakedb.items, {
      field1: { '$in': ['value1', 'value21'] },
      'field2.field3': { $ne: 32 }
    });
    chai.expect(items).to.have.length(2);
  });

  it('$all and $gt', function() {
    var items;
    items = filter.filterItems(mocks.fakedb.items, {
      field5: { '$all': ['a', 'b'] },
      'field2.field3': { '$gt': 31 }
    });
    chai.expect(items).to.have.length(1);
  });

  it('$not', function() {
    var items;
    items = filter.filterItems(mocks.fakedb.items, {
      'field2.field3': { $not: { $gt: 32 } }
    });
    chai.expect(items).to.have.length(2);
  });

  it('basic match finds items in array', function() {
    var docs = [{key: [1, 2]}];
    var filtered = filter.filterItems(docs, {key: 2});
    chai.expect(filtered).to.deep.equal(docs);

    filtered = filter.filterItems(docs, {key: 3});
    chai.expect(filtered).to.deep.equal([]);
  });

  it('$eq finds items in array', function() {
    var docs = [{key: [1, 2]}];
    var filtered = filter.filterItems(docs, {key: {$eq: 2}});
    chai.expect(filtered).to.deep.equal(docs);

    filtered = filter.filterItems(docs, {key: {$eq: 3}});
    chai.expect(filtered).to.deep.equal([]);
  });

  it('$new finds items in array', function() {
    var docs = [{key: [1, 2]}];
    var filtered = filter.filterItems(docs, {key: {$ne: 2}});
    chai.expect(filtered).to.deep.equal([]);

    filtered = filter.filterItems(docs, {key: {$ne: 3}});
    chai.expect(filtered).to.deep.equal(docs);
  });
});
