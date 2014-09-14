var util = require('util')
  , chai = require('chai')
  , path = require('path')
  , log = require('../lib/log')
  , filter = require('../lib/filter');

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

describe('buildOpNode', function() {
  var expect = chai.expect;

  before(function() {
    filter.init();
  });

  it('handles basic match', function() {
    var rootNode = filter.buildOpNode({a: 'avalue', 'b': 3});
    expect(rootNode).to.deep.equal({
      op: '$and',
      field: null,
      args: [
        { op: '$eq', field: 'a', args: [ '"avalue"' ] },
        { op: '$eq', field: 'b', args: [ '3' ] }
      ]
    });
  });

  it('handles $or', function() {
    var rootNode = filter.buildOpNode({
      $or: [
        {a: 'avalue'},
        {'b': 3}
      ]
    });
    expect(rootNode).to.deep.equal({
      op: '$or',
      field: null,
      args: [
        { op: '$eq', field: 'a', args: [ '"avalue"' ] },
        { op: '$eq', field: 'b', args: [ '3' ] }
      ]
    });
  });

  it('handles $and', function() {
    var rootNode = filter.buildOpNode({
      field1: { '$in': ['value1', 'value21'] },
      'field2.field3': { $ne: 32 }
    });
    expect(rootNode).to.deep.equal({
      op: '$and',
      field: null,
      args: [
        { op: '$in', field: 'field1', args: [ '"value1"', '"value21"' ] },
        { op: '$ne', field: 'field2.field3', args: [ '32' ] }
      ]
    });
  });

  it('handles $all', function() {
    var rootNode = filter.buildOpNode({
      field5: { '$all': ['a', 'b'] },
      'field2.field3': { '$gt': 31 }
    });
    expect(rootNode).to.deep.equal({
      op: '$and',
      field: null,
      args: [
        { op: '$all', field: 'field5', args: [ '"a"', '"b"' ] },
        { op: '$gt', field: 'field2.field3', args: [ '31' ] }
      ]
    });
  });

  it('handles $not', function() {
    var rootNode = filter.buildOpNode({
      'field2.field3': { $not: { $gt: 32 } }
    });
    expect(rootNode).to.deep.equal({
      op: '$not',
      field: 'field2.field3',
      args: [
        { op: '$gt', field: 'field2.field3', args: [ '32' ] }
      ]
    });
  });
});
