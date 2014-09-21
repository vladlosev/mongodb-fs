var _ = require('lodash')
var chai = require('chai')
var path = require('path')

var log = require('../lib/log')
var filter = require('../lib/filter')
var mocks = require('./mocks');

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

describe('filterItems', function() {
  var expect = chai.expect;
  var items = [
    {_id: 1, field1: [1, 2], field2: {a: 10, b: 20}},
    {_id: 2, field1: [2, 3], field2: {a: 100, b: 200}},
    {_id: 3, field1: [5, 6, 7]}];

  before(function() {
    filter.init();
  });

  describe('logical connectives', function() {
    it('$and', function() {
      var filtered = filter.filterItems(items,
        {'$and': [{_id: 1}, {field1: 2}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items,
        {'$and': [{_id: 3}, {field1: 2}]});
      expect(filtered).to.deep.equal([]);

      filtered = filter.filterItems(items,
        {'$and': [{field1: 2}, {_id: 3}]});
      expect(filtered).to.deep.equal([]);
    });

    it('$or', function() {
      var filtered = filter.filterItems(items,
        {'$or': [{_id: 1}, {_id: 3}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 3]);

      filtered = filter.filterItems(items,
        {'$or': [{_id: 1}, {field2: 8}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items,
        {'$or': [{_id: 5}, {_id: 3}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items,
        {'$or': [{_id: 10}, {field2: 50}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$nor', function() {
      var filtered = filter.filterItems(items,
        {'$nor': [{_id: 1}, {_id: 3}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2]);

      filtered = filter.filterItems(items,
        {'$nor': [{_id: 1}, {field2: 8}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

      filtered = filter.filterItems(items,
        {'$nor': [{_id: 5}, {_id: 3}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items,
        {'$nor': [{_id: 10}, {field2: 50}]});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
    });
  });

  describe('numbers', function() {
    it('match basic values', function() {
      var filtered = filter.filterItems(items, {_id: 1});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items, {_id: 5});
      expect(filtered).to.deep.equal([]);
    });

    it('match named subfield values', function() {
      var filtered = filter.filterItems(items, {'field2.a': 10});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items, {'field2.a': 1});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);

      filtered = filter.filterItems(items, {'field2.a': null});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
    });

    it('match indexed array elements', function() {
      var filtered = filter.filterItems(items, {'field1.0': 2});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2]);

      filtered = filter.filterItems(items, {'field1.1': 2});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items, {'field1.3': null});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('do not match regular expressions', function() {
      var filtered = filter.filterItems(items, {_id: /2/});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$eq', function() {
      var filtered = filter.filterItems(items, {_id: {'$eq': 1}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items, {_id: {'$eq': 5}});
      expect(filtered).to.deep.equal([]);

      filtered = filter.filterItems(items, {field1: {'$eq': 2}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {'field2.a': {'$eq': 10}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
    });

    it('$ne', function() {
      var filtered = filter.filterItems(items, {_id: {'$ne': 1}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

      filtered = filter.filterItems(items, {_id: {'$ne': 5}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

      filtered = filter.filterItems(items, {field1: {'$ne': 2}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field2.a': {'$ne': 0}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
    });

    it('$lt', function() {
      var filtered = filter.filterItems(items, {_id: {'$lt': 3}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {field1: {'$lt': 2}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items, {'field2.a': {'$lt': 200}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {'field1.5': {'$lt': 5}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$lte', function() {
      var filtered = filter.filterItems(items, {_id: {'$lte': 2}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {field1: {'$lte': 3}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {'field2.a': {'$lte': 100}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {'field2.a': {'$lte': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field1.5': {'$lte': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$gt', function() {
      var filtered = filter.filterItems(items, {_id: {'$gt': 2}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {field1: {'$gt': 2}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

      filtered = filter.filterItems(items, {'field2.a': {'$gt': 50}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2]);

      filtered = filter.filterItems(items, {'field2.a': {'$gt': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);

      filtered = filter.filterItems(items, {'field1.5': {'$gt': 5}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$gte', function() {
      var filtered = filter.filterItems(items, {_id: {'$gte': 2}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

      filtered = filter.filterItems(items, {field1: {'$gte': 3}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

      filtered = filter.filterItems(items, {'field2.a': {'$gte': 100}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2]);

      filtered = filter.filterItems(items, {'field2.a': {'$gte': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field2.a': {'$gt': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);

      filtered = filter.filterItems(items, {'field1.5': {'$gte': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$in', function() {
      var filtered = filter.filterItems(items, {_id: {'$in': [2, 3]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

      filtered = filter.filterItems(items, {field1: {'$in': [2, 7]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

      filtered = filter.filterItems(items, {'field2.a': {'$in': [null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field1.5': {'$in': [null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$nin', function() {
      var filtered = filter.filterItems(items, {_id: {'$nin': [2, 3]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items, {field1: {'$nin': [2, 7]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

      filtered = filter.filterItems(items, {'field2.a': {'$nin': [10, 100]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field2.a': {'$nin': [10, null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2]);

      filtered = filter.filterItems(items, {'field1.5': {'$nin': [null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
    });

    it('$all', function() {
      var filtered = filter.filterItems(items, {field1: {'$all': [2]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {field1: {'$all': [1, 2]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

      filtered = filter.filterItems(items, {field1: {'$all': [1, 7]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);

      filtered = filter.filterItems(items, {'field2.a': {'$all': [null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
    });

    it('$all fails with non-array', function() {
      chai.expect(
        function() { filter.filterItems(items, {field1: {'$all': 2}}); })
        .to.throw('BadValue $all needs an array: 2');
    });

    it('$all fails with $ in array', function() {
      chai.expect(
        function() {
          filter.filterItems(items, {field1: {'$all': [{'$gt': 2}]}});
        }).to.throw("BadValue no $ expressions in $all: [ { '$gt': 2 } ]");
    });

    it('$not', function() {
      var filtered = filter.filterItems(items, {_id: {'$not': {'$gt': 2}}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {field1: {'$not': {'$eq': 2}}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field2.a': {'$not': {'$eq': 10}}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

      filtered = filter.filterItems(items, {'field2.a': {'$not': {'$eq': null}}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
    });

    it('$not with regexp matches everything', function() {
      var filtered = filter.filterItems(items, {_id: {'$not': /2/}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

      filtered = filter.filterItems(items, {field1: {'$not': /2/}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

      filtered = filter.filterItems(items, {'field2.a': {'$not': /2/}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
    });

    it('$not fails on non-operators', function() {
      chai.expect(
        function() { filter.filterItems(items, {_id: {'$not': 2}}); })
        .to.throw("BadValue $not needs a regex or a document");

      chai.expect(
        function() { filter.filterItems(items, {_id: {'$not': [2]}}); })
        .to.throw("BadValue $not needs a regex or a document");

      chai.expect(
        function() { filter.filterItems(items, {_id: {'$not': {b: 2}}}); })
        .to.throw("BadValue unknown operator: b");
    });

    it('$not with $regex fails', function() {
      chai.expect(
        function() {
          filter.filterItems(items, {_id: {'$not': {'$regex': '2'}}});
        }).to.throw("BadValue $not cannot have a regex");
    });

    it('$exists', function() {
      var filtered = filter.filterItems(items, {'field2.a': {'$exists': true}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {'field2.a': {'$exists': false}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field1.2': {'$exists': true}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);

      filtered = filter.filterItems(items, {'field1.2': {'$exists': false}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
    });

    it('$exists accepts numeric parameter', function() {
      var filtered = filter.filterItems(items, {'field2.a': {'$exists': 8}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

      filtered = filter.filterItems(items, {'field2.a': {'$exists': 0}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
    });

    it('$regex does not match', function() {
      var filtered = filter.filterItems(items, {_id: {'$regex': '2'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });
  });

  describe('strings support', function() {
    var items = [
      {_id: 'a', field1: ['a', 'b'], field2: {a: 'x', b: 'z'}},
      {_id: 'b', field1: ['b', 'c'], field2: {a: 'y', b: 'zzz'}},
      {_id: 'c', field1: ['d', 'e', 'f\na']}];

    it('match basic values', function() {
      var filtered = filter.filterItems(items, {_id: 'a'});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

      filtered = filter.filterItems(items, {_id: 'h'});
      expect(filtered).to.deep.equal([]);
    });

    it('match named subfield values', function() {
      var filtered = filter.filterItems(items, {'field2.a': 'x'});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

      filtered = filter.filterItems(items, {'field2.a': 'a'});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);

      filtered = filter.filterItems(items, {'field2.a': null});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);
    });

    it('match indexed array elements', function() {
      var filtered = filter.filterItems(items, {'field1.0': 'b'});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);

      var filtered = filter.filterItems(items, {'field1.1': 'b'});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

      var filtered = filter.filterItems(items, {'field1.3': null});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);

    });

    describe('match regular expressions', function() {
      it('with no options', function() {
        var filtered = filter.filterItems(items, {_id: /[ac]/});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);

        filtered = filter.filterItems(items, {_id: {'$regex': /x+/}});
        expect(filtered).to.deep.equal([]);

        filtered = filter.filterItems(items, {field1: {'$regex': /b/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

        filtered = filter.filterItems(items, {'field2.b': {'$regex': /z/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });

      it('with ignore case option', function() {
        var filtered = filter.filterItems(items, {_id: /A/i});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });

      it('with multiline option', function() {
        var filtered = filter.filterItems(items, {field1: /^a/m});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
      });
    });

    it('$eq', function() {
      var filtered = filter.filterItems(items, {_id: {'$eq': 'a'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

      filtered = filter.filterItems(items, {_id: {'$eq': 'h'}});
      expect(filtered).to.deep.equal([]);

      filtered = filter.filterItems(items, {field1: {'$eq': 'b'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

      filtered = filter.filterItems(items, {'field2.a': {'$eq': 'x'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

      filtered = filter.filterItems(items, {'field2.a': {'$eq': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);
    });

    it('$ne', function() {
      var filtered = filter.filterItems(items, {_id: {'$ne': 'a'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);

      filtered = filter.filterItems(items, {_id: {'$ne': 'h'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);

      filtered = filter.filterItems(items, {field1: {'$ne': 'b'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);

      filtered = filter.filterItems(items, {'field2.a': {'$ne': 'x'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);

      filtered = filter.filterItems(items, {'field2.a': {'$ne': null}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
    });

    it('$lt', function() {
      var filtered = filter.filterItems(items, {_id: {'$lt': 'c'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

      filtered = filter.filterItems(items, {field1: {'$lt': 'b'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

      filtered = filter.filterItems(items, {'field2.b': {'$lt': 'zz'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
    });

    it('$lte', function() {
      var filtered = filter.filterItems(items, {_id: {'$lte': 'b'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

      filtered = filter.filterItems(items, {field1: {'$lte': 'c'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

      filtered = filter.filterItems(items, {'field2.b': {'$lte': 'zz'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
    });

    it('$gt', function() {
      var filtered = filter.filterItems(items, {_id: {'$gt': 'b'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);

      filtered = filter.filterItems(items, {field1: {'$gt': 'b'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);

      filtered = filter.filterItems(items, {'field2.b': {'$gt': 'z'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);
    });

    it('$gte', function() {
      var filtered = filter.filterItems(items, {_id: {'$gte': 'b'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);

      filtered = filter.filterItems(items, {field1: {'$gte': 'c'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);

      filtered = filter.filterItems(items, {'field2.b': {'$gte': 'zz'}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);
    });

    it('$in', function() {
      var filtered = filter.filterItems(items, {_id: {'$in': ['b', 'c']}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);

      filtered = filter.filterItems(items, {field1: {'$in': ['b', 'e']}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);

      filtered = filter.filterItems(items, {'field2.a': {'$in': [null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);

      filtered = filter.filterItems(items, {'field1.5': {'$in': [null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal([]);
    });

    it('$nin', function() {
      var filtered = filter.filterItems(items, {_id: {'$nin': ['b', 'c']}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

      filtered = filter.filterItems(items, {field1: {'$nin': ['b', 7]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);

      filtered = filter.filterItems(items, {'field2.b': {'$nin': ['z', 'zzz']}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);

      filtered = filter.filterItems(items, {'field2.b': {'$nin': ['z', null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);

      filtered = filter.filterItems(items, {'field1.5': {'$nin': [null]}});
      expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);
    });

    describe('$regex', function() {
      it('specified as strings', function() {
        var filtered = filter.filterItems(items, {_id: {'$regex': '[ac]'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);

        filtered = filter.filterItems(items, {_id: {'$regex': 'x+'}});
        expect(filtered).to.deep.equal([]);

        filtered = filter.filterItems(items, {field1: {'$regex': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

        filtered = filter.filterItems(items, {'field2.b': {'$regex': 'z'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });

      it('specified as RegExps', function() {
        var filtered = filter.filterItems(items, {_id: {'$regex': /[ac]/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);

        filtered = filter.filterItems(items, {_id: {'$regex': /x+/}});
        expect(filtered).to.deep.equal([]);

        filtered = filter.filterItems(items, {field1: {'$regex': /b/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

        filtered = filter.filterItems(items, {'field2.b': {'$regex': /z/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

        // With RegExp options.
        filtered = filter.filterItems(items, {_id: {'$regex': /A/i}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

        filtered = filter.filterItems(items, {'field1': {'$regex': /^a/m}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
      });

      it('with $options', function() {
        var filtered = filter.filterItems(
          items,
          {_id: {'$regex': 'A', '$options': 'i'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

        filtered = filter.filterItems(
          items,
          {'field1': {'$regex': '^a', '$options': 'm'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
      });

      it('with $options overriding RegExp options', function() {
        var filtered = filter.filterItems(
          items,
          {_id: {'$regex': /A/, '$options': 'i'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
        filtered = filter.filterItems(
          items,
          {_id: {'$regex': /A/, '$options': ''}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);

        filtered = filter.filterItems(
          items,
          {'field1': {'$regex': /^a/, '$options': 'm'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);

        filtered = filter.filterItems(
          items,
          {'field1': {'$regex': /^a/m, '$options': ''}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });
    });
  });

  describe('dates support', function() {
    xit('match basic values', function() {
    });

    xit('$eq', function() {
    });

    xit('$ne', function() {
    });

    xit('$lt', function() {
    });

    xit('$lte', function() {
    });

    xit('$gt', function() {
    });

    xit('$gte', function() {
    });

    xit('$in', function() {
    });

    xit('$nin', function() {
    });
  });
});
