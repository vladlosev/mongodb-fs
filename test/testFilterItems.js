'use strict';

var _ = require('lodash');
var chai = require('chai');
var mongoose = require('mongoose');
var path = require('path');
var bson = require('bson');

var log = require('../lib/log');
var filter = require('../lib/filter');

var logLevel = process.env.LOG_LEVEL || 'WARN';

log.init({
  category: path.basename(__filename),
  level: logLevel
});

describe('filterItems', function() {
  var expect = chai.expect;
  var items = [
    {_id: 1, field1: [1, 2], field2: {a: 10, b: 20}},
    {_id: 2, field1: [2, 3], field2: {a: 100, b: 200}},
    {_id: 3, field1: [5, 6, 7]}];

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

      expect(function() { filter.filterItems(items, {'$and': {a: 2}}); })
        .to.throw('BadValue $and needs an array');
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

      expect(function() { filter.filterItems(items, {'$or': {a: 2}}); })
        .to.throw('BadValue $or needs an array');
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

      expect(function() { filter.filterItems(items, {'$nor': {a: 2}}); })
        .to.throw('BadValue $nor needs an array');
    });

    it('fails with unknown operators', function() {
      expect(
        function() { filter.filterItems(items, {'$eq': 2}); })
        .to.throw('BadValue unknown top level operator: $eq');
    });
  });

  describe('numbers', function() {
    describe('direct field comparison', function() {
      it('matches basic values', function() {
        var filtered = filter.filterItems(items, {_id: 1});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

        filtered = filter.filterItems(items, {_id: 5});
        expect(filtered).to.deep.equal([]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: 2});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches subfields of array elements implicitly', function() {
        var items = [
          {_id: 1, field1: [{a: 1}, {a: 2}]},
          {_id: 2, field1: [{a: 2}, {a: 3}]},
          {_id: 3, field1: [{a: 5}, {a: 6}, {a: 7}]}];
        var filtered = filter.filterItems(items, {'field1.a': 2});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('does not match subfields of elements of array elements implicitly',
        function() {
          var items = [
            {_id: 1, field1: [[{a: 1}], [{a: 2}]]},
            {_id: 2, field1: [[{a: 2}], [{a: 3}]]},
            {_id: 3, field1: [[{a: 5}], [{a: 6}], [{a: 7}]]}];
          var filtered = filter.filterItems(items, {'field1.a': 2});
          expect(_.pluck(filtered, '_id')).to.deep.equal([]);

          items = [
            {_id: 1, field1: [[{a: 1}, {a: 2}]]},
            {_id: 2, field1: [[{a: 2}, {a: 3}]]},
            {_id: 3, field1: [[{a: 5}, {a: 6}, {a: 7}]]}];
          filtered = filter.filterItems(items, {'field1.a': 2});
          expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': 10});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

        filtered = filter.filterItems(items, {'field2.a': 1});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);

        filtered = filter.filterItems(items, {'field2.a': null});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches array elements in dot notation', function() {
        var filtered = filter.filterItems(items, {'field1.0': 2});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);

        filtered = filter.filterItems(items, {'field1.1': 2});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);

        filtered = filter.filterItems(items, {'field1.3': null});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not match regular expressions', function() {
        var filtered = filter.filterItems(items, {_id: /2/});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('fails on unknown operators', function() {
        expect(
          function() { filter.filterItems(items, {_id: {'$wombat': 3}}); })
          .to.throw('BadValue unknown operator: $wombat');
      });
    });

    describe('$eq', function() {
      it('returns results when there are matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$eq': 1}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('returns nothing when there are no matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$eq': 5}});
        expect(filtered).to.deep.equal([]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$eq': 2}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$eq': 10}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });
    });

    describe('$ne', function() {
      it('returns results when there are matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$ne': 1}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('returns all documents when all match', function() {
        var filtered = filter.filterItems(items, {_id: {'$ne': 5}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$ne': 2}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$ne': 0}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });
    });

    describe('$lt', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$lt': 3}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$lt': 2}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$lt': 200}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches array elements in dot notation', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$lt': 5}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$lte', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$lte': 2}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches array elements', function() {
        var filtered = filter.filterItems(items, {field1: {'$lte': 3}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$lte': 100}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$lte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$lte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$gt', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$gt': 2}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches array elements', function() {
        var filtered = filter.filterItems(items, {field1: {'$gt': 2}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$gt': 50}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
      });

      it('does not matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$gt': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('matches array elements in dot notation', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$gt': 5}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$gte', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$gte': 2}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches array elements', function() {
        var filtered = filter.filterItems(items, {field1: {'$gte': 3}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$gte': 100}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$gte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$gte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$in', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$in': [2, 3]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches array elements', function() {
        var filtered = filter.filterItems(items, {field1: {'$in': [2, 7]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$in': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$in': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('matches subfields of array elements implicitly', function() {
        var items = [
          {_id: 1, field1: [{a: 1}, {a: 2}]},
          {_id: 2, field1: [{a: 2}, {a: 3}]},
          {_id: 3, field1: [{a: 5}, {a: 6}, {a: 7}]}];
        var filtered = filter.filterItems(items, {'field1.a': {'$in': [1, 3]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('does not match subfields of elements of array elements implicitly',
        function() {
          var items = [
            {_id: 1, field1: [[{a: 1}], [{a: 2}]]},
            {_id: 2, field1: [[{a: 2}], [{a: 3}]]},
            {_id: 3, field1: [[{a: 5}], [{a: 6}], [{a: 7}]]}];
          var filtered = filter.filterItems(
            items,
            {'field1.a': {'$in': [1, 3]}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([]);

          items = [
            {_id: 1, field1: [[{a: 1}, {a: 2}]]},
            {_id: 2, field1: [[{a: 2}, {a: 3}]]},
            {_id: 3, field1: [[{a: 5}, {a: 6}, {a: 7}]]}];
          filtered = filter.filterItems(items, {'field1.a': {'$in': [1, 3]}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$nin', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$nin': [2, 3]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches array elements', function() {
        var filtered = filter.filterItems(items, {field1: {'$nin': [2, 7]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$nin': [10, 100]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$nin': [10, null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
      });

      it('matched non-existing array elements to null', function() {
        var filtered = filter.filterItems(
          items,
          {'field1.5': {'$nin': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });
    });

    describe('$all', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {field1: {'$all': [2]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

        filtered = filter.filterItems(items, {field1: {'$all': [1, 2]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('returns nothing when no documents match', function() {
        var filtered = filter.filterItems(items, {field1: {'$all': [1, 7]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not match numbers', function() {
        var filtered = filter.filterItems(items, {_id: {'$all': [1, 7]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not match objects', function() {
        var filtered = filter.filterItems(items, {field2: {'$all': [10]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$all': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('fails with non-array', function() {
        expect(
          function() { filter.filterItems(items, {field1: {'$all': 2}}); })
          .to.throw('BadValue $all needs an array: 2');
      });

      it('fails with an operator in the array', function() {
        expect(
          function() {
            filter.filterItems(items, {field1: {'$all': [{'$gt': 2}]}});
          }).to.throw("BadValue no $ expressions in $all: [ { '$gt': 2 } ]");
      });
    });

    describe('$not', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$not': {'$gt': 2}}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches array elements', function() {
        var filtered = filter.filterItems(
          items,
          {field1: {'$not': {'$eq': 2}}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$not': {'$eq': 10}}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('does not match non-existing fields with {$eq: null}', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$not': {'$eq': null}}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches everything when given a regex literal', function() {
        var filtered = filter.filterItems(items, {_id: {'$not': /2/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

        filtered = filter.filterItems(items, {field1: {'$not': /2/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

        filtered = filter.filterItems(items, {'field2.a': {'$not': /2/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('fails on non-operators', function() {
        expect(
          function() { filter.filterItems(items, {_id: {'$not': 2}}); })
          .to.throw('BadValue $not needs a regex or a document');

        expect(
          function() { filter.filterItems(items, {_id: {'$not': [2]}}); })
          .to.throw('BadValue $not needs a regex or a document');

        expect(
          function() { filter.filterItems(items, {_id: {'$not': {b: 2}}}); })
          .to.throw('BadValue unknown operator: b');
      });

      it('fails with $regex', function() {
        expect(
          function() {
            filter.filterItems(items, {_id: {'$not': {'$regex': '2'}}});
          }).to.throw('BadValue $not cannot have a regex');
      });
    });

    describe('$exists', function() {
      it('matches existing fields with true parameter', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$exists': true}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches non-existing fields with false parameter', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$exists': false}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches existing array elements with true parameter', function() {
        var filtered = filter.filterItems(
          items,
          {'field1.2': {'$exists': true}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches numeric members of array elements with true parameter',
        function() {
          var items = [
            {_id: 1, field1: [{a: 1}, {a: 2}]},
            {_id: 2, field1: [{a: 2}, {'2': 4}]}];
          var filtered = filter.filterItems(
            items,
            {'field1.2': {'$exists': true}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
      });

      it('does not match numeric members of array elements with false param',
        function() {
          var items = [
            {_id: 1, field1: [{a: 1}, {a: 2}]},
            {_id: 2, field1: [{a: 2}, {'2': 4}]}];
          var filtered = filter.filterItems(
            items,
            {'field1.2': {'$exists': false}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches non-existing array elements with false parameter', function() {
        var filtered = filter.filterItems(
          items,
          {'field1.2': {'$exists': false}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('accepts numeric parameter', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$exists': 8}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);

        filtered = filter.filterItems(items, {'field2.a': {'$exists': 0}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches subfields of array elements implicitly', function() {
        var items = [
          {_id: 1, field1: [{a: 1}, {a: 2}]},
          {_id: 2, field1: [{a: 2}, {a: 3}]},
          {_id: 3, field1: [{a: 5}, {a: 6}, {a: 7}]}];

        var filtered = filter.filterItems(
          items,
          {'field1.a': {'$exists': true}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

        filtered = filter.filterItems(items, {'field1.a': {'$exists': false}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);

        filtered = filter.filterItems(items, {'field1.b': {'$exists': true}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);

        filtered = filter.filterItems(items, {'field1.b': {'$exists': false}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('does not match subfields of elements of array elements implicitly',
        function() {
          var items = [
            {_id: 1, field1: [[{a: 1}], [{a: 2}]]},
            {_id: 2, field1: [[{a: 2}], [{a: 3}]]},
            {_id: 3, field1: [[{a: 5}], [{a: 6}], [{a: 7}]]}];
          var filtered = filter.filterItems(
            items,
            {'field1.a': {'$exists': true}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([]);

          filtered = filter.filterItems(
            items,
            {'field1.a': {'$exists': false}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);

          items = [
            {_id: 1, field1: [[{a: 1}, {a: 2}]]},
            {_id: 2, field1: [[{a: 2}, {a: 3}]]},
            {_id: 3, field1: [[{a: 5}, {a: 6}, {a: 7}]]}];

          filtered = filter.filterItems(
            items,
            {'field1.a': {'$exists': true}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([]);

          filtered = filter.filterItems(
            items,
            {'field1.a': {'$exists': false}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });
    });

    describe('$regex', function() {
      it('does not match numbers', function() {
        var filtered = filter.filterItems(items, {_id: {'$regex': '2'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });
  });

  describe('strings support', function() {
    var items = [
      {_id: 'a', field1: ['a', 'b'], field2: {a: 'x', b: 'z'}},
      {_id: 'b', field1: ['b', 'c'], field2: {a: 'y', b: 'zzz'}},
      {_id: 'c', field1: ['d', 'e', 'f\na']}];

    describe('direct field comparison', function() {
      it('matches basic values', function() {
        var filtered = filter.filterItems(items, {_id: 'a'});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

        filtered = filter.filterItems(items, {_id: 'h'});
        expect(filtered).to.deep.equal([]);
      });

      it('matches named subfield values', function() {
        var filtered = filter.filterItems(items, {'field2.a': 'x'});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

        filtered = filter.filterItems(items, {'field2.a': 'a'});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);

        filtered = filter.filterItems(items, {'field2.a': null});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);
      });

      it('matches indexed array elements', function() {
        var filtered = filter.filterItems(items, {'field1.0': 'b'});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);

        filtered = filter.filterItems(items, {'field1.1': 'b'});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);

        filtered = filter.filterItems(items, {'field1.3': null});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('regular expression matching', function() {
      it('works with no options', function() {
        var filtered = filter.filterItems(items, {_id: /[ac]/});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);

        filtered = filter.filterItems(items, {_id: {'$regex': /x+/}});
        expect(filtered).to.deep.equal([]);

        filtered = filter.filterItems(items, {field1: {'$regex': /b/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);

        filtered = filter.filterItems(items, {'field2.b': {'$regex': /z/}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });

      it('works with ignore case option', function() {
        var filtered = filter.filterItems(items, {_id: /A/i});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });

      it('works with multiline option', function() {
        var filtered = filter.filterItems(items, {field1: /^a/m});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
      });
    });

    describe('$eq', function() {
      it('returns results when there are matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$eq': 'a'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });

      it('returns nothing when there are no matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$eq': 'h'}});
        expect(filtered).to.deep.equal([]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$eq': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$eq': 'x'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });

      it('matches non-existent subfields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$eq': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);
      });
    });

    describe('$ne', function() {
      it('returns results when there are matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$ne': 'a'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);

        filtered = filter.filterItems(items, {_id: {'$ne': 'h'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$ne': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$ne': 'x'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);
      });

      it('matches non-existent subfields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$ne': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });
    });

    describe('$lt', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$lt': 'c'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$lt': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.b': {'$lt': 'zz'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });
    });

    describe('$lte', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$lte': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$lte': 'c'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.b': {'$lte': 'zz'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });
    });

    describe('$gt', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$gt': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$gt': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.b': {'$gt': 'z'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);
      });
    });

    describe('$gte', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$gte': 'b'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$gte': 'c'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(items, {'field2.b': {'$gte': 'zz'}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);
      });
    });

    describe('$in', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$in': ['b', 'c']}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b', 'c']);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$in': ['b', 'e']}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$in': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$in': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$nin', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {_id: {'$nin': ['b', 'c']}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$nin': ['b', 7]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.b': {'$nin': ['z', 'zzz']}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['c']);

        filtered = filter.filterItems(
          items,
          {'field2.b': {'$nin': ['z', null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['b']);
      });

      it('matched non-existing array elements to null', function() {
        var filtered = filter.filterItems(
          items,
          {'field1.5': {'$nin': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b', 'c']);
      });
    });

    describe('$regex', function() {
      describe('when specified as string', function() {
        it('returns matching documents', function() {
          var filtered = filter.filterItems(items, {_id: {'$regex': '[ac]'}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
        });

        it('returns nothing when no documents match', function() {
          var filtered = filter.filterItems(items, {_id: {'$regex': 'x+'}});
          expect(filtered).to.deep.equal([]);
        });

        it('matches array elements implicitly', function() {
          var filtered = filter.filterItems(items, {field1: {'$regex': 'b'}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
        });

        it('matches subfields in dot notation', function() {
          var filtered = filter.filterItems(
            items,
            {'field2.b': {'$regex': 'z'}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
        });
      });

      describe('when specified as RegExp', function() {
        it('returns matching documents', function() {
          var filtered = filter.filterItems(items, {_id: {'$regex': /[ac]/}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
        });

        it('returns nothing when no documents match', function() {
          var filtered = filter.filterItems(items, {_id: {'$regex': /x+/}});
          expect(filtered).to.deep.equal([]);
        });

        it('matches array elements implicitly', function() {
          var filtered = filter.filterItems(items, {field1: {'$regex': /b/}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
        });

        it('matches subfields in dot notation', function() {
          var filtered = filter.filterItems(
            items,
            {'field2.b': {'$regex': /z/}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'b']);
        });

        it('accepts the ignore case option', function() {
          var filtered = filter.filterItems(items, {_id: {'$regex': /A/i}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
        });

        it('accepts the multiline option', function() {
          var filtered = filter.filterItems(
            items,
            {'field1': {'$regex': /^a/m}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
        });
      });

      describe('when provided with $options', function() {
        it('accepts the ignore case option', function() {
          var filtered = filter.filterItems(
            items,
            {_id: {'$regex': 'A', '$options': 'i'}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
        });

        it('accepts the multiline option', function() {
          var filtered = filter.filterItems(
            items,
            {'field1': {'$regex': '^a', '$options': 'm'}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
        });
      });

      describe('when given both regex with options and $options', function() {
        it('ignore case $option overrides no ignore case', function() {
          var filtered = filter.filterItems(
            items,
            {_id: {'$regex': /A/, '$options': 'i'}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
        });

        it('no ignore case $option overrides ignore case', function() {
          var filtered = filter.filterItems(
            items,
            {_id: {'$regex': /A/i, '$options': ''}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([]);
        });

        it('multiline $option overrides no multiline', function() {
          var filtered = filter.filterItems(
            items,
            {'field1': {'$regex': /^a/, '$options': 'm'}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a', 'c']);
        });

        it('no multiline $option overrides multiline', function() {
          var filtered = filter.filterItems(
            items,
            {'field1': {'$regex': /^a/m, '$options': ''}});
          expect(_.pluck(filtered, '_id')).to.deep.equal(['a']);
        });
      });
    });
  });

  describe('dates support', function() {
    var date1 = new Date('1995-02-08');
    var date2 = new Date('1998-05-28');
    var date3 = new Date('2009-04-01');
    var date4 = new Date('2011-09-01');
    var date10 = new Date('2014-05-08');
    var date11 = new Date('2014-09-22');
    // Make copies to ensure that lookup does not use identity comparison.
    var date1Copy = new Date(date1.valueOf());
    var date2Copy = new Date(date2.valueOf());
    var date3Copy = new Date(date3.valueOf());
    var date4Copy = new Date(date4.valueOf());
    var date10Copy = new Date(date10.valueOf());
    var date11Copy = new Date(date11.valueOf());
    var items = [
      {_id: 1, field0: date1, field1: [date1, date2], field2: {a: date10}},
      {_id: 2, field0: date2, field1: [date2, date3], field2: {a: date11}},
      {_id: 3, field0: date3, field1: [date3, date4]}];

    describe('direct field comparison', function() {
      it('returns results when there are matching documents', function() {
        var filtered = filter.filterItems(items, {field0: date1Copy});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('returns nothing when there are no matching documents', function() {
        var filtered = filter.filterItems(items, {field0: date10Copy});
        expect(filtered).to.deep.equal([]);
      });

      it('does not match dates and their timestamp values', function() {
        var filtered = filter.filterItems(items, {field0: date1.valueOf()});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('returns documents with matches named subfield values', function() {
        var filtered = filter.filterItems(items, {'field2.a': date10Copy});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('returns no documents when no subfield values match', function() {
        var filtered = filter.filterItems(items, {'field2.a': date1Copy});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': null});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });
    });

    describe('$eq', function() {
      it('returns results when there are matching documents', function() {
        var filtered = filter.filterItems(items, {field0: {'$eq': date1Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('returns nothing when there are no matching documents', function() {
        var filtered = filter.filterItems(items, {field0: {'$eq': date10Copy}});
        expect(filtered).to.deep.equal([]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$eq': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$eq': date10Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$eq': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });
    });

    describe('$ne', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {field0: {'$ne': date1Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);

        filtered = filter.filterItems(items, {field0: {'$ne': date10Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$ne': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$ne': date1Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });
    });

    describe('$lt', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {field0: {'$lt': date3Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$lt': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$lt': date11Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('does not match non-existing array elements', function() {
        var filtered = filter.filterItems(
          items,
          {'field1.5': {'$lt': date11Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not compare dates and their timestamp values', function() {
        var filtered = filter.filterItems(
          items,
          {field0: {'$lt': date2.valueOf()}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$lte', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {field0: {'$lte': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(
          items,
          {field1: {'$lte': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$lte': date10Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$lte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$lte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not compare dates and their timestamp values', function() {
        var filtered = filter.filterItems(
          items,
          {field0: {'$lte': date2.valueOf()}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$gt', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {field0: {'$gt': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$gt': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$gt': date10Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
      });

      it('does not compare to null', function() {
        // Unlike in plain JavaScript, dates do not compare to null.
        var filtered = filter.filterItems(items, {'field2.a': {'$gt': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not match non-existing fields', function() {
        var filtered = filter.filterItems(
          items,
          {'field1.5': {'$gt': date1Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not compare dates and their timestamp values', function() {
        var filtered = filter.filterItems(
          items,
          {field0: {'$gt': date2.valueOf()}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$gte', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {field0: {'$gte': date2Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {field1: {'$gte': date3Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$gte': date11Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$gte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$gte': null}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('does not compare dates and their timestamp values', function() {
        var filtered = filter.filterItems(
          items,
          {field0: {'$gte': date2.valueOf()}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$in', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(
          items,
          {field0: {'$in': [date2Copy, date3Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2, 3]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(
          items,
          {field1: {'$in': [date2Copy, date4Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('matches non-existing fields to null', function() {
        var filtered = filter.filterItems(items, {'field2.a': {'$in': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'field1.5': {'$in': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });
    });

    describe('$nin', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(
          items,
          {field0: {'$nin': [date2Copy, date3Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(
          items,
          {field1: {'$nin': [date2Copy, date4Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });

      it('matches subfields in dot notation', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$nin': [date10Copy, date11Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([3]);
      });

      it('does not match non-existing fields to null', function() {
        var filtered = filter.filterItems(
          items,
          {'field2.a': {'$nin': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches non-existing array elements to null', function() {
        var filtered = filter.filterItems(
          items,
          {'field1.5': {'$nin': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2, 3]);
      });
    });
  });

  describe('ObjectIds support', function() {
    var id1 = new bson.ObjectID();
    var id2 = new bson.ObjectID();
    var id3 = new bson.ObjectID();
    var id4 = new bson.ObjectID();
    var id5 = new bson.ObjectID();
    var id1Copy = new bson.ObjectID(id1.toString());
    var id2Copy = new bson.ObjectID(id2.toString());
    var id3Copy = new bson.ObjectID(id3.toString());
    var id4Copy = new bson.ObjectID(id4.toString());
    var id5Copy = new bson.ObjectID(id5.toString());

    var id1DifferentType = new mongoose.Types.ObjectId(id1.toHexString());
    var id3DifferentType = new mongoose.Types.ObjectId(id3.toHexString());

    var items = [
      {_id: 1, id: id1, ids: [id3, id4]},
      {_id: 2, id: id2, ids: [id4, id5]}];

    describe('direct field comparison', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {id: id1Copy});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('returns nothing when there are no matching documents', function() {
        var filtered = filter.filterItems(items, {id: id3Copy});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {ids: id4Copy});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      if (!(id1DifferentType instanceof bson.ObjectID)) {
        it('matches equal ObjectIDs of different type', function() {
          var filtered = filter.filterItems(items, {id: id1DifferentType});
          expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
        });

        it('matches equal ObjectIDs of different type in array elements',
          function() {
            var filtered = filter.filterItems(items, {ids: id3DifferentType});
            expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
        });
      }
    });

    describe('$eq', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {id: {'$eq': id1Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('returns nothing when there are no matching documents', function() {
        var filtered = filter.filterItems(items, {id: {'$eq': id3Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {ids: {'$eq': id4Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });
    });

    describe('$ne', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(items, {id: {'$ne': id1Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);

        filtered = filter.filterItems(items, {id: {'$ne': id5Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(items, {ids: {'$ne': id5Copy}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      if (!(id1DifferentType instanceof bson.ObjectID)) {
        it('matches ObjectIDs of different type', function() {
          var filtered = filter.filterItems(
            items,
            {id: {'$ne': id1DifferentType}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
        });

        it('matches ObjectIDs of different type in array elements',
          function() {
            var filtered = filter.filterItems(
              items,
              {ids: {'$ne': id3DifferentType}});
            expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
        });
      }
    });

    describe('$in', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(
          items,
          {id: {'$in': [id2Copy, id3Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(
          items, {ids: {'$in': [id3Copy, id5Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('does not match non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'ids.5': {'$in': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([]);
      });

      if (!(id1DifferentType instanceof bson.ObjectID)) {
        it('matches ObjectIDs of different type', function() {
          var filtered = filter.filterItems(
            items,
            {id: {'$in': [id1DifferentType]}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
        });

        it('matches ObjectIDs of different type in array elements',
          function() {
            var filtered = filter.filterItems(
              items,
              {ids: {'$in': [id3DifferentType]}});
            expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
        });
      }
    });

    describe('$nin', function() {
      it('returns matching documents', function() {
        var filtered = filter.filterItems(
          items,
          {id: {'$nin': [id2Copy, id3Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1]);
      });

      it('matches array elements implicitly', function() {
        var filtered = filter.filterItems(
          items, {ids: {'$nin': [id3Copy, id5Copy]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      it('matches non-existing array elements to null', function() {
        var filtered = filter.filterItems(items, {'ids.5': {'$nin': [null]}});
        expect(_.pluck(filtered, '_id')).to.deep.equal([1, 2]);
      });

      if (!(id1DifferentType instanceof bson.ObjectID)) {
        it('matches ObjectIDs of different type', function() {
          var filtered = filter.filterItems(
            items,
            {id: {'$nin': [id1DifferentType]}});
          expect(_.pluck(filtered, '_id')).to.deep.equal([2]);
        });
      }
    });
  });
});
