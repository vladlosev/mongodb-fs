'use strict';

var _ = require('lodash')
var chai = require('chai')
var ObjectID = require('bson').ObjectID;

var projection = require('../lib/projection');

describe('projection', function() {
  var expect = chai.expect;

  var id1 = new ObjectID();
  var id1Copy = new ObjectID(id1);

  describe('validateProjection', function() {
    it('allows pure inclusion projection', function() {
      chai.expect(projection.validateProjection({x: 1, y: 1}))
        .to.not.exist();
    });

    it('allows inclusion projection with _id excluded', function() {
      chai.expect(projection.validateProjection({x: 1, _id: 0}))
        .to.not.exist();
    });

    it('allows pure inclusion projection', function() {
      chai.expect(projection.validateProjection({x: 0, y: 0}))
        .to.not.exist();
    });

    it('disallows mixed inclusion and exclusion', function() {
      chai.expect(projection.validateProjection({x: 1, y: 0}))
        .to.have.property('message')
        .to.have.string(
          'BadValue Projection cannot have a mix of inclusion and exclusion.');
    });
  });

  describe('getProjection', function() {
    var doc = {_id: id1, a: 1, b: {c: 'd', x: 'y'}};

    it('returns full document for null', function() {
      chai.expect(projection.getProjection([doc], null))
        .to.deep.equal([doc]);
    });

    it('returns full document for empty projection doc', function() {
      chai.expect(projection.getProjection([doc], {}))
        .to.deep.equal([doc]);
    });

    it('selects fields with inclusion projection', function() {
      chai.expect(projection.getProjection([doc], {a: 1}))
        .to.deep.equal([{_id: id1, a: 1}]);
    });

    it('selects subfields with dot notation', function() {
      chai.expect(projection.getProjection([doc], {'b.c': 1}))
        .to.deep.equal([{_id: id1, b: {c: 'd'}}]);
    });

    it('allows _id exclusion in inclusion projection', function() {
      chai.expect(projection.getProjection([doc], {a: 1, _id: 0}))
        .to.deep.equal([{a: 1}]);
    });

    it('excludes fields with exclusion projection', function() {
      chai.expect(projection.getProjection([doc], {b: 0}))
        .to.deep.equal([{_id: id1, a: 1}]);
    });

    it('excludes fields with dot notation', function() {
      chai.expect(projection.getProjection([doc], {'b.c': 0}))
        .to.deep.equal([{_id: id1, a: 1, b: {x: 'y'}}]);
    });

    it('excluding non-existent fields does not affect projection', function() {
      chai.expect(projection.getProjection([doc], {f: 0}))
        .to.deep.equal([doc]);
    });

    it('exclusion can generate empty subdocuments', function() {
      chai.expect(projection.getProjection([doc], {'b.c': 0, 'b.x': 0}))
        .to.deep.equal([{_id: id1, a: 1, b: {}}]);
    });
  });
});
