'use strict';

var _ = require('lodash');
var util = require('util');

var utils = require('../utils');
var BaseCommand = require('./base');
var ElementWrapper = require('../element-wrapper');

var wrapElementForAccess = ElementWrapper.wrapElementForAccess;

function Aggregate(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Aggregate, 'aggregate');

function getErrorReply(errorMessage, code) {
  var error = {ok: false, errmsg: errorMessage};
  if (code) {
    error.code = code;
  }
  return {documents: [error]};
}

function buildValueKey(value) {
  if (utils.isObjectId(value)) {
    return 'objectid:' + value;
  } else if (_.isNumber(value)) {
    return 'number:' + value;
  } else if (_.isString(value)) {
    return 'string:' + value;
  } else if (value === null || value === undefined) {
    return 'null:';
  } else {
    throw new Error('aggreagte: unsupported _id value ' + value);
  }
}

function getFieldExpressionValue(doc, expression) {
  if (/^[$]/.test(expression)) {
    var value = wrapElementForAccess(doc, expression.substring(1)).getValue();
    if (value === undefined) {
      value = null;
    }
    return value;
  } else {
    return expression;
  }
}

function executeGroupStage(groupSpec, documents) {
  if (!('_id' in groupSpec)) {
    return getErrorReply(
      'exception: a group specification must include an _id',
      15955);
  }
  var groups = {};
  for (var i = 0; i < documents.length; ++i) {
    var doc = documents[i];
    var groupId = getFieldExpressionValue(doc, groupSpec._id);

    if (groupId === undefined) {
      groupId = null;
    }
    var groupKey = buildValueKey(groupId);
    var accumulator = groups[groupKey];

    if (!accumulator) {
      accumulator = {_id: groupId};
      groups[groupKey] = accumulator;
    }
    var keys = _.keys(groupSpec);
    for (var j = 0; j < keys.length; ++j) {
      var key = keys[j];
      if (key === '_id') {
        continue;
      }
      var value = groupSpec[key];
      if (!_.isPlainObject(value)) {
        return getErrorReply(
          util.format(
            "exception: the group aggregate field '%s' " +
            'must be defined as an expression inside an object',
            key),
          15951);
      }
      var valueKeys = _.keys(value);
      if (valueKeys.length !== 1) {
        return getErrorReply(
          util.format(
            "exception: the computed aggregate '%s' " +
            'must specify exactly one operator',
            key),
          15954);
      }
      var operator = valueKeys[0];
      var expression = value[operator];
      if (operator === '$sum') {
        var expressionValue = getFieldExpressionValue(doc, expression);
        if (!_.isFinite(expressionValue)) {
          continue;
        }
        accumulator[key] = (accumulator[key] || 0) + expressionValue;
      } else {
        // TODO(vladlosev): Handle more operators.
        return getErrorReply(
          util.format("exception: unknown group operator '%s'", operator),
          15952);
      }
    }
  }
  return _.values(groups);
}

Aggregate.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var query = clientReqMsg.query;
  var pipeline = query.pipeline;
  var docs = collection;

  if (!_.isArray(pipeline)) {
    pipeline = [pipeline];
  }
  for (var index = 0; index < pipeline.length; ++index) {
    var stage = pipeline[index];
    var newDocs = [];
    if (!_.isPlainObject(stage)) {
      return getErrorReply(
        util.format('exception: pipeline element %d is not an object', index),
        15942);
    }
    var keys = _.keys(stage);
    if (keys.length !== 1) {
      return getErrorReply(
        'exception: A pipeline stage specification object ' +
        'must contain exactly one field.',
        16435);
    }
    switch (keys[0]) {
      case '$group':
        newDocs = executeGroupStage(stage.$group, docs);
        if (!_.isArray(newDocs)) {
          return newDocs;  // Is actually an error.
        }
        break;
      // TODO(vladlosev): handle more stages.
      default:
        return getErrorReply(
          util.format(
            "exception: Unrecognized pipeline stage name: '%s'",
            keys[0]),
          16436);
    }
    docs = newDocs;
  }
  return {documents: {ok: true, result: docs}};
};

module.exports = Aggregate;
