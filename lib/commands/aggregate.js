'use strict';

var _ = require('lodash');
var util = require('util');

var aggregateExpression = require('../aggregate/expression');
var aggregateUtils = require('../aggregate/utils');
var filter = require('../filter');
var utils = require('../utils');
var BaseCommand = require('./base');

function Aggregate(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Aggregate, 'aggregate');

// We are implementing aggregation groups via objects. Objects in JavaScript
// can only have strings as keys, but MongoDB aggregations can be based on all
// kinds of JavaScript types.  In order to distinguish for example, between a
// number 2 and a string '2', we need some extra logic which this function
// provides.
function buildValueKey(value) {
  if (utils.isObjectId(value)) {
    return 'objectid:' + value;
  } else if (_.isNumber(value)) {
    return 'number:' + value;
  } else if (_.isString(value)) {
    return 'string:' + value;
  } else if (_.isDate(value)) {
    return 'date:' + value.toJSON();
  } else if (_.isBoolean(value)) {
    return 'bool:' + value;
  } else if (_.isNull(value) || _.isUndefined(value)) {
    return 'null:';
  } else {
    throw new Error('aggreagte: unsupported _id value ' + value);
  }
}

function minMaxOperation(minmax) {
  return {
    accumulate: function(accumulator, key, value) {
      if (value === null) {
        return;
      }
      if (!(key in accumulator) ||
          minmax(aggregateUtils.compareValues(
            accumulator[key],
            value), 0) === 0) {
        accumulator[key] = value;
      }
    },

    finalize: function(accumulator, key) {
      if (!(key in accumulator)) {
        accumulator[key] = null;
      }
    }
  };
}

var minOperation = minMaxOperation(Math.min);

var maxOperation = minMaxOperation(Math.max);

var sumOperation = {
  accumulate: function(accumulator, key, value) {
    if (_.isFinite(value)) {
      accumulator[key] = (accumulator[key] || 0) + value;
    }
  },

  finalize: function(accumulator, key) {
    if (!(key in accumulator)) {
      accumulator[key] = 0;
    }
  }
};

// Fills groupOperations with operations objects generated from groupSpec.
// Returns the error parsing groupSpec or falsy value when no errors.
function getGroupOperations(groupSpec, groupOperations) {
  var keys = _.keys(groupSpec);

  for (var j = 0; j < keys.length; ++j) {
    var key = keys[j];
    if (key === '_id') {
      continue;
    }
    var value = groupSpec[key];

    if (!_.isPlainObject(value)) {
      return aggregateUtils.getErrorReply(
        util.format(
          "exception: the group aggregate field '%s' " +
          'must be defined as an expression inside an object',
          key),
        15951);
    }
    var valueKeys = _.keys(value);
    if (valueKeys.length !== 1) {
      return aggregateUtils.getErrorReply(
        util.format(
          "exception: the computed aggregate '%s' " +
          'must specify exactly one operator',
          key),
        15954);
    }
    // In the group spec `{_id: '$x', total: {$sum: '$y'}}`, `total` is key,
    // `$sum` is operator, and `$y` is expression.
    var operator = valueKeys[0];
    var expression = value[operator];
    var operation;

    switch (operator) {
      case '$sum':
        operation = sumOperation;
        break;
      case '$min':
        operation = minOperation;
        break;
      case '$max':
        operation = maxOperation;
        break;
      default:
        // TODO(vladlosev): Handle more operators.
        return aggregateUtils.getErrorReply(
          util.format("exception: unknown group operator '%s'", operator),
          15952);
    }
    var parseError = aggregateExpression.validate(expression);
    if (parseError) {
      return parseError;
    }
    groupOperations[key] = {operation: operation, expression: expression};
  }
  return null;
}

// Aggregation pipeline is executed in stages. The results of one stage are fed
// into the next one.  This function performs execution of a single stage on the
// source documents.  If an error is encountered, the function returns the
// error object rather then the array of documents.
function executeGroupStage(groupSpec, documents) {
  if (!('_id' in groupSpec)) {
    return aggregateUtils.getErrorReply(
      'exception: a group specification must include an _id',
      15955);
  }
  var groupOperations = {};
  var parseError = getGroupOperations(groupSpec, groupOperations);
  if (parseError) {
    return parseError;
  }
  var groups = {};
  try {
    for (var i = 0; i < documents.length; ++i) {
      var doc = documents[i];
      var groupId = aggregateExpression.getValue(doc, groupSpec._id);

      if (_.isUndefined(groupId)) {
        groupId = null;
      }
      var groupKey = buildValueKey(groupId);
      var accumulator = groups[groupKey];

      if (!accumulator) {
        accumulator = {_id: groupId};
        groups[groupKey] = accumulator;
      }

      /* eslint-disable no-loop-func */
      _.forOwn(groupOperations, function(aggregate, key) {
        var expressionValue = aggregateExpression.getValue(
          doc,
          aggregate.expression);
        aggregate.operation.accumulate(accumulator, key, expressionValue);
      });
      /* eslint-enable no-loop-func */
    }
    _.forEach(groups, function(group) {
      _.forOwn(groupOperations, function(aggregate, key) {
        aggregate.operation.finalize(group, key);
      });
    });
  } catch (error) {
    if (_.isPlainObject(error)) {
      return error;  // `error` is an error reply to send back.
    }
    throw error;
  }
  return _.values(groups);
}

function executeMatchStage(query, documents) {
  try {
    return filter.filterItems(documents, query);
  } catch (error) {
    if (error instanceof utils.InputDataError) {
      return aggregateUtils.getErrorReply(error.message);
    } else {
      throw error;
    }
  }
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
    if (!_.isPlainObject(stage)) {
      return aggregateUtils.getErrorReply(
        util.format('exception: pipeline element %d is not an object', index),
        15942);
    }
    var keys = _.keys(stage);
    if (keys.length !== 1) {
      return aggregateUtils.getErrorReply(
        'exception: A pipeline stage specification object ' +
        'must contain exactly one field.',
        16435);
    }
    var stageResult;
    switch (keys[0]) {
      case '$group':
        stageResult = executeGroupStage(stage.$group, docs);
        break;
      case '$match':
        stageResult = executeMatchStage(stage.$match, docs);
        break;
      // TODO(vladlosev): handle more stages.
      default:
        stageResult = aggregateUtils.getErrorReply(
          util.format(
            "exception: Unrecognized pipeline stage name: '%s'",
            keys[0]),
          16436);
    }
    // A stage will return either its result which is always an array or a
    // plain object with error response to return to the client.
    if (_.isArray(stageResult)) {
      docs = stageResult;
    } else {
      return stageResult;
    }
  }
  return {documents: {ok: true, result: docs}};
};

module.exports = Aggregate;
