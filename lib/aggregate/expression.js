'use strict';

var _ = require('lodash');
var util = require('util');

var aggregateUtils = require('./utils');
var filter = require('../filter');
var utils = require('../utils');
var ElementWrapper = require('../element-wrapper');

var wrapElementForAccess = ElementWrapper.wrapElementForAccess;

// Retrieves a value for aggregating, based on the field expression
// supplied in an aggregation stage.
// TODO(vladlosev): Extend this to handle different functions defined in the
// aggregation framework and variables.
function getFieldValue(doc, expression) {
  if (/^[$]/.test(expression)) {
    var value = wrapElementForAccess(doc, expression.substring(1)).getValue();
    if (_.isUndefined(value)) {
      value = null;
    }
    return value;
  } else {
    return expression;
  }
}

// Validates an aggregation expression.  Returns null if expression is valid or
// an error reply object to send to the client.
function validateExpression(expression) {
  if (_.isPlainObject(expression)) {
    var keys = _.keys(expression);
    if (keys.length === 0) {
      return null;
    }
    for (var i = 0; i < keys.length; ++i) {
      var operator = keys[i];
      var params = expression[operator];

      if (!/^[$]/.test(operator)) {
        if (i === 0) {
          return aggregateUtils.getErrorReply(
            'exception: field inclusion is not allowed inside of $expressions',
            16420);
        } else {
          return aggregateUtils.getErrorReply(
            util.format(
              'exception: this object is already an operator expression, ' +
              "and can't be used as a document expression (at '%s')",
              operator),
            15990);
        }
      }

      switch (operator) {
        case '$ifNull':
          if (!_.isArray(params) || params.length !== 2) {
            return aggregateUtils.getErrorReply(
              util.format(
                'exception: Expression $ifNull takes exactly 2 arguments. ' +
                '%d were passed in.',
                _.isArray(params) ? params.length : 1),
              16020);
          }
          break;
        default:
          // TODO(vladlosev): Add support for other aggregation expression
          // operators.
          if (i === 0) {
            return aggregateUtils.getErrorReply(
              util.format("exception: invalid operator '%s'", operator),
              15999);
          } else {
            return aggregateUtils.getErrorReply(
              util.format(
                'exception: the operator must be the only field ' +
                "in a pipeline object (at '%s'",
                operator),
              15983);
          }
      }
    }
  }
  return null;
}

// Returns a value of aggregation expression.  Assumes the expression has been
// validated by validateExpression.
// TODO(vladlosev): Extend this to handle different functions defined in the
// aggregation framework and variables.
function getExpressionValue(doc, expression) {
  if (_.isPlainObject(expression)) {
    var keys = _.keys(expression);
    if (keys.length === 0) {
      return expression;
    }
    var operator = keys[0];
    var params = expression[operator];

    switch (operator) {
      case '$ifNull': {
        var firstParamValue = getExpressionValue(doc, params[0]);
        return firstParamValue === null ?
          getExpressionValue(doc, params[1]) :
          firstParamValue;
      }
    }
  } else {
    return getFieldValue(doc, expression);
  }
}

module.exports = {
  validate: validateExpression,
  getValue: getExpressionValue
};
