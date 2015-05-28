'use strict';

var _ = require('lodash');
var util = require('util');

var aggregateUtils = require('./utils');
var ElementWrapper = require('../element-wrapper');

var wrapElementForAccess = ElementWrapper.wrapElementForAccess;

function getTypeName(value) {
  if (_.isNumber(value)) {
    return 'NumberDouble';
  } else if (_.isString(value)) {
    return 'String';
  } else if (_.isDate(value)) {
    return 'Date';
  } else if (_.isBoolean(value)) {
    return 'Bool';
  } else if (value === null) {
    return 'EOO';
  } else if (_.isArray(value)) {
    return 'Array';
  } else {
    return 'Object';
  }
}

// Retrieves a value for aggregating, based on the field expression
// supplied in an aggregation stage.
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
        case '$size':
          if (_.isArray(params) && params.length !== 1) {
             return aggregateUtils.getErrorReply(
               util.format(
                 'exception: Expression $size takes exactly 1 arguments. ' +
                 '%d were passed in.',
                 params.length),
               16020);
          }
          break;
        case '$cond':
          if (_.isArray(params)) {
            if (params.length !== 3) {
              return aggregateUtils.getErrorReply(
                util.format(
                  'exception: Expression %s takes exactly 3 arguments. ' +
                  '%d were passed in.',
                  operator,
                  params.length),
                16020);
            }
          } else if (_.isPlainObject(params)) {
            if (!('if' in params)) {
              return aggregateUtils.getErrorReply(
                util.format(
                  "exception: Missing 'if' parameter to %s",
                  operator),
                17080);
            }
            if (!('then' in params)) {
              return aggregateUtils.getErrorReply(
                util.format(
                  "exception: Missing 'then' parameter to %s",
                  operator),
                17080);
            }
            if (!('else' in params)) {
              return aggregateUtils.getErrorReply(
                util.format(
                  "exception: Missing 'else' parameter to %s",
                  operator),
                17080);
            }
            var otherParams = _.keys(_.omit(params, 'if', 'then', 'else'));
            if (!_.isEmpty(otherParams)) {
              return aggregateUtils.getErrorReply(
                util.format(
                  'exception: Unrecognized parameter to %s: %s',
                  operator,
                  otherParams[0]),
                17083);
            }
          } else {
            return aggregateUtils.getErrorReply(
              util.format(
                'exception: Expression %s takes exactly 3 arguments. ' +
                '1 were passed in.',
                operator),
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
      case '$cond': {
        var ifExpression;
        var thenExpression;
        var elseExpression;
        if (_.isArray(params)) {
          ifExpression = params[0];
          thenExpression = params[1];
          elseExpression = params[2];
        } else {
          ifExpression = params.if;
          thenExpression = params.then;
          elseExpression = params.else;
        }
        var ifValue = getExpressionValue(doc, ifExpression);
        var thenValue = getExpressionValue(doc, thenExpression);
        var elseValue = getExpressionValue(doc, elseExpression);
         return ifValue === false || ifValue === 0 || ifValue === null ?
          elseValue :
          thenValue;
      }
      case '$size': {
        var paramValue = getExpressionValue(
          doc,
          _.isArray(params) ? params[0] : params);
        if (_.isArray(paramValue)) {
          return paramValue.length;
        } else {
          throw aggregateUtils.getErrorReply(util.format(
              'exception: The argument to $size must be an Array, ' +
              'but was of type: %s',
              getTypeName(paramValue)),
            17124);
        }
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
