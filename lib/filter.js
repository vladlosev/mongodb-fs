var _ = require('lodash');
var bson = require('bson');
var util = require('util');

var log = require('../lib/log');
 
var logger;

// Allows comparing ObjectIDs loaded from different modules.
function isObjectId(value) {
  return value && value.constructor && value.constructor.name === 'ObjectID';
}

// Helper for _.isEqual to customize ObjectID comparison.  May also be used
// directly, given that values being compared are ObjectIDs.
function objectIdEquals(value1, value2) {
  if (isObjectId(value1) && isObjectId(value2)) {
    return value1.equals(value2);
  }
  return undefined;
}

// Evaluates matching of a top-level expression which can be either a logical
// operator expression or a field expression.  Returns true if the element in
// context matches the selector.
function evaluateTopLevelExpression(context, selector) {
  logger.trace('In evaluateTopLevelExpression(', context.path, ',', selector, ')');

  var result = _.every(selector, function(value, key) {
    if (key.length > 0 && key[0] === '$') {
      switch (key) {
        case '$and':  // fallthrough
        case '$or':   // fallthrough
        case '$nor':
          return evaluateTopLevelOperator(context, key, value);
          break;
        default:
          throw new Error('BadValue unknown top level operator: ' + key);
      }
    } else {
      return evaluateFieldExpression(context, key, value);
    }
  });
  logger.trace('Top level result:', result);
  return result;
}

// Evaluates result of matching one of the three top level logical operators:
// $and, $or, or $nor, e.g. {$or: [{a: 1}, {b: 'x'}]}.  Returns true if the
// value in the context (typically, a document in a MongoDB collection) matches
// the selector in args.
function evaluateTopLevelOperator(context, operator, args) {
  logger.trace('In evaluateTopLevelOperator(', context.path, ',', operator, ',', args, ')');

  if (!Array.isArray(args)) {
    throw new Error('BadValue ' + operator + ' needs an array');
  }
  var result = operator === '$and';
  _.forEach(args, function(arg) {
    var currentResult = evaluateTopLevelExpression(context, arg);
    if (operator === '$and' && !currentResult) {
      result = false;
      return false;  // Make forEach drop out of the loop.
    } else if (operator !== '$and' && currentResult) {
      result = true;
      return false;
    }
  });
  return operator === '$nor' ? !result : result;
}

// Evaluates a field expression such as {a: 1, b: {$gt: 2}}.
function evaluateFieldExpression(parentContext, selector, value) {
  logger.trace('In evaluateFieldExpression(', parentContext.path.join('.'), ',', selector, ',', value, ')');

  var context = createContext(parentContext, selector.split('.'));
  if (_.isPlainObject(value)) {
    var options = {};
    if ('$options' in value) {
      if (!('$regex' in value)) {
        throw new Error('$options needs $regex');
      }
      options.$options = value.$options;
    }
    var haveOperator = _.some(value, function(val, key) {
      return /^\$/.test(key);
    });
    return _.every(value, function(value, key) {
      if (haveOperator) {
        if (key === '$options') {
          // Ignore the $options key here; it's not a real operator.
          return true;
        }
        return evaluateOperator(context, key, value, options);
      } else {
        return evaluateValue(context, value);
      }
    });
  } else {
    return evaluateValue(context, value);
  }
}

// Evaluates a value in a given context.  The value may be either a literal
// such as 45, 'a', null, or a JavaScript regular expression such as /ab+/.
// The values are compared directly to the value of the context and regular
// expression are tested against them.  Returns true if the result of the match
// is a success.
function evaluateValue(context, value) {
  if (_.isRegExp(value)) {
    // Only allow i and m options, per
    // http://docs.mongodb.org/manual/reference/operator/query/regex/#op._S_options.
    var regexpOptions = '';
    if (value.ignoreCase) {
      regexpOptions += 'i';
    }
    if (value.multiline) {
      regexpOptions += 'm';
    }
    var options = {inferred: true};
    if (regexpOptions) {
      options.$options = regexpOptions;
    }
    return evaluateOperator(context, '$regex', value.source, options);
  } else {
    return evaluateOperator(context, '$eq', value, {inferred: true});
  }
}

// Evaluates operator such as {$gt: 5} in a given context.
function evaluateOperator(context, operator, value, options) {
  logger.trace('In evaluateOperator(', context, ',', operator, ',', value, ',', options, ')');

  // Returns true if the object in the context can be compared for inequality
  // against the given value.  This is necessary because JavaScript treats the
  // dates as their numeric values for such comparisons and Mongo does not.
  function objectsComparable(context, value) {
     return context.exists &&
            value !== null &&
            (context.value instanceof Date === value instanceof Date);
  }

  // Returns true if array contains value.  Treats Date and ObjectID objects in
  // a special manner.
  function arrayContains(array, value) {
    if (value instanceof Date) {
      var timestamp = +value;
      return 0 <= _.findIndex(array, function(element) {
        return element instanceof Date && +element === timestamp;
      });
    } else if (isObjectId(value)) {
      return 0 <= _.findIndex(array, function(element) {
        return objectIdEquals(value, element);
      });
    } else {
      return _.contains(array, value);
    }
  }

  if (!/^[$]/.test(operator)) {
    throw new Error('BadValue unknown operator ' + key);
  }
  var comparer;
  var result;
  switch (operator) {
    case '$eq':
      comparer = function(context, value) {
        if (!context.exists && value === null) {
          // context.value for out-of-bounds array items will be undefined and
          // this comparison will return false, so null will not match out of
          // bounds elements or their children.
          return context.value === null;
        } else {
          return context.exists && _.isEqual(context.value, value, objectIdEquals);
        }
      };
      break;
    case '$ne':
      return !evaluateOperator(context, '$eq', value, {inferred: true});
    case '$gt':
      comparer = function(context, value) {
        return objectsComparable(context, value) && context.value > value;
      };
      break;
    case '$gte':
      comparer = function(context, value) {
        return _.isEqual(context.value, value, objectIdEquals) ||
          (objectsComparable(context, value) && context.value > value);
      };
      break;
    case '$lt':
      comparer = function(context, value) {
        return objectsComparable(context, value) && context.value < value;
      };
      break;
    case '$lte':
      comparer = function(context, value) {
        return _.isEqual(context.value, value, objectIdEquals) ||
          (objectsComparable(context, value) && context.value < value);
      }
      break;
    case '$in':
      comparer = function(context, value) {
        if (context.value === undefined) {
          return false;
        }
        return arrayContains(value, context.value);
      };
      break;
    case '$nin':
      comparer = function(context, value) {
        return !arrayContains(value, context.value);
      };
      break;
    case '$exists':
      if (!value) {
        // {$exists: false} in MongoDB works not by trying to find all context
        // that do not exist but rather by trying to find all contexts that
        // exist and matchig if it fails to find any.  In that respect it is
        // like {$not: {$exists: true}}.
        return !evaluateOperator(context, operator, !value);
      }
      comparer = function(context, value) {
        return context.exists === !!value;
      };
      break;
    case '$not':
      if (_.isRegExp(value)) {
        return !evaluateValue(context, value);
      }
      if (!_.isPlainObject(value)) {
        throw new Error(
          'BadValue $not needs a regex or a document: ' + util.format(value));
      }
      return !_.every(value, function(element, key) {
        if (!/^[$]/.test(key)) {
          throw new Error('BadValue unknown operator: ' + util.format(key));
        } else if (key === '$regex') {
          throw new Error('BadValue $not cannot have a regex');
        }
        return evaluateOperator(context, key, element);
      });
    case '$all':
      comparer = function(context, value) {
        if (!Array.isArray(value)) {
          throw new Error('BadValue $all needs an array: ' + util.format(value));
        }
        result = true;
        for (var i = 0; i < value.length; ++i) {
          if (_.isPlainObject(value[i])) {
            _.forOwn(value[i], function(element, key) {
              if (/^[$]/.test(key)) {
                throw new Error(
                  'BadValue no $ expressions in $all: ' + util.format(value));
              }
            });
          }
          var elementContext = createContext(context, [i.toString()]);
          result &= evaluateValue(context, value[i]);
        }
        return result;
      };
      break;
    case '$regex':
      var regexp;
      if ('$options' in options) {
        if (_.isRegExp(value)) {
          // Just keep the expression and let options be overridden.
          value = value.source;
        }
        regexp = new RegExp(value, options.$options);
      } else {
        regexp = new RegExp(value);
      }
      comparer = function(context, value) {
        if (!context.exists || !_.isString(context.value)) {
          return false;
        }
        return regexp.test(context.value);
      };
      break;
    default:
      throw new Error('BadValue unknown operator: ' + operator);
  }
  result = evaluateComparerOrRecurse(context, comparer, value);
  logger.trace('Result:', result);
  return result;
}

// A helper that either recurses evaluateComparerInContext when there are
// child elements or invokes comparer directly.
function evaluateComparerOrRecurse(context, comparer, value) {
  if (context.path.length === 0) {
    logger.trace('Terminating evaluateComparerOrRecurse(', context, ',', value, ')');
    if (!comparer(context, value)) {
      return evaluateArrayElements(context, comparer, value);
    }
    return true;
  } else {
    return evaluateComparerInContext(context, comparer, value);
  }
}

// Runs comparer recursively against array elements until a match is found.
function evaluateArrayElements(context, comparer, value) {
  if (_.isArray(context.value) && !context.parentIterating) {
    for (var i = 0; i < context.value.length; ++i) {
      var result = evaluateComparerOrRecurse(
        {
          path: context.path,
          value: context.value[i],
          exists: true,
          parentIterating: true
        },
        comparer,
        value);
      if (result) {
        return true;
      }
    }
  }
  return false;
}

function evaluateComparerInContext(context, comparer, value) {
  logger.trace('In evaluateComparerInContext(', context, ',', value, ')');
  var firstSegment = context.path[0];
  var selectorIsNumeric = /^[0-9]+$/.test(firstSegment);
  var remainingSegments = context.path.slice(1);
  var result;

  if (_.isArray(context.value)) {
    if (selectorIsNumeric) {
      var index = parseInt(firstSegment);
      if (index < context.value.length) {
        result = evaluateComparerOrRecurse(
          {path: remainingSegments, value: context.value[index], exists: true},
          comparer,
          value);
      } else {
        // Children will match negative assertions such as {$exists: false}
        // in this context and we have to allow that situation.
        result = evaluateComparerOrRecurse(
          {path: remainingSegments, value: undefined, exists: false},
          comparer,
          value);
      }
      if (result) {
        return true;
      }
    }
  }
  if (context.exists) {
    if (_.isArray(context.value)) {
      // We failed to match comparer and value against parent array element
      // directly and now will try to match against all array elements.
      if (context.parentIterating) {
        // This is an 'array within array' situation (the parent is iterating
        // meaning it's also an array).  We don't iterate second level array
        // children and our selector has failed to match the array element.
        // Arrays don't have any named objects thus the selector will not be
        // able to match anything but we still need to run the comparer against
        // it in case it contains a negative assertion such as $ne or {$exists:
        // false} which may still match.
        result = evaluateComparerOrRecurse(
          {path: remainingSegments, value: undefined, exists: false},
          comparer,
          value);
      } else {
        result = evaluateArrayElements(context, comparer, value);
      }
    } else if (!_.isPlainObject(context.value)) {
      // Children will match negative assertions such as {$exists: false}
      // in this context and we have to allow that situation.
      result = evaluateComparerOrRecurse(
        {path: remainingSegments, value: undefined, exists: false},
        comparer,
        value);
    } else if (firstSegment in context.value) {
      result = evaluateComparerOrRecurse(
        {
          path: remainingSegments,
          value: context.value[firstSegment],
          exists: true
        },
        comparer,
        value);
    } else {
      result = evaluateComparerOrRecurse(
        {path: remainingSegments, value: null, exists: false},
        comparer,
        value);
    }
  } else {
    result = evaluateComparerOrRecurse(
      {path: remainingSegments, value: null, exists: false},
      comparer,
      value);
  }
  return result;
}

// Creates context from selectors which is an array obtained from a string
// value of field selector such as 'car.wheels.1.hub' by splitting on dots.
function createContext(parentContext, selectors) {
  return {
    value: parentContext.value,
    path: parentContext.path.concat(selectors),
    exists: parentContext.exists
  };
}

module.exports = {
  init: function() {
    logger = log.getLogger();
  },
  filterItems: function(documents, selector) {
    return _.filter(documents, function(doc) {
      var documentContext = {
        path: [],
        exists: true,
        value: doc
      };
      logger.trace('Evaluating document', doc);
      return evaluateTopLevelExpression(documentContext, selector);
    });
  }
};
