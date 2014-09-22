var _ = require('lodash');
var bson = require('bson');
var util = require('util');

var log = require('../lib/log');
 
var logger;

// Allows compareing ObjectIDs loaded from different modules.
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

  var result = true;
  _.forOwn(selector, function(value, key) {
    if (key.length > 0 && key[0] === '$') {
      switch (key) {
        case '$and':
        case '$or':
        case '$nor':
          result &= evaluateTopLevelOperator(context, key, value);
          break;
        default:
          throw new Error('BadValue unknown top level operator: ' + key);
      }
    } else {
      result &= evaluateFieldExpression(context, key, value);
    }
    if (!result) {
      return false;
    }
  });
  logger.trace('Top level result:', result);
  return result;
}

// Evaluates result of matching one of the three top level logical operators:
// $and, $or, or $nor.
function evaluateTopLevelOperator(context, operator, args) {
  logger.trace('In evaluateTopLevelOperator(', context.path, ',', operator, ',', args, ')');

  if (!Array.isArray(args)) {
    throw new Error('argument to must be an array');
  }
  var result = operator === '$and';
  _.forEach(args, function(arg) {
    var currentResult = evaluateTopLevelExpression(context, arg);
    if (operator === '$and' && !currentResult) {
      result = false;
      return false;
    } else if (operator !== '$and' && currentResult) {
      result = true;
      return false;
    }
  });
  return operator === '$nor' ? !result : result;
}

// Evaluates a field expression such as {a: 1, b: {$gt: 2}}.
function evaluateFieldExpression(parentContext, selector, value) {
  logger.trace('In evaluateFieldExpression(', parentContext.path, ',', selector, ',', value, ')');

  var context = createContext(parentContext, selector.split('.'));
  if (_.isPlainObject(value)) {
    var options = {};
    if ('$options' in value) {
      if (!('$regex' in value)) {
        throw new Error('$options needs $regex');
      }
      options.$options = value.$options;
    }
    var haveOperator;
    _.forOwn(value, function(value, key) {
      if (key.match(/^[$]/)) {
        haveOperator = true;
      } 
    });
    var result = true;
    _.forOwn(value, function(value, key) {
      if (haveOperator) {
        if (key !== '$options') {
          result &= evaluateOperator(context, key, value, options);
        }
      } else {
        result &= evaluateValue(context, value);
      }
    });
    return result;
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

  if (!operator.match(/^[$]/)) {
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
          // TODO(vlad): Implement array element matching throughout context
          // chain.
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
          objectsComparable(context, value) && context.value > value;
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
          objectsComparable(context, value) && context.value < value;
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
      result = true;
      _.forOwn(value, function(element, key) {
        if (!/^[$]/.test(key)) {
          throw new Error('BadValue unknown operator: ' + util.format(key));
        } else if (key === '$regex') {
          throw new Error('BadValue $not cannot have a regex');
        }
        result &= evaluateOperator(context, key, element);
      });
      return !result;
    case '$all':
      comparer = function(context, value) {
        if (!Array.isArray(value)) {
          throw new Error('BadValue $all needs an array: ' + util.format(value));
        }
        result = true;
        for (var i = 0; i < value.length; ++i) {
          if (_.isPlainObject(value[i])) {
            _.forOwn(value[i], function(element, key) {
              if (key.match(/^[$]/)) {
                throw new Error(
                  'BadValue no $ expressions in $all: ' + util.format(value));
              }
            });
          }
          var elementContext = createContextSegment(context, i.toString());
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
  result = comparer(context, value);
  if (!result && Array.isArray(context.value)) {
    for (var i = 0; i < context.value.length; ++i) {
       var elementContext = createContextSegment(context, i.toString());
       if (comparer(elementContext, value)) {
         return true;
       }
    }
  }
  logger.trace('Result:', result);
  return result;
}

// Creates context from selectors which is an array obtained from a string
// value of field selector such as 'car.wheels.1.hub' by splitting on dots.
function createContext(parentContext, selectors) {
  logger.trace('In createContext(', parentContext.path, ',', selectors, ')');
  var firstSelector = selectors[0];
  var remainingSelectors = selectors.slice(1);
  var newContext = createContextSegment(parentContext, firstSelector);
  if (remainingSelectors.length > 0) {
    newContext = createContext(newContext, remainingSelectors);
  }
  return newContext;
}

function newContext(path, valueExists, value) {
  return {
    path: path,
    exists: valueExists,
    value: value
  };
}

// Creates a single context segment based on parentContext and elementName.
function createContextSegment(parentContext, elementName) {
  logger.trace('In createContextSegment(', parentContext.path, ',', elementName, ')');

  var path = parentContext.path ? parentContext.path + '.' + elementName : elementName;

  if (!parentContext.exists) {
    return newContext(path, false, parentContext.value);
  }
  if (Array.isArray(parentContext.value)) {
    if (!elementName.match('^[0-9]+$')) {
      // TODO(vladlosev): Implement matching fields without specifying array
      // index.
      // http://docs.mongodb.org/manual/tutorial/query-documents/#match-a-field-without-specifying-array-index
      return newContext(path, false);
    } else {
      elementName = parseInt(elementName);
      if (parentContext.value.length <= elementName) {
        return newContext(path, false);
      } else {
        return newContext(path, true, parentContext.value[elementName]);
      }
    }
  } else if (_.isPlainObject(parentContext.value)) {
    var keyExists = elementName in parentContext.value;
    // Values not present in elements are treated as nulls.
    return newContext(
      path,
      keyExists,
      keyExists ? parentContext.value[elementName] : null);
  } else {
    return newContext(path, false, undefined);
  }
}

module.exports = {
  init: function() {
    logger = log.getLogger();
  },
  filterItems: function(documents, selector) {
    return _.filter(documents, function(doc) {
      var documentContext = {
        path: '',
        exists: true,
        value: doc
      };
      logger.trace('Evaluating document', doc);
      return evaluateTopLevelExpression(documentContext, selector);
    });
  }
};
