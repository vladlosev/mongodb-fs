var util = require('util')
  , _ = require('lodash')
  , bson = require('bson')
  , helper = require('./helper')
  , log = require('../lib/log')
  , operators, logger, that;
 
var logger;


function objectIdEquals(value1, value2) {
  // A hack to allow ObjectID implementations loaded from different modules to
  // work together until the proper comparison is implemented.
  if (typeof value1 === 'object' && value1 !== null &&
      value1.constructor && value1.constructor.name === 'ObjectID') {
    if (value1.equals(value2)) {
      return true;
    }
    if (bson.ObjectID.isValid(value2)) {
      return value1.equals(new bson.ObjectID(value2));
    }
    return false;
  }
  return undefined;
}

function equalHelper(value1, value2) {
  var equal = objectIdEquals(value1, value2);
  if (typeof equal !== 'undefined') {
    return equal;
  }
  equal = objectIdEquals(value2, value1);
  if (typeof equal !== 'undefined') {
    return equal;
  }
  return undefined;
}


operators = {
  '$all': function (args) {
    var i, o1;
    o1 = args[0];
    if (typeof o1.indexOf === 'undefined') {
      return false;
    }
    for (i = 1; i < args.length; i++) {
      if (o1.indexOf(args[i]) === -1) {
        return false;
      }
    }
    return true;
  },
  '$eq': function (args) {
    var o1, o2;
    o1 = args[0];
    o2 = args[1];
    if (_.isEqual(o1, o2, equalHelper)) {
      return true;
    } else if (Array.isArray(o1)) {
      return _.find(o1, function(value) { return _.isEqual(value, o2, equalHelper); });
    }
    return false;
  },
  '$gt': function (args) {
    var o1, o2;
    o1 = args[0];
    o2 = args[1];
    return o1 > o2;
  },
  '$gte': function (args) {
    var o1, o2;
    o1 = args[0];
    o2 = args[1];
    return o1 >= o2;
  },
  '$in': function (args) {
    var i, o1, list;
    o1 = args[0];
    return _.find(args.slice(1), function(value) {
      return _.isEqual(value, o1, equalHelper);
    });
  },
  '$lt': function (args) {
    var o1, o2;
    o1 = args[0];
    o2 = args[1];
    return o1 < o2;
  },
  '$lte': function (args) {
    var o1, o2;
    o1 = args[0];
    o2 = args[1];
    return o1 <= o2;
  },
  '$ne': function (args) {
    return !operators.$eq(args);
  },
  '$nin': function (args) {
    return !operators.$in(args);
  },
  '$and': function (array) {
    var i;
    for (i = 0; i < array.length; i++) {
      if (!array[i]) return false;
    }
    return true;
  },
  '$or': function (array) {
    var i;
    for (i = 0; i < array.length; i++) {
      if (array[i]) return true;
    }
    return false;
  },
  '$not': function (args) {
    var expr = args[args.length - 1];
    return !expr;
  }
};

function OpNode(op, args) {
  this.op = op;
  this.field = null;
  this.args = (args && (args instanceof Array ? args : [args])) || [];
}

that = {
  init: function () {
  },
  isOperator: function (s) {
    return s.indexOf('$') === 0;
  },
  getFirstPropertyName: function (o) {
    var key;
    for (key in o) {
      return key;
    }
  },
  findOperator: function (o) {
    var key;
    for (key in o) {
      if (key.indexOf('$') === 0) {
        return key;
      }
    }
    return null;
  },
  evaluateNode: function (node) {
    var i, args, arg, key, expr, value, opKey, stringValue;
    if (node.op) {
      args = [];
      if (node.field) {
        args.push(util.format('item.%s', node.field));
      }
      for (i = 0; i < node.args.length; i++) {
        arg = node.args[i];
        if (arg instanceof OpNode) {
          arg = that.evaluateNode(arg);
        }
        args.push(arg);
      }
      expr = util.format('operators["%s"]([%s])', node.op, args.join(','));
    } else {
      if (typeof node.args[0] === 'object') {
        for (key in node.args[0]) {
          value = node.args[0][key];
          if (typeof value === 'string') {
            expr = util.format('(item.%s == "%s")', key, value);
          } else if (typeof value === 'number'
            || typeof value === 'boolean') {
            expr = util.format('(item.%s == %s)', key, value);
          } else if (typeof value === 'object') {
            for (opKey in operators) {
              if (value.hasOwnProperty(opKey)) {
                if (typeof value[opKey] === 'number' || typeof value[opKey] === 'boolean') {
                  stringValue = util.format('%s', value[opKey]);
                } else if (typeof value[opKey] === 'string') {
                  stringValue = util.format('"%s"', value[opKey]);
                } else {
                  stringValue = util.format('%s', JSON.stringify(value[opKey]));
                }
                expr = util.format('operators["%s"](item.%s, %s)', opKey, key, stringValue);
              }
            }
          }
        }
      } else {
        logger.error('node.args[0] not an object');
      }
    }
    return expr;
  },
  buildOpNode: function (o, parent, field) {
    var key, node, i, operator;
    logger.trace('parent :', parent);
    logger.trace('field :', field);
    if (field === undefined) {
      field = null;
    }
  	  
     if(typeof o == 'object' && o !== null && o.constructor.name == 'ObjectID'){
       o = o.toString();
    }

    if (typeof o !== 'object') {
      logger.debug('o :', o);
      if (parent) {
        if (!parent.op || parent.op === '$and' || parent.op === '$or') {
          node = new OpNode();
          node.op = '$eq';
          node.field = field;
          node.args = [util.format(typeof o === 'string' ? '"%s"' : '%s', o)];
          parent.args.push(node);
        } else {
          parent.args.push(util.format(typeof o === 'string' ? '"%s"' : '%s', o));
        }
        return;
      } else {
        node = new OpNode();
        node.op = '$eq';
        node.field = field;
        node.args = [util.format(typeof o === 'string' ? '"%s"' : '%s', o)];
        logger.debug('node :', node);
        return node;
      }
    }
    if (o instanceof Array) {
      if (parent) {
        for (i = 0; i < o.length; i++) {
          that.buildOpNode(o[i], parent, field);
        }
      }
      return;
    }
    operator = that.findOperator(o);
    if (operator) {
      node = new OpNode();
      node.op = operator;
      node.field = field;
      that.buildOpNode(o[node.op], node, field);
      if (node.args.length === 1 && (node.op === '$and' || node.op === '$or')) {
        node = node.args[0];
      }
      if (parent) {
        parent.args.push(node);
      } else {
        return node;
      }
    } else {
      if (parent) {
        for (key in o) {
          that.buildOpNode(o[key], parent, key);
        }
        return;
      } else {
        node = new OpNode();
        node.op = '$and';
        node.args = [];
        for (key in o) {
          that.buildOpNode(o[key], node, key);
        }
        if (node.args.length === 1 && (node.op === '$and' || node.op === '$or')) {
          node = node.args[0];
        }
        return node;
      }
    }
  },
  filterItems: function (items, selector) {
    var node, rootNode, expr, result, match;
    logger.trace('selector :', selector);
    node = that.buildOpNode(selector);
    rootNode = node;
    logger.debug('rootNode :', util.inspect(rootNode, {depth: null}));
    expr = that.evaluateNode(rootNode);
    logger.debug('expr :', expr);
    result = [];
    items.forEach(function (item) {
      match = eval(expr);
      if (match) {
        result.push(item);
      }
    });
    return result;
  }
};

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
          throw new Error('Must be a top level operator: $and, $or, or $nor');
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

// TODO(vlad): Treat inferred operations differently (e.g.,
// {field: /regexp/} vs. {field: {$regexp: "regexp"}}.
function evaluateOperator(context, operator, value, options) {
  logger.trace('In evaluateOperator(', context, ',', operator, ',', value, ',', options, ')');

  function objectsComparable(context, value) {
     return context.exists &&
            value !== null &&
            (context.value instanceof Date === value instanceof Date);
  }

  function arrayContains(array, value) {
    if (value instanceof Date) {
      var timestamp = +value;
      return 0 <= _.findIndex(array, function(element) {
        return element instanceof Date && +element === timestamp;
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
          return context.exists && _.isEqual(context.value, value);
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
        return _.isEqual(context.value, value) ||
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
        return _.isEqual(context.value, value) ||
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

that = {
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

module.exports = that;
