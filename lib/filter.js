var util = require('util')
  , helper = require('./helper')
  , operators, logger, that;

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
    return o1 == o2;
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
    for (i = 1; i < args.length; i++) {
      if (o1 === args[i]) {
        return true;
      }
    }
    return false;
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
    var o1, o2;
    o1 = args[0];
    o2 = args[1];
    return o1 != o2;
  },
  '$nin': function (args) {
    var i, o1;
    o1 = args[0];
    for (i = 1; i < args.length; i++) {
      if (o1 == args[i]) {
        return false;
      }
    }
    return true;
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
    logger = require('../lib/log').getLogger();
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
  	  
     if(typeof o == 'object' && o.constructor.name == 'ObjectID'){
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

module.exports = that;
