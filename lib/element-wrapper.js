'use strict';

var _ = require('lodash');
var util = require('util');

var utils = require('./utils');

// Wraps an element of doc accessible via path in the dot notation
// (http://docs.mongodb.org/manual/core/document/#document-dot-notation) in a
// handler object that allows getting or setting the value of the element.  For
// example, wrapElementForAccess({a: {b: 1}}, 'a.b').getValue() will return 1.
// When getting a value, if leaf or intermediate children do not exist, the
// result will be undefined. For example, wrapElementForAccess({a}, 'b') will
// return undefined.  When setting a value which parent does not exists, the
// parent will be created. For example, after running this code:
//   var doc = {a: 1};
//   wrapElementForAccess(doc, 'b.c').setValue(5);
// doc will be {a: 1, b: {c: 5}}.
//
function wrapElementForAccess(doc, path) {
  function newDocTraversalError(selector) {
    return new utils.InputDataError(util.format(
      'cannot use the part (%s of %s) to traverse the element (%s)',
      selector, path, util.format(doc)));
  };
  var wrapElement = function(parentElem, selector) {
    var doc = parentElem.getValue();

    return {
      getValue: function() {
        return _.isUndefined(doc) ? doc : doc[selector];
      },
      setValue: function(value) {
        if (_.isArray(doc)) {
          if (selector.match(/^[0-9]$/)) {
            selector = parseInt(selector);
            while (doc.length <= selector) {
              doc.push(null);
            }
          } else {
            throw newDocTraversalError(selector);
          }
        } else if (utils.isAtomic(doc) && !_.isUndefined(doc)) {
          throw newDocTraversalError(selector);
        }
        if (_.isUndefined(doc)) {
          doc = {};
          parentElem.setValue(doc);
        }
        doc[selector] = value;
      },
      deleteValue: function() {
        if (_.isArray(doc) && selector.match(/^[0-9]$/)) {
          var index = parseInt(selector);
          if (index < doc.length) {
            doc[index] = null;
          }
        } else if (_.isPlainObject(doc)) {
          delete doc[selector];
        }
      }
    };
  }

  var wrapElementAtPath = function(parentElem, selectors) {
    if (selectors.length === 0) {
      return parentElem;
    }
    var currentElem = wrapElement(parentElem, selectors[0]);
    if (currentElem.error) {
      return currentElem;
    }
    return wrapElementAtPath(currentElem, selectors.slice(1));
  };

  return wrapElementAtPath({getValue: function() { return doc; }}, path.split('.'));
}

module.exports = {
  wrapElementForAccess: wrapElementForAccess
};
