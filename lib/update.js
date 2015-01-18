var _ = require('lodash');
var util = require('util')
var ObjectID = require('bson').ObjectID;

var helper = require('./helper');

function InputDataError(message) {
  this.message = message;
}
InputDataError.prototype = new Error();

// Determines whethe value is an atomic value (i.e., not an object or an
// array).
function isAtomic(value) {
  if (typeof value === 'number') {
    return true;
  } else if (typeof value === 'string') {
    return true;
  } else if (value instanceof Date) {
    return true;
  } else if (value instanceof ObjectID) {
    return true;
  } else if (value === null) {
    return true;
  }
  return false;
}

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
    return new InputDataError(util.format(
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
        } else if (isAtomic(doc) && !_.isUndefined(doc)) {
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

// Assuming source is a MongoDB selector document, retrieves values from
// equality comparisons (e,g, {a: 1}) and set them in destination.  If source
// contains the $and operator, recurses over its arguments. E.g.,
//   var doc = {};
//   copyEqualityValues(doc, {a: 1, $and: [{b: 2}, {$and: [c: 5, 'd.e': 6]}]});
// will make doc equal {a: 1, b: 2, c: 5, d: {e: 6}}.
//
function copyEqualityValues(destination, source) {
  _.forOwn(source, function(value, key) {
    if (!helper.isOperator(key)) {
      if (isAtomic(value)) {
        if (!helper.isOperator(value)) {
          wrapElementForAccess(destination, key).setValue(value);
        }
      } else {
        copyEqualityValues(destination, value);
      }
    } else if (key === '$and') {
      _.forEach(value, function(elem) {
        copyEqualityValues(destination, elem);
      });
    }
  });
}

function arrayPull(arr, value) {
  var valuesEqual = function(a, b) {
    if (a instanceof ObjectID) {
      return a.equals(b);
    } else if (b instanceof ObjectID) {
      return b.equals(a);
    }
    return _.isEqual(a, b);
  }
  var i = 0;
  while (i < arr.length) {
    if (valuesEqual(value, arr[i])) {
      arr.splice(i, 1);
    } else {
      ++i;
    }
  }
}

function update(docs, query, updateDoc, multiUpdate, upsertedDoc, context) {
  var updateContainsOperators = _.any(
    updateDoc,
    function(value, key) { return helper.isOperator(key); });

  if (upsertedDoc && updateContainsOperators) {
    copyEqualityValues(upsertedDoc, query);
  }
  try {
    _.forEach(docs, function (doc, index) {
      if (!multiUpdate && index > 0) {
        // multi is off and we have already updated one document.
        return false;  // Exit the loop.
      }
      context.affectedDocuments++;
      var value;
      // The document contains no operators, so its contents must be replaced
      // entirely (see
      // http://docs.mongodb.org/manual/reference/method/db.collection.update/#replace-a-document-entirely).
      // Remove here fields that have no corresponding fields in the update
      // document.
      if (!updateContainsOperators) {
        _.forOwn(doc, function(value, key) {
          if (key !== '_id' && !(key in updateDoc)) {
            delete doc[key];
          }
        });
      }
      for (updateKey in updateDoc) {
        if (updateKey === '$setOnInsert' && !upsertedDoc) {
          continue;
        }
        if (updateKey === '$set' ||
            updateKey === '$unset' ||
            updateKey === '$setOnInsert' ||
            updateKey === '$inc' ||
            updateKey === '$pull') {
          for (propKey in updateDoc[updateKey]) {
            var property = wrapElementForAccess(doc, propKey);
            value = updateDoc[updateKey][propKey];
            if (updateKey == '$inc') {
              property.setValue(property.getValue() + value);
            } else if (updateKey === '$unset') {
              property.deleteValue();
            } else if (updateKey === '$pull') {
              // TODO(vladlosev): Support queries in $pull,
              // e.g. db.collection.update({name: 'joe'}, {$pull: {scores: {$lt : 50}}})
              var arr = property.getValue();
              if (_.isUndefined(arr)) continue;
              if (!_.isArray(arr)) {
                throw new InputDataError(
                  'Cannot apply $pull to a non-array value');
              }
              arrayPull(arr, value);
            } else {
              property.setValue(value);
            }
          }
        } else if (updateKey === '$pushAll') {
          for (propKey in updateDoc[updateKey]) {
            var values = updateDoc[updateKey][propKey];
            if (!_.isArray(values)) {
              var valuesTypeName;
              switch (typeof values) {
                case 'string':
                  valuesTypeName = 'String';
                  break;
                case 'number':
                  valuesTypeName = 'NumberDouble';
                  break;
                default:
                  valuesTypeName = 'embedded document';
              }
              throw new InputDataError(
                "$pushAll requires an array of values but was given an " + values);
            }
            var property = wrapElementForAccess(doc, propKey);
            var arr = property.getValue();
            if (_.isUndefined(arr)) {
              property.setValue(values);
            } else if (_.isArray(arr)) {
              property.setValue(arr.concat(values));
            } else {
              throw new InputDataError(
                "The field '" + propKey + "' must be an array.");
            }
          }
        } else if (helper.isOperator(updateKey)) {
          throw new Error('update value "' + updateKey + '" not supported');
        } else {
          // Literal value to set.
          var property = wrapElementForAccess(doc, updateKey);
          property.setValue(updateDoc[updateKey]);
        }
      }
    });
    if (upsertedDoc && !('_id' in upsertedDoc)) {
      upsertedDoc._id = new ObjectID();
    }
  } catch (error) {
    if (error instanceof InputDataError) {
      context.lastError = error.message;
    } else {
      throw error;
    }
  }
}

module.exports = update;
