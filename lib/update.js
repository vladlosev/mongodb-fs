'use strict';

var _ = require('lodash');
var ObjectID = require('bson').ObjectID;

var filter = require('./filter');
var utils = require('./utils');
var ElementWrapper = require('./element-wrapper');

var wrapElementForAccess = ElementWrapper.wrapElementForAccess;

// Assuming source is a MongoDB selector document, retrieves values from
// equality comparisons (e,g, {a: 1}) and set them in destination.  If source
// contains the $and operator, recurses over its arguments. E.g.,
//   var doc = {};
//   copyEqualityValues(doc, {a: 1, $and: [{b: 2}, {$and: [c: 5, 'd.e': 6]}]});
// will make doc equal {a: 1, b: 2, c: 5, d: {e: 6}}.
//
function copyEqualityValues(destination, source) {
  _.forOwn(source, function(value, key) {
    if (!utils.isOperator(key)) {
      if (utils.isAtomic(value)) {
        if (!utils.isOperator(value)) {
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
  function valuesEqual(a, b) {
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

function operatorPull(property, value) {
  var arr = property.getValue();
  if (_.isUndefined(arr)) {
    return;
  }
  if (!_.isArray(arr)) {
    throw new utils.InputDataError('Cannot apply $pull to a non-array value');
  }
  var keys = _.keys(value);
  if (_.isPlainObject(value)) {
    var matchingElements;
    if (utils.isOperator(keys[0]) && !filter.isTopLevelOperator(keys[0])) {
      matchingElements = filter.filterItemsByQuery(arr, value);
    } else {
      matchingElements = filter.filterItems(arr, value);
    }
    for (var i = 0; i < matchingElements.length; ++i) {
      arrayPull(arr, matchingElements[i]);
    }
  } else {
    arrayPull(arr, value);
  }
}

function operatorPushAll(property, value, options) {
  if (!_.isArray(value)) {
    var valueTypeName;
    switch (typeof value) {
      case 'string':
        valueTypeName = 'String';
        break;
      case 'number':
        valueTypeName = 'NumberDouble';
        break;
      default:
        valueTypeName = 'embedded document';
    }
    throw new utils.InputDataError(
      '$pushAll requires an array of values but was given an ' +
      valueTypeName);
  }
  var arr = property.getValue();
  if (_.isUndefined(arr)) {
    property.setValue(value);
  } else if (_.isArray(arr)) {
    property.setValue(arr.concat(value));
  } else {
    throw new utils.InputDataError(
      "The field '" + options.fieldName + "' must be an array.");
  }
}

var updateOperations = {
  '$set': function(property, value) { property.setValue(value); },
  '$unset': function(property) { property.deleteValue(); },
  '$inc': function(property, value) {
    property.setValue(property.getValue() + value);
  },
  '$setOnInsert': function(property, value, options) {
    if (options.upsertedDoc) {
      property.setValue(value);
    }
  },
  '$pull': operatorPull,
  '$pushAll': operatorPushAll
};

function update(docs, query, updateDoc, multiUpdate, upsertedDoc) {
  var affectedDocuments = 0;

  var updateContainsOperators = _.any(
    updateDoc,
    function(value, key) { return utils.isOperator(key); });

  if (upsertedDoc && updateContainsOperators) {
    copyEqualityValues(upsertedDoc, query);
  }
  try {
    _.forEach(docs, function(doc, index) {
      if (!multiUpdate && index > 0) {
        // multi is off and we have already updated one document.
        return false;  // Exit the loop.
      }
      affectedDocuments++;
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
      for (var updateKey in updateDoc) {
        if (utils.isOperator(updateKey)) {
          var operation = updateOperations[updateKey];
          if (operation) {
            for (var propKey in updateDoc[updateKey]) {
              var property = wrapElementForAccess(doc, propKey);
              var value = updateDoc[updateKey][propKey];
              operation(
                property,
                value,
                {upsertedDoc: upsertedDoc, fieldName: propKey});
            }
          } else {
            throw new Error("update value '" + updateKey + "' not supported");
          }
        } else {
          // Literal value to set.
          property = wrapElementForAccess(doc, updateKey);
          property.setValue(updateDoc[updateKey]);
        }
      }
    });
    if (upsertedDoc && !('_id' in upsertedDoc)) {
      upsertedDoc._id = new ObjectID();
    }
  } catch (error) {
    if (error instanceof utils.InputDataError) {
      return {documents: {
        ok: 0,
        err: error.message,
        n: affectedDocuments
      }};
    } else {
      throw error;
    }
  }
  return {documents: {ok: true, n: affectedDocuments}};
}

module.exports = update;
