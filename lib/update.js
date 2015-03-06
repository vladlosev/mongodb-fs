'use strict';

var _ = require('lodash');
var util = require('util')
var ObjectID = require('bson').ObjectID;

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

function update(docs, query, updateDoc, multiUpdate, upsertedDoc) {
  var affectedDocuments = 0;
  var error;

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
      var propKey;
      for (var updateKey in updateDoc) {
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
                throw new utils.InputDataError(
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
              throw new utils.InputDataError(
                "$pushAll requires an array of values but was given an " +
                valuesTypeName);
            }
            var property = wrapElementForAccess(doc, propKey);
            var arr = property.getValue();
            if (_.isUndefined(arr)) {
              property.setValue(values);
            } else if (_.isArray(arr)) {
              property.setValue(arr.concat(values));
            } else {
              throw new utils.InputDataError(
                "The field '" + propKey + "' must be an array.");
            }
          }
        } else if (utils.isOperator(updateKey)) {
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
    if (error instanceof utils.InputDataError) {
      return {documents: {
        ok: false,
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
