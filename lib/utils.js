'use strict';

var _ = require('lodash');
var util = require('util');
var ObjectID = require('bson').ObjectID;

function InputDataError(message, code) {
  this.message = message;
  if (code) {
    this.code = code;
  }
}
util.inherits(InputDataError, Error);


// Allows comparing ObjectIDs loaded from different modules.
function isObjectId(value) {
  return value && value.constructor && value.constructor.name === 'ObjectID';
}

// Helper for _.isEqual to customize ObjectID comparison.  May also be used
// directly, given that values being compared are ObjectIDs.
function objectIdEquals(value1, value2) {
  if (isObjectId(value1) && isObjectId(value2)) {
    return value1.toHexString() === value2.toHexString();
  }
  return undefined;
}

module.exports = {
  // Determines whether a value is an operator (i.e., starts with a $ sign).
  // Assumes the value is a string.
  isOperator: function(value) {
    return value.length > 0 && value[0] === '$';
  },

  isEmpty: function(obj) {
    for (var prop in obj) {
      if (obj.hasOwnProperty(prop)) {
        return false;
      }
    }
    return true;
  },

  // Determines whethe value is an atomic value (i.e., not an object or an
  // array).
  isAtomic: function isAtomic(value) {
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
  },

  cloneDocuments: function cloneDocuments(doc) {
    return _.cloneDeep(
      doc,
      function(value) {
        return value instanceof ObjectID ? new ObjectID(value) : undefined;
      });
  },

  // Allows comparing ObjectIDs loaded from different modules.
  isObjectId: isObjectId,

  // Helper for _.isEqual to customize ObjectID comparison.  May also be used
  // directly, given that values being compared are ObjectIDs.
  objectIdEquals: objectIdEquals,

  // MongoDB-style object comparison.  ObjectIDs use their own comparison code.
  isEqual: function isEqual(value1, value2) {
    return _.isEqual(value1, value2, objectIdEquals);
  },

  safeCallback: function(callback) {
    return callback || function() {};
  },

  InputDataError: InputDataError
};
