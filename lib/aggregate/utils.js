'use strict';

var _ = require('lodash');

var utils = require('../utils');

function getErrorReply(errorMessage, code) {
  var error = {ok: false, errmsg: errorMessage};
  if (code) {
    error.code = code;
  }
  return {documents: [error]};
}

// Ranks a value's type for $min and $max operation. See comment for
// compareValues for details.
function typeRank(value) {
  if (_.isNumber(value)) {
    return 3;
  } else if (_.isString(value)) {
    return 4;
  } else if (utils.isObjectId(value)) {
    return 8;
  } else if (_.isPlainObject(value)) {
    return 5;
  } else if (_.isArray(value)) {
    return 6;
  } else if (_.isBoolean(value)) {
    return 9;
  } else if (_.isDate(value)) {
    return 10;
  } else {
    throw new Error('Unexpected value compared: ' + value);
  }
}

// Compares values for computing result of $min and $max operations of the
// $group stage.  Returns a negative number if a ranks lower than b, zero if
// they rank equal, and a positive if b ranks lower than a.  NOTE: this
// function is intended to compare only JSON objects.  Do not pass in objects
// with circular references!
//
// $min and $max define full order on JavaScript constructs.  The values are
// ranked by type.  Numbers always rank lower than strings, and strings, lower
// than objects.  For objects, keys are are compared lexicographically in the
// order of appearance in the object. For keys that are equal, values are
// compared. For arrays values are elements are compared in lexicographical
// order.  The full type ranking (not fully supported here) is defined in
// http://docs.mongodb.org/manual/reference/bson-types/#bson-types-comparison-order.
//
// null values are not participating in the comparison and null is only
// returned as the result of $max or $min if there are no other values to
// consider.
function compareValues(a, b) {
  var rankDiff = typeRank(a) - typeRank(b);
  if (rankDiff !== 0) {
    return rankDiff;
  }
  var i;

  if (_.isPlainObject(a)) {
    var keysForA = _.keys(a);
    var keysForB = _.keys(b);
    var minKeyLength = Math.min(keysForA.length, keysForB.length);

    for (i = 0; i < minKeyLength; ++i) {
      if (keysForA[i] < keysForB[i]) {
       return -1;
      } else if (keysForA[i] > keysForB[i]) {
        return 1;
      } else {
        var valueDiff = compareValues(a[keysForA[i]], b[keysForB[i]]);
        if (valueDiff !== 0) {
          return valueDiff;
        }
      }
    }
    // At this point, one of the objects in the prefix of another (both keys
    // and values) and we only need to compare lengths.
    return keysForA.length - keysForB.length;
  } else if (_.isArray(a)) {
    var minLength = Math.min(a, b);
    for (i = 0; i < minLength; ++i) {
      var diff = compareValues(a[i], b[i]);
      if (diff !== 0) {
        return diff;
      }
    }
    return a.length - b.length;
  } else if (a < b) {
    return -1;
  } else if (a > b) {
    return 1;
  } else {
    return 0;
  }
}

module.exports = {
  compareValues: compareValues,
  getErrorReply: getErrorReply
};
