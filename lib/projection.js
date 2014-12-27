var _ = require('lodash');
var ObjectID = require('bson').ObjectID;

var helper = require('./helper');

function selectField(doc, fieldSelector) {
  if (fieldSelector.length > 0) {
    var key = fieldSelector[0];
    if (key in doc) {
      var value = selectField(doc[key], fieldSelector.slice(1));
      if (value !== undefined) {
        var result = {};
        result[key] = value;
        return result;
      }
    }
    return undefined;
  } else {
    return doc;
  }
}

function excludeField(doc, fieldSelector) {
  if (fieldSelector.length > 0) {
    var key = fieldSelector[0];
    if (key in doc) {
      if (fieldSelector.length === 1) {
        delete doc[key];
      } else {
        excludeField(doc[key], fieldSelector.slice(1));
      }
    }
  }
}

// Validates projectionDoc as a projection document. Returns an Error if
// projectionDoc is invalid or a falsy value if it's valid.
function validateProjection(projectionDoc) {
  var includeFields = Object.keys(_.pick(projectionDoc, function(elem) {
    return elem;
  }));
  var excludeFields = Object.keys(_.pick(projectionDoc, function(elem) {
    return !elem;
  }));
  var result;

  if (includeFields.length > 0) {
    if (excludeFields.length > 1 ||
        (excludeFields.length === 1 && excludeFields[0] !== '_id')) {
      return new Error(
        'BadValue Projection cannot have a mix of inclusion and exclusion.');
    }
  }
  return null;
}

// Returns projections of docuements in docs described by projectionDoc.
// This method assumes projectionDoc has been validated by validateProjection.
// Its behavior is undefined unless validation has been performed.
function getProjection(docs, projectionDoc) {
  if (!projectionDoc || Object.keys(projectionDoc).length === 0) {
    return docs;
  }
  var includeFields = Object.keys(_.pick(projectionDoc, function(elem) {
    return elem;
  }));
  var excludeFields = Object.keys(_.pick(projectionDoc, function(elem) {
    return !elem;
  }));
  var result;

  if (includeFields.length > 0) {
    result = _.map(docs, function(doc) {
      var resultDoc = {};
      if (excludeFields.length === 0) {
        resultDoc._id = doc._id;
      }
      _.forEach(includeFields, function(selector) {
        var selectedPart = selectField(doc, selector.split('.'));
        if (selectedPart !== undefined) {
          _.merge(resultDoc, selectedPart);
        }
      });
      return resultDoc;
    });
  } else {
    result = _.map(docs, function(doc) {
      var resultDoc = helper.cloneDocuments(doc);
      _.forEach(excludeFields, function(selector) {
        excludeField(resultDoc, selector.split('.'));
      });
      return resultDoc;
    });
  }
  return result;
}

module.exports = {
  validateProjection: validateProjection,
  getProjection: getProjection
};
