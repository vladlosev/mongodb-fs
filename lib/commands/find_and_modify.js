'use strict';

var _ = require('lodash');
var util = require('util');

var filter = require('../filter');
var projection = require('../projection');
var update = require('../update');
var utils = require('../utils');
var BaseCommand = require('./base');

function FindAndModify(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(FindAndModify, 'findandmodify');

function getErrorReply(error) {
  if (error instanceof utils.InputDataError) {
    return getErrorReply(error.message);
  }
  return {documents: [{ok: false, errmsg: error}]};
}


FindAndModify.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var updateKeys = Object.keys(clientReqMsg.query.update || {});
  var firstOperatorIndex = _.findIndex(updateKeys, utils.isOperator);
  var firstNonOperatorIndex = _.findIndex(updateKeys, function(key) {
    return !utils.isOperator(key);
  });
  var docs;
  try {
    docs = filter.filterItems(collection, clientReqMsg.query.query);
  } catch (error) {
    return getErrorReply(error);
  }
  if (firstOperatorIndex >= 0 && firstNonOperatorIndex >= 0) {
    var errorMessage;
    if (firstOperatorIndex < firstNonOperatorIndex) {
      errorMessage = util.format(
        'exception: Unknown modifier: %s',
        updateKeys[firstNonOperatorIndex]);
    } else {
      errorMessage = util.format(
        "exception: The dollar ($) prefixed field '%s' " +
        "in '%s' is not valid for storage.",
        updateKeys[firstOperatorIndex],
        updateKeys[firstOperatorIndex]);
    }
    return getErrorReply(errorMessage);
  }
  var literalSubfield = _.findKey(
    clientReqMsg.query.update,
    function(value, key) {
      return !utils.isOperator(key) && _.contains(key, '.');
    });

  if (literalSubfield) {
    return getErrorReply(util.format(
        "exception: The dotted field '%s' in '%s' is not valid for storage.",
        literalSubfield,
        literalSubfield));
  }
  if (!projection.validateProjection(clientReqMsg.query.fields)) {
    return getErrorReply(
      'exception: You cannot currently mix including and excluding fields. ' +
      'Contact us if this is an issue.');
  }
  var returnedDoc = null;
  if (!clientReqMsg.query.new) {
    returnedDoc = utils.cloneDocuments(docs[0]);
  }
  var upsertedDoc;
  if (clientReqMsg.query.update) {
    if (docs.length === 0 && clientReqMsg.query.upsert) {
      upsertedDoc = {};
      docs = [upsertedDoc];
    }
    var updateResult = update(
      docs,
      clientReqMsg.query.query,
      clientReqMsg.query.update,
      false,
      upsertedDoc);
    if (updateResult.documents.err) {
      return getErrorReply(updateResult.documents.err);
    }
  } else if (clientReqMsg.query.remove) {
    if (clientReqMsg.query.new) {
      return getErrorReply("remove and returnNew can't co-exist");
    }
    if (docs.length > 0) {
      _.pull(collection, docs[0]);
    }
  } else {
    return getErrorReply('need remove or update');
  }
  if (upsertedDoc) {
    collection.push(upsertedDoc);
    this.ensureCollection(clientReqMsg, collection);
  }
  if (clientReqMsg.query.new) {
    returnedDoc = docs.length > 0 ? docs[0] : null;
  }
  returnedDoc = projection.getProjection(
    [returnedDoc],
    clientReqMsg.query.fields)[0];
  return {documents: {value: returnedDoc}};
};

module.exports = FindAndModify;
