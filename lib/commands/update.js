'use strict';

var _ = require('lodash');
var util = require('util');

var filter = require('../filter');
var update = require('../update');
var utils = require('../utils');
var BaseCommand = require('./base');

function keyIsOperator(value, key) {
  return utils.isOperator(key);
}

function Update(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Update, 'update');

function getErrorReply(error) {
  if (error instanceof utils.InputDataError) {
    return getErrorReply(error.message);
  } else if (error instanceof Error) {
    throw error;
  }
  return {documents: {ok: false, err: error}};
}

Update.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var queryDoc;
  var updateDoc;
  var isMultiUpdate;
  var isUpsert;

  if (clientReqMsg.query.update) {
    queryDoc = clientReqMsg.query.updates[0].q;
    updateDoc = clientReqMsg.query.updates[0].u;
    isMultiUpdate = clientReqMsg.query.updates[0].multi;
    isUpsert = clientReqMsg.query.updates[0].upsert;
  } else {
    queryDoc = clientReqMsg.selector;
    updateDoc = clientReqMsg.update;
    isMultiUpdate = clientReqMsg.flags.multiUpdate;
    isUpsert = clientReqMsg.flags.upsert;
  }

  var docs;
  try {
    docs = filter.filterItems(collection, queryDoc);
  } catch (error) {
    return getErrorReply(error);
  }
  var updateContainsOnlyOperators = _.every(updateDoc, keyIsOperator);

  if (isMultiUpdate && !updateContainsOnlyOperators) {
    return getErrorReply('multi update only works with $ operators');
  }
  var literalSubfield = _.findKey(
    updateDoc,
    function(value, key) {
      return !utils.isOperator(key) && _.contains(key, '.');
    });

  if (literalSubfield) {
    return getErrorReply(util.format(
      "can't have . in field names [%s]",
      literalSubfield));
  }
  var upsertedDoc;
  if (docs.length === 0 && isUpsert) {
    upsertedDoc = {};
    collection.push(upsertedDoc);
    docs = [upsertedDoc];
    this.ensureCollection(clientReqMsg, collection);
  }
  return update(docs, queryDoc, updateDoc, isMultiUpdate, upsertedDoc);
};

module.exports = Update;
