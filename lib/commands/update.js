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

function getErrorReply(message) {
  return {documents: {ok: false, err: message}};
}

Update.prototype.handle = function(clientReqMsg) {
  this.logger.info('doUpdate');
  var collection = this.getCollection(clientReqMsg);
  var docs = filter.filterItems(collection, clientReqMsg.selector);
  this.affectedDocuments = 0;
  var updateContainsOperators = _.any(clientReqMsg.update, keyIsOperator);
  var updateContainsOnlyOperators = _.every(
    clientReqMsg.update,
    keyIsOperator);

  if (clientReqMsg.flags.multiUpdate && !updateContainsOnlyOperators) {
    return getErrorReply('multi update only works with $ operators');
  }
  var literalSubfield = _.findKey(
    clientReqMsg.update,
    function(value, key) {
      return !utils.isOperator(key) && _.contains(key, '.');
    });

  if (literalSubfield) {
    return getErrorReply(util.format(
      "can't have . in field names [%s]",
      literalSubfield));
  }
  var upsertedDoc;
  if (docs.length === 0 && clientReqMsg.flags.upsert) {
    upsertedDoc = {};
    collection.push(upsertedDoc);
    docs = [upsertedDoc];
    this.ensureCollection(clientReqMsg, collection);
  }
  return update(
    docs,
    clientReqMsg.selector,
    clientReqMsg.update,
    clientReqMsg.flags.multiUpdate,
    upsertedDoc);
};

module.exports = Update;