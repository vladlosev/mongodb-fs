'use strict';

var _ = require('lodash');
var util = require('util');

var filter = require('../filter');
var update = require('../update');
var utils = require('../utils');

function keyIsOperator(value, key) {
  return utils.isOperator(key);
}

module.exports = function doUpdate(socket, clientReqMsg) {
  this.logger.info('doUpdate');
  var collection = this.getCollection(clientReqMsg);
  var docs = filter.filterItems(collection, clientReqMsg.selector);
  this.affectedDocuments = 0;
  var updateContainsOperators = _.any(clientReqMsg.update, keyIsOperator);
  var updateContainsOnlyOperators = _.every(
    clientReqMsg.update,
    keyIsOperator);

  if (clientReqMsg.flags.multiUpdate && !updateContainsOnlyOperators) {
    this.lastError = 'multi update only works with $ operators';
    return;
  }

  var literalSubfield = _.findKey(
    clientReqMsg.update,
    function(value, key) {
      return !utils.isOperator(key) && _.contains(key, '.');
    });
  if (literalSubfield) {
    this.lastError = util.format(
      "can't have . in field names [%s]",
      literalSubfield);
    return;
  }
  var upsertedDoc;
  if (docs.length === 0 && clientReqMsg.flags.upsert) {
    upsertedDoc = {};
    collection.push(upsertedDoc);
    docs = [upsertedDoc];
  }
  update(
    docs,
    clientReqMsg.selector,
    clientReqMsg.update,
    clientReqMsg.flags.multiUpdate,
    upsertedDoc,
    this);
};
