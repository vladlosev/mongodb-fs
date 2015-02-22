'use strict';

var _ = require('lodash');
var util = require('util');

var filter = require('../filter');
var projection = require('../projection');
var protocol = require('../protocol');
var update = require('../update');
var utils = require('../utils');

module.exports = function doFindAndModify(socket, clientReqMsg) {
  function writeErrorReply(clientReqMsg, errorMessage) {
    var reply = {documents: [{ok: false, errmsg: errorMessage}]};
    var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
    socket.write(replyBuf);
  }

  this.logger.info('doFindAndModify');
  var dbName = clientReqMsg.fullCollectionName.replace('.$cmd', '');
  var collectionName = clientReqMsg.query.findandmodify;
  var collection = this.mocks[dbName][collectionName] || [];
  var docs = filter.filterItems(collection, clientReqMsg.query.query);
  var updateKeys = Object.keys(clientReqMsg.query.update || {});
  var firstOperatorIndex = _.findIndex(updateKeys, utils.isOperator);
  var firstNonOperatorIndex = _.findIndex(updateKeys, function(key) {
    return !utils.isOperator(key);
  });
  if (firstOperatorIndex >= 0 && firstNonOperatorIndex >= 0) {
    var errorMessage;
    if (firstOperatorIndex < firstNonOperatorIndex) {
      errorMessage = util.format(
        "exception: Unknown modifier: %s",
        updateKeys[firstNonOperatorIndex]);
    } else {
      errorMessage = util.format(
        "exception: The dollar ($) prefixed field '%s' " +
        "in '%s' is not valid for storage.",
        updateKeys[firstOperatorIndex],
        updateKeys[firstOperatorIndex]);
    }
    writeErrorReply(clientReqMsg, errorMessage);
    return;
  }
  var literalSubfield = _.findKey(
    clientReqMsg.query.update,
    function(value, key) {
      return !utils.isOperator(key) && _.contains(key, '.');
    });
  if (literalSubfield) {
    writeErrorReply(
      clientReqMsg,
      util.format(
        "exception: The dotted field '%s' in '%s' is not valid for storage.",
        literalSubfield,
        literalSubfield));
    return;
  }
  if (!projection.validateProjection(clientReqMsg.query.fields)) {
    writeErrorReply(
      clientReqMsg,
      'exception: You cannot currently mix including and excluding fields. ' +
      'Contact us if this is an issue.');
    return;
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
    update(
      docs,
      clientReqMsg.query.query,
      clientReqMsg.query.update,
      false,
      upsertedDoc,
      this);
  } else if (clientReqMsg.query.remove) {
    if (clientReqMsg.query.new) {
      writeErrorReply(clientReqMsg, "remove and returnNew can't co-exist");
      return;
    }
    if (docs.length > 0) {
      _.pull(collection, docs[0]);
    }
  } else {
    this.lastError = 'need remove or update';
  }
  if (this.lastError) {
    writeErrorReply(clientReqMsg, this.lastError);
    delete this.lastError;
    return;
  }
  if (upsertedDoc) {
    if (!this.mocks[dbName][collectionName]) {
      this.mocks[dbName][collectionName] = docs;
    } else {
      collection.push(upsertedDoc);
    }
  }
  if (clientReqMsg.query.new) {
    returnedDoc = docs.length > 0 ? docs[0] : null;
  }
  returnedDoc = projection.getProjection(
    [returnedDoc],
    clientReqMsg.query.fields)[0];
  var reply = {documents: {value: returnedDoc}};
  var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
  socket.write(replyBuf);
};
