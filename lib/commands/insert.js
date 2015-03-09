'use strict';

var utils = require('../utils');
var BaseCommand = require('./base');
var CreateIndexes = require('./create_indexes');

function Insert(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Insert, 'insert');

Insert.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var createIndexesReply = this.handleIndexInsertion(clientReqMsg);
  if (createIndexesReply) {
    return createIndexesReply;
  }

  var affectedDocuments = 0;
  clientReqMsg.documents.forEach(function(doc) {
    collection.push(doc);
    affectedDocuments++;
  });
  if (affectedDocuments > 0) {
    this.ensureCollection(clientReqMsg, collection);
  }
  return {documents: {ok: true, n: affectedDocuments}};
};

Insert.prototype.handleIndexInsertion = function(clientReqMsg) {
  var databaseAndCollectionName = this.getDbAndCollectionName(clientReqMsg);
  var collectionName = databaseAndCollectionName.collection;

  if (collectionName === 'system.indexes') {
    var databaseName = databaseAndCollectionName.database;
    var createIndexesRequest = utils.cloneDocuments(clientReqMsg);

    createIndexesRequest.header.opCode = 2004;  // OP_QUERY.
    createIndexesRequest.fullCollectionName = databaseName + '.$cmd';
    createIndexesRequest.query = {
      createIndexes: collectionName,
      indexes: clientReqMsg.documents
    };

    var createIndexesCommand = new CreateIndexes(this.server);
    var createIndexesReply = createIndexesCommand.handle(createIndexesRequest);
    if (createIndexesReply.documents.errmsg) {
      createIndexesReply.documents.err = createIndexesReply.documents.errmsg;
    }
    return createIndexesReply;
  }
  return undefined;
};

module.exports = Insert;
