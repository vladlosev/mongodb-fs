'use strict';

var BaseCommand = require('./base');

function Insert(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Insert, 'insert');

Insert.prototype.handle = function(clientReqMsg) {
  this.logger.info('doInsert');
  var collection = this.getCollection(clientReqMsg);
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

module.exports = Insert;
