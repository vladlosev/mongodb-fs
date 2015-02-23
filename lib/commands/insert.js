'use strict';

var util = require('util');

var BaseCommand = require('./base');

function Insert(server) {
  BaseCommand.call(this, server);
  this.logger = this.server.logger;
}
util.inherits(Insert, BaseCommand);
Insert.commandName = 'insert';
Insert.prototype.commandName = 'insert';

Insert.prototype.handle = function(clientReqMsg) {
  this.logger.info('doInsert');
  var collection = this.getCollection(clientReqMsg);
  var affectedDocuments = 0;
  clientReqMsg.documents.forEach(function(doc) {
    collection.push(doc);
    affectedDocuments++;
  }.bind(this));
  if (affectedDocuments > 0) {
    this.ensureCollection(clientReqMsg, collection);
  }
  return {documents: {ok: true, n: affectedDocuments}};
};

module.exports = Insert;
