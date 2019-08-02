'use strict';

var util = require('util');

function BaseCommand(server) {
  this.server = server;
  this.logger = this.server.logger;
}

BaseCommand.setUpCommand = function(CommandType, commandName) {
  util.inherits(CommandType, BaseCommand);
  CommandType.commandName = commandName;
  CommandType.prototype.commandName = commandName;
};

BaseCommand.prototype.getDbAndCollectionName = function(clientReqMsg) {
  var fullCollectionName = clientReqMsg.fullCollectionName;
  var firstDot = fullCollectionName.indexOf('.');
  if (firstDot === -1) {
    throw new Error('BadValue Full collection name must contain comma');
  }
  var databaseName = fullCollectionName.substring(0, firstDot);
  var collectionName = fullCollectionName.substring(firstDot + 1);
  if (collectionName === '$cmd') {
    fullCollectionName = clientReqMsg.query[this.commandName] ||
      clientReqMsg.query[this.commandName.toLowerCase()];
    firstDot = fullCollectionName.indexOf('.');
    collectionName = fullCollectionName;
  }
  return {database: databaseName, collection: collectionName};
};

BaseCommand.prototype.getCollection = function(clientReqMsg) {
   var name = this.getDbAndCollectionName(clientReqMsg);
   return this.server.getCollection(name.database, name.collection);
};

BaseCommand.prototype.ensureCollection = function(clientReqMsg, collection) {
   var name = this.getDbAndCollectionName(clientReqMsg);
   return this.server.ensureCollection(name.database, name.collection, collection);
};

module.exports = BaseCommand;
