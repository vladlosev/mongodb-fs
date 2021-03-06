'use strict';

var filter = require('../filter');
var BaseCommand = require('./base');

function Delete(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Delete, 'delete');

Delete.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var affectedDocuments = 0;

  var queryDoc;
  if (clientReqMsg.query.delete) {
    queryDoc = clientReqMsg.query.deletes[0].q;
  } else {
    queryDoc = clientReqMsg.selector;
  }
  var docs = filter.filterItems(collection, queryDoc);
  var i = 0;
  while (i < collection.length) {
    var item = collection[i];
    if (docs.indexOf(item) !== -1) {
      collection.splice(i, 1);
      affectedDocuments++;
    } else {
      i++;
    }
  }
  return {documents: {ok: true, n: affectedDocuments}};
};

module.exports = Delete;
