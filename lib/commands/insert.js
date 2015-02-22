'use strict';

module.exports = function doInsert(socket, clientReqMsg) {
  this.logger.info('doInsert');
  var collection = this.getCollection(clientReqMsg);
  this.affectedDocuments = 0;
  clientReqMsg.documents.forEach(function(doc) {
    collection.push(doc);
    this.affectedDocuments++;
  }.bind(this));
};
