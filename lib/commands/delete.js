'use strict';

var _ = require('lodash');

var filter = require('../filter');
var BaseCommand = require('./base');

function Delete(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Delete, 'delete');

Delete.prototype.handle = function(clientReqMsg) {
  this.logger.info('doDelete');
  var collection = this.getCollection(clientReqMsg);
  var affectedDocuments = 0;
  var docs = filter.filterItems(collection, clientReqMsg.selector);
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