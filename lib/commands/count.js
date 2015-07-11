'use strict';

var filter = require('../filter');
var BaseCommand = require('./base');

function Count(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Count, 'count');

Count.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var query = clientReqMsg.query.query;
  var docs = filter.filterItems(collection, query);
  return {documents: {ok: true, n: docs.length}};
};

module.exports = Count;
