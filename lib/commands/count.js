'use strict';

var util = require('util');

var filter = require('../filter');
var BaseCommand = require('./base');

function Count(server) {
  BaseCommand.call(this, server);
  this.logger = this.server.logger;
}
util.inherits(Count, BaseCommand);
Count.commandName = 'count';
Count.prototype.commandName = 'count';

Count.prototype.handle = function(clientReqMsg) {
  this.logger.info('doCount');
  var collection = this.getCollection(clientReqMsg);
  var query = clientReqMsg.query.query;
  var docs = filter.filterItems(collection, query);
  return {documents: {ok: true, n: docs.length}};
};

module.exports = Count;
