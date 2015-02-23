'use strict';

var util = require('util');

var filter = require('../filter');
var projection = require('../projection');
var BaseCommand = require('./base');

function Find(server) {
  BaseCommand.call(this, server);
  this.logger = this.server.logger;
}
util.inherits(Find, BaseCommand);
Find.commandName = 'find';
Find.prototype.commandName = 'find';

Find.prototype.handle = function(clientReqMsg) {
  this.logger.info('doFind');
  var collection = this.getCollection(clientReqMsg);
  var query = clientReqMsg.query;
  var docs;
  if ('$query' in query) {
    query = query.$query;
  }
  try {
    if (!projection.validateProjection(clientReqMsg.returnFieldSelector)) {
      throw new Error(
        'BadValue Projection cannot have a mix of inclusion and exclusion.');
    }
    docs = filter.filterItems(collection, query);
  } catch (err) {
    if (err.message.match(/^BadValue /)) {
      return {documents: [{'$err': err.message}]};
    } else {
      throw err;
    }
  }
  var skip = clientReqMsg.numberToSkip;
  var limit = clientReqMsg.numberToReturn || docs.length;
  if (limit < 0) {
    limit = -limit;
  }
  docs = docs.slice(skip, skip + limit);
  docs = projection.getProjection(docs, clientReqMsg.returnFieldSelector);
  return {documents: docs};
};

module.exports = Find;
