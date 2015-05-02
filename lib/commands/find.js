'use strict';

var filter = require('../filter');
var projection = require('../projection');
var utils = require('../utils');
var BaseCommand = require('./base');

function Find(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Find, 'find');

Find.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var query = clientReqMsg.query;
  var docs;
  if ('$query' in query) {
    query = query.$query;
  }
  try {
    if (!projection.validateProjection(clientReqMsg.returnFieldSelector)) {
      throw new utils.InputDataError(
        'BadValue Projection cannot have a mix of inclusion and exclusion.');
    }
    docs = filter.filterItems(collection, query);
  } catch (error) {
    if (error instanceof utils.InputDataError) {
      return {documents: [{'$err': error.message}]};
    } else {
      throw error;
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
