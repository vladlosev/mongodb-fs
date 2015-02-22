'use strict';

var filter = require('../filter');
var projection = require('../projection');
var protocol = require('../protocol');

module.exports = function doFind(socket, clientReqMsg) {
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
      var reply = {documents: [{'$err': err.message}]};
      var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
      socket.write(replyBuf);
      return;
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
  delete this.affectedDocuments;
  var replyBuf = protocol.toOpReplyBuf(clientReqMsg, { documents: docs });
  socket.write(replyBuf);
};
