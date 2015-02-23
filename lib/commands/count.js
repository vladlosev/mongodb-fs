'use strict';

var filter = require('../filter');
var protocol = require('../protocol');

module.exports = function doCount(socket, clientReqMsg) {
  this.logger.info('doCount');
  var dbName = clientReqMsg.fullCollectionName.replace('.$cmd', '');
  var collectionName = clientReqMsg.query.count;
  var collection = this.getCollection(dbName + '.' + collectionName);
  var query = clientReqMsg.query.query;
  var docs = filter.filterItems(collection, query);
  var reply = {documents: {ok: true, n: docs.length}};
  var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
  socket.write(replyBuf);
};
