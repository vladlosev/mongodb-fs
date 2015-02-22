'use strict';

var filter = require('../filter');
var protocol = require('../protocol');

module.exports = function doCount(socket, clientReqMsg) {
  this.logger.info('doCount');
  var dbName = clientReqMsg.fullCollectionName.replace('.$cmd', '');
  var collection = this.mocks[dbName][clientReqMsg.query.count] || [];
  var query = clientReqMsg.query.query;
  var docs = filter.filterItems(collection, query);
  var reply = {documents: {ok: true, n: docs.length}};
  var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
  socket.write(replyBuf);
};
