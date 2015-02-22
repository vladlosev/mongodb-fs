var _ = require('lodash');
var util = require('util');
var net = require('net');

var protocol = require('./protocol');
var filter = require('./filter');
var projection = require('./projection');
var update = require('./update');
var utils = require('./utils');

var cmdFind = require('./commands/find');
var cmdCount = require('./commands/count');
var cmdInsert = require('./commands/insert');
var cmdDelete = require('./commands/delete');
var cmdUpdate = require('./commands/update');
var cmdFindAndModify = require('./commands/find_and_modify');
var cmdDistinct = require('./commands/distinct');

that = {
  mocks: null,

  init: function(mocks) {
    that.logger = require('../lib/log').getLogger('processor');
    that.mocks = mocks;
    filter.init();
  },

  getCollection: function(clientReqMsg) {
    var fullCollectionName;
    if (clientReqMsg.fullCollectionName) {
      fullCollectionName = clientReqMsg.fullCollectionName;
    } else {
      fullCollectionName = clientReqMsg;
    }
    var match = /([^.]+)[.](.*)/.exec(fullCollectionName);
    if (match) {
      return that.mocks[match[1]][match[2]] || [];
    }
    return [];
  },

  process: function(socket) {
    that.logger.info('New client connection');
    var processSocketData = function(buf) {
      var header, clientReqMsg;
      header = protocol.fromMsgHeaderBuf(buf);
      switch (header.opCode) {
        case protocol.OP_QUERY:
          clientReqMsg = protocol.fromOpQueryBuf(header, buf);
          if (clientReqMsg.fullCollectionName.match(/\.\$cmd$/)) {
            if (clientReqMsg.query) {
              if (clientReqMsg.query.findandmodify) {
                command = that.doFindAndModify;
                break;
              } else if (clientReqMsg.query.distinct) {
                command = that.doDistinct;
                break;
              } else if (clientReqMsg.query.count) {
                command = that.doCount;
                break;
              }
            }
            command = that.doCmdQuery;
          } else {
            command = that.doFind;
          }
          break;
        case protocol.OP_INSERT:
          clientReqMsg = protocol.fromOpInsertBuf(header, buf);
          command = that.doInsert;
          break;
        case protocol.OP_DELETE:
          clientReqMsg = protocol.fromOpDeleteBuf(header, buf);
          command = that.doDelete;
          break;
        case protocol.OP_UPDATE:
          clientReqMsg = protocol.fromOpUpdateBuf(header, buf);
          command = that.doUpdate;
          break;
        default:
          throw new Error('Operation not supported: ' + header.opCode);
      }
      command.call(that, socket, clientReqMsg);
      if (buf.bytesRead < buf.length) {
        processSocketData(buf.slice(buf.bytesRead));
      }
    };
    socket.on('data', function(socket) {
      try {
        processSocketData(socket);
      } catch (err) {
        that.logger.error('Uncaught exception processing socket data: ', err.stack);
      }
    });
    socket.on('end', function() {
      that.logger.info('Client connection closed');
    });
  },

  doCmdQuery: function(socket, clientReqMsg) {
    var reply, replyBuf;
    that.logger.debug('doCmdQuery');
    if (clientReqMsg.query['ismaster']) {
      reply = {
        documents: { 'ismaster': true, 'ok': true }
      };
      replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
      socket.write(replyBuf);
    } else if (clientReqMsg.query['getlasterror']) {
      reply = {
        documents: { 'ok': true }
      };
      if ('affectedDocuments' in that) {
        reply.documents.n = that.affectedDocuments;
        delete that.affectedDocuments;
      }
      if ('lastError' in that) {
        reply.documents.ok = false;
        reply.documents.err = that.lastError;
        that.logger.debug('Reporting lasterror:', that.lastError);
        delete that.lastError;
      }
      replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
      socket.write(replyBuf);
    } else {
      that.logger.error('clientReqMsg :', clientReqMsg);
      throw new Error(util.format(
        'Query opeation not supported: %s',
        util.inspect(clientReqMsg.query)));
    }
  },

  doFind: cmdFind,
  doCount: cmdCount,
  doInsert: cmdInsert,
  doDelete: cmdDelete,
  doUpdate: cmdUpdate,
  doFindAndModify: cmdFindAndModify,
  doDistinct: cmdDistinct
};

module.exports = that;
