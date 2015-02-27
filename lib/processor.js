'use strict';

var _ = require('lodash');
var util = require('util');
var net = require('net');

var protocol = require('./protocol');
var filter = require('./filter');
var projection = require('./projection');
var update = require('./update');
var utils = require('./utils');

var Count = require('./commands/count');
var Delete = require('./commands/delete');
var Distinct = require('./commands/distinct');
var Find = require('./commands/find');
var FindAndModify = require('./commands/find_and_modify');
var Insert = require('./commands/insert');
var QueryCommand = require('./commands/query_command');
var Update = require('./commands/update');

var that = {
  mocks: null,

  init: function(mocks) {
    that.logger = require('../lib/log').getLogger('processor');
    that.mocks = mocks;
    filter.init();
  },

  getCollection: function(databaseName, collectionName) {
    return that.mocks[databaseName][collectionName] || [];
  },

  ensureCollection: function(databaseName, collectionName, collection) {
    var database = this.mocks[databaseName];
    if (!database[collectionName]) {
      database[collectionName] = collection;
    } else if (database[collectionName] !== collection) {
      throw new Error(util.format(
        'Attempt to replace collection %s.%s',
        databaseName,
        collectionName));
    }
  },

  process: function(socket) {
    that.logger.info('New client connection');
    var processSocketData = function(buf) {
      var header = protocol.fromMsgHeaderBuf(buf);
      var clientReqMsg;
      var Command;
      switch (header.opCode) {
        case protocol.OP_QUERY:
          clientReqMsg = protocol.fromOpQueryBuf(header, buf);
          if (clientReqMsg.fullCollectionName.match(/\.\$cmd$/)) {
            if (clientReqMsg.query) {
              if (clientReqMsg.query.findandmodify) {
                Command = FindAndModify;
                break;
              } else if (clientReqMsg.query.distinct) {
                Command = Distinct;
                break;
              } else if (clientReqMsg.query.count) {
                Command = Count;
                break;
              }
            }
            Command = QueryCommand;
          } else {
            Command = Find;
          }
          break;
        case protocol.OP_INSERT:
          clientReqMsg = protocol.fromOpInsertBuf(header, buf);
          Command = Insert;
          break;
        case protocol.OP_DELETE:
          clientReqMsg = protocol.fromOpDeleteBuf(header, buf);
          Command = Delete;
          break;
        case protocol.OP_UPDATE:
          clientReqMsg = protocol.fromOpUpdateBuf(header, buf);
          Command = Update;
          break;
        default:
          throw new Error('Operation not supported: ' + header.opCode);
      }
      var command = new Command(that);
      var reply = command.handle(clientReqMsg);

      if (header.opCode === protocol.OP_QUERY) {
        var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
        socket.write(replyBuf);
      } else {
        that.lastReply = reply;
      }
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
  }
};

module.exports = that;
