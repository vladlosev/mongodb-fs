'use strict';

var _ = require('lodash');
var events = require('events');
var util = require('util');

var protocol = require('./protocol');
var log = require('./log');

var Aggregate = require('./commands/aggregate');
var CreateIndexes = require('./commands/create_indexes');
var Count = require('./commands/count');
var Delete = require('./commands/delete');
var Distinct = require('./commands/distinct');
var Find = require('./commands/find');
var FindAndModify = require('./commands/find_and_modify');
var Insert = require('./commands/insert');
var QueryCommand = require('./commands/query_command');
var Update = require('./commands/update');

// Retirns a copy of clientReqMsg edited for debug output.
function debugFormat(clientReqMsg) {
  var result = _.cloneDeep(clientReqMsg);
  var format;
  switch (result.header.opCode) {
    case protocol.OP_QUERY:
      format = 'OP_QUERY (%d)';
      break;
    case protocol.OP_INSERT:
      format = 'OP_INSERT (%d)';
      break;
    case protocol.OP_DELETE:
      format = 'OP_DELETE (%d)';
      break;
    case protocol.OP_UPDATE:
      format = 'OP_UPDATE (%d)';
      break;
    default:
      format = '%d';
  }
  result.opCode = util.format(format, result.header.opCode);
  delete result.header;
  result.flags = _.pick(result.flags, _.identity);
  if (Object.keys(result.flags).length === 0) {
    delete result.flags;
  }
  if (!result.numberToSkip) {
    delete result.numberToSkip;
  }
  if (!result.returnFieldSelector) {
    delete result.returnFieldSelector;
  }
  return util.inspect(result, {depth: 5});
}

function ServerConnection(mocks, socket, logger) {
  this._mocks = mocks;
  this.socket = socket;
  this.logger = logger;

  socket.on('data', function(buffer) {
    try {
      this.processSocketData(buffer);
    } catch (error) {
      this.logger.error(
        'Uncaught exception processing socket data: ',
        error.stack);
    }
  }.bind(this));

  socket.on('end', function() {
    this.logger.info('Client connection closed');
    this.emit('end');
  }.bind(this));
}
util.inherits(ServerConnection, events.EventEmitter);

ServerConnection.prototype.getCollection = function getCollection(
  databaseName,
  collectionName) {
  return this._mocks[databaseName][collectionName] || [];
};

ServerConnection.prototype.collectionExists = function collectionExists(
    databaseName,
    collectionName) {
  return !!(this._mocks[databaseName] || {})[collectionName];
};

ServerConnection.prototype.ensureCollection = function ensureCollection(
  databaseName,
  collectionName,
  collection) {
  var database = this._mocks[databaseName];
  if (!database[collectionName]) {
    database[collectionName] = collection;
  } else if (database[collectionName] !== collection) {
    throw new Error(util.format(
      'Attempt to replace collection %s.%s',
      databaseName,
      collectionName));
  }
};

ServerConnection.prototype.processSocketData = function processSocketData(buf) {
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
          } else if (clientReqMsg.query.createIndexes) {
            Command = CreateIndexes;
            break;
          } else if (clientReqMsg.query.aggregate) {
            Command = Aggregate;
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
  var command = new Command(this);
  this.logger.debug('Received client request:', debugFormat(clientReqMsg));
  this.logger.info('Running command', command.commandName);
  var reply = command.handle(clientReqMsg);
  this.logger.trace('Command returned:', reply);

  if (header.opCode === protocol.OP_QUERY) {
    var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
    this.socket.write(replyBuf);
  } else {
    this.lastReply = reply;
  }
  if (buf.bytesRead < buf.length) {
    this.processSocketData(buf.slice(buf.bytesRead));
  }
};

function Processor(mocks) {
  this._logger = log.getLogger('processor');
  this._mocks = mocks;
  this._connections = [];
}

Processor.prototype.process = function onNewConnection(socket) {
  var serverConnection = new ServerConnection(
    this._mocks,
    socket,
    this._logger);

  serverConnection.on('end', function endEventHandler() {
    serverConnection.removeListener('end', endEventHandler);
    _.pull(this._connections, serverConnection);
  });
  this._connections.push(serverConnection);
};

module.exports = Processor;
