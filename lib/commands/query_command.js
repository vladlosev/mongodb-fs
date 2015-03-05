'use strict';

var util = require('util');

var BaseCommand = require('./base');

function QueryCommand(server) {
  BaseCommand.call(this, server);
}
// QueryCommand handles more than a single command, so commandName and
// prototype.commandName properties are not applicable.
BaseCommand.setUpCommand(QueryCommand, undefined);

QueryCommand.prototype.handle = function(clientReqMsg) {
  this.logger.debug('doCmdQuery');
  if (clientReqMsg.query.isMaster) {
    return {documents: {ismaster: true, ok: true}};
  } else if (clientReqMsg.query.getlasterror) {
    return this.server.lastReply;
  } else if (clientReqMsg.query.whatsmyuri) {
    // This is an internal MongDB command used by the mongo command line
    // client.
    var socket = this.server.socket;
    var format = socket.remoteFamily === 'IPv6' ? '[%s]:%d' : '%s:%d';
    var hostPort = util.format(format, socket.remoteAddress, socket.remotePort);

    return {documents: {you: hostPort, ok: 1}};
  } else if (clientReqMsg.query.getLog) {
    // This is an internal MongDB command used by the mongo command line
    // client.
    return {documents: {totalLinesWritten: 0, log: [], ok: 1}};
  } else if (clientReqMsg.query.replSetGetStatus) {
    // This is an internal MongDB command used by the mongo command line
    // client.
    return {documents: {ok: 0, errmsg: 'not supported'}};
  } else {
    this.logger.error('clientReqMsg :', clientReqMsg);
    throw new Error(util.format(
      'Query opeation not supported: %s',
      util.inspect(clientReqMsg.query)));
  }
};

module.exports = QueryCommand;
