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
  if (clientReqMsg.query.ismaster) {
    return {documents: {ismaster: true, ok: true}};
  } else if (clientReqMsg.query.getlasterror) {
    return this.server.lastReply;
  } else {
    this.logger.error('clientReqMsg :', clientReqMsg);
    throw new Error(util.format(
      'Query opeation not supported: %s',
      util.inspect(clientReqMsg.query)));
  }
};

module.exports = QueryCommand;