'use strict';

var _ = require('lodash');
var childProcess = require('child_process');
var net = require('net');

var log = require('./log');
var Processor = require('./processor');
var utils = require('./utils');

function Server(config) {
  this.server = null;
  this.running = false;
  this.child = null;
  this.config = _.extend({port: 27017, fork: false}, config);

  var logConfig = this.config.log;

  if (logConfig) {
    if (this.config.fork && logConfig.logger) {
      // Cannot pass active object into forked process directly, just use its
      // log level if available.
      logConfig.logLevel = logConfig.logger.level || logConfig.logLevel;
      delete logConfig.logger;
    } else if (logConfig.logger && !logConfig.logger.trace) {
      // Our code logs using the `trace` method.  If it's not present on the
      // the provided logger, extend it with the method stubbed.
      logConfig.logger = Object.create(
        logConfig.logger,
        {trace: {value: function() {}}});
    }
  }
  log.init(this.config.log);

  this.logger = log.getLogger('main');
  this.processor = new Processor(this.config.mocks);
}

Server.prototype.start = function start(callback) {
  callback = utils.safeCallback(callback);

  if (this.config.fork) {
    this.child = childProcess.fork(__filename);
    this.child.send({ action: 'start', config: this.config });
    this.child.on('message', function(data) {
      if (data.state === 'started') {
        callback(data.err);
      }
    });
    return;
  }
  this.logger.info('Starting server');
  this.server = net.createServer(this.processor.process.bind(this.processor));
  this.server.listen(this.config.port, function(error) {
    if (!error) {
      this.logger.info('Server ready, listening to port ', this.config.port);
      this.running = true;
    }
    callback(error);
  }.bind(this));
};

Server.prototype.stop = function stop(callback) {
  callback = utils.safeCallback(callback);

  if (this.child) {
    this.child.send({ action: 'stop' });
    this.child.on('message', function(data) {
      if (data.state === 'stopped') {
        callback(data.err);
      }
      this.child = null;
    }.bind(this));
    return;
  }
  this.logger.info('Stopping server');
  this.server.close(function() {
    this.logger.info('Server stopped');
    this.running = false;
    callback();
  }.bind(this));
};

Server.prototype.isRunning = function isRunning() {
  return this.running || this.child;
};

Server.init = function init(config) {
  Server._defaultInstance = new Server(config);
  Server.start = Server._defaultInstance.start.bind(Server._defaultInstance);
  Server.stop = Server._defaultInstance.stop.bind(Server._defaultInstance);
  Server.isRunning = Server._defaultInstance.isRunning.bind(
    Server._defaultInstance);
};

if (module.parent) {
  module.exports = Server;
} else {
  var server;
  process.on('message', function handleParentMessage(data) {
    if (data.action === 'start') {
      data.config.fork = false;
      server = new Server(data.config);
      server.start(function(err) {
        process.send({state: 'started', err: err});
      });
    } else if (data.action === 'stop') {
      server.stop(function(err) {
        process.send({state: 'stopped', err: err});
        process.removeListener('message', handleParentMessage);
      });
    }
  });
}
