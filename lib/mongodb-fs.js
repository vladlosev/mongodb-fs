var _ = require('lodash');
var childProcess = require('child_process');
var net = require('net');

var log = require('./log');
var processor = require('./processor');
var utils = require('./utils');

var that = {
  config: {
    port: 27017,
    fork: false
  },
  server: null,
  running: false,
  child: null,
  logger: null,

  init: function(config) {
    _.extend(that.config, config);
    log.init(that.config.log);
    that.logger = log.getLogger();
    processor.init(that.config.mocks);
  },

  start: function(callback) {
    callback = utils.safeCallback(callback);

    if (that.config.fork) {
      that.child = childProcess.fork(__filename);
      that.child.send({ action: 'start', config: that.config });
      that.child.on('message', function(data) {
        if (data.state === 'started') {
          callback(data.err);
        }
      });
      return;
    }
    that.logger.trace('starting server');
    that.server = net.createServer(processor.process);
    that.server.listen(that.config.port, function(err) {
      that.logger.trace('server ready, listening to port ', that.config.port);
      that.running = true;
      callback(err);
    });
  },

  stop: function(callback) {
    callback = utils.safeCallback(callback);
    that.logger.trace('closing server');

    if (that.child) {
      that.child.send({ action: 'stop' });
      that.child.on('message', function(data) {
        if (data.state === 'stopped') {
          callback(data.err);
        }
        that.child = null;
      });
      return;
    }
    that.server.close(function() {
      that.logger.trace('server closed');
      that.running = false;
      callback();
    });
  },

  isRunning: function() {
    return that.running || that.child;
  }
};

if (module.parent) {
  module.exports = that;
} else {
  process.on('message', function(data) {
    if (data.action === 'start') {
      data.config.fork = false;
      that.init(data.config);
      that.start(function(err) {
        process.send({state: 'started', err: err});
      });
    } else if (data.action === 'stop') {
      that.stop(function(err) {
        process.send({state: 'stopped', err: err});
        process.exit(0);
      });
    }
  });
}
