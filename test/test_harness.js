'use strict';

var log4js = require('log4js');
var mongodb = require('mongodb');
var path = require('path');
var util = require('util');

var mongodbFs = require('../lib/server');

function TestHarness(mocks, port, logLevel) {
  this.config = {
    port: port || 27027,
    mocks: mocks,
    log: {level: logLevel || process.env.LOG_LEVEL || 'warn'}
  };
  this.logger = this.createLogger(__filename);
  this.initialized = false;
}

TestHarness.prototype.setUp = function setUp(done) {
  mongodbFs.init(this.config);
  if (this.initialized) {
    return process.nextTick(done);
  }
  this.logger.info('Starting fake server...');
  mongodbFs.start(function(error) {
    if (error) return done(error);

    this.logger.info('Connecting...');
    var databaseName = Object.keys(this.config.mocks)[0];
    var connectUrl = util.format(
      'mongodb://localhost:%d/%s',
      this.config.port,
      databaseName);

    mongodb.MongoClient.connect(
      connectUrl,
      function(error, client) {
        if (error) {
          return mongodbFs.stop(function() { done(error); });
        }
        this.dbClient = client;
        done();
      }.bind(this));
  }.bind(this));
};

TestHarness.prototype.tearDown = function tearDown(done) {
  this.initialized = false;
  this.logger.info('Disconnecting...');
  this.dbClient.close(function() {
    this.logger.info('Stopping fake server...');
    mongodbFs.stop(done);
  }.bind(this));
};

TestHarness.prototype.createLogger = function createLogger(modulePathName) {
  var loggerName = path.basename(modulePathName).replace(/[.]js$/, '');
  var logger = log4js.getLogger(loggerName);
  logger.setLevel(this.config.log.level);
  return logger;
};

var defaultInstance = new TestHarness({fakedb: {}});

TestHarness._defaultInstance = defaultInstance;

TestHarness.mocks = defaultInstance.config.mocks;
TestHarness.port = defaultInstance.config.port;
TestHarness.setUp = defaultInstance.setUp.bind(defaultInstance);
TestHarness.tearDown = defaultInstance.tearDown.bind(defaultInstance);
TestHarness.createLogger = defaultInstance.createLogger.bind(defaultInstance);

module.exports = TestHarness;
