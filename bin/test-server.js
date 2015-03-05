'use strict';

var util = require('util');

var mongodbFs = require('../lib/mongodb-fs');

var port = 27027;
var database = 'fakedb';

var config = {
  port: port,
  mocks: {},
  log: {level: process.env.LOG_LEVEL}
};
config[database] = {};

mongodbFs.init(config);

mongodbFs.start(function(error) {
  if (error) {
    console.error('Failed to start:', error.message);
  } else {
    console.info('Server started, awaiting connections.');
    console.info(util.format(
      "Run 'mongo localhost:%d/%s' to connect.",
      port,
      database)); 
    console.info('Press Ctrl+C to stop.');
  }
});
