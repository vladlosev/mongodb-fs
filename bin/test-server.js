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
config.mocks[database] = {};

mongodbFs.init(config);

mongodbFs.start(function(error) {
  if (error) {
    process.stderr.write('Failed to start:' + error.message + '\n');
  } else {
    process.stdout.write('Server started, awaiting connections.\n');
    process.stdout.write(util.format(
      "Run 'mongo localhost:%d/%s' to connect.\n",
      port,
      database));
    process.stdout.write('Press Ctrl+C to stop.\n');
  }
});
