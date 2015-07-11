'use strict';

var mongoose = require('mongoose');
var util     = require('util');

var mongodbFs = require('../lib/server');

/* eslint-disable no-console */

mongoose.model('MyModel', {
  a: String,
  b: String
});

mongodbFs.init({
  port: 27027,
  mocks: {
    fakedb: {
      mymodels: []
    }
  }
});

function reportError(when, error) {
  console.error(util.format('Error %s: %s\n', when, error));
}

mongodbFs.start(function(err) {
  if (err) return reportError('starting server', err);
  var connection = mongoose.createConnection(
    'mongodb://localhost:27027/fakedb',
    {server: {poolSize: 1}},
    function(err) {
      if (err) return reportError('opening connection', err);
      var MyModel = connection.model('MyModel');
      var myModel = new MyModel({
        a: 'avalue',
        b: 'bvalue'
      });
      myModel.save(function(err) {
        if (err) return reportError('saving document', err);
        MyModel.find(function(err, myModels) {
          if (err) return reportError('loading document', err);
          console.info('myModels :', myModels);
          connection.close(function(err) { // clean death
            if (err) return reportError('closing connection', err);
            mongodbFs.stop(function(err) {
              if (err) return reportError('stopping server', err);
              console.info('bye!');
            });
          });
        });
      });
    });
});
