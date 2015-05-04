'use strict';

var chai = require('chai');
var mongoose = require('mongoose');
var path = require('path');
var MongoDbFs = require('../lib/mongodb-fs');

var logConfig = {
  level: process.env.LOG_LEVEL || 'warn',
  category: path.basename(__filename)
};

// Chai uses properties rather than methods for assertions.
/* eslint-disable no-unused-expressions */

describe('Multi-instance support', function() {
  var ModelOne;
  var ModelTwo;
  var databaseOne = {
    collectionOne: [{_id: new mongoose.Types.ObjectId(), key: 'value'}]
  };
  var databaseTwo = {};

  var serverOne = new MongoDbFs({
    mocks: {fakedbone: databaseOne},
    port: 27027,
    log: logConfig
  });
  var serverTwo = new MongoDbFs({
    mocks: {fakedbtwo: databaseTwo},
    port: 27028,
    log: logConfig
  });

  before(function(done) {
    var connectionOne = mongoose.createConnection();
    var connectionTwo = mongoose.createConnection();
    ModelOne = connectionOne.model(
      'ModelOne',
      new mongoose.Schema(
        {any: mongoose.Schema.Types.Mixed},
        {collection: 'collectionOne', cache: false}));
    ModelTwo = connectionTwo.model(
      'ModelTwo',
      new mongoose.Schema(
        {any: mongoose.Schema.Types.Mixed},
        {collection: 'collectionTwo', cache: false}));

    var serverOptions = {server: {poolSize: 1}};

    serverOne.start(function(error) {
      if (error) return done(error);
      serverTwo.start(function(error) {
        if (error) return done(error);
        connectionOne.open(
          'mongodb://localhost:27027/fakedbone',
          serverOptions,
          function(error) {
            if (error) return done(error);
            connectionTwo.open(
              'mongodb://localhost:27028/fakedbtwo',
              serverOptions,
              done);
        });
      });
    });
  });

  after(function(done) {
    mongoose.disconnect(function() {
      serverOne.stop(function() {
        serverTwo.stop(done);
      });
    });
  });

  it('supports multiple server instances', function(done) {
    chai.expect(databaseTwo.collectionTwo).to.not.exist;
    ModelOne.findOne({}, function(error, result) {
      if (error) return done(error);
      ModelTwo.collection.insert(result.toObject(), function(error) {
        if (error) return done(error);
        chai.expect(databaseTwo.collectionTwo).to.have.length(1);
        chai.expect(databaseTwo.collectionTwo[0])
          .to.have.property('key', 'value');
        done();
      });
    });
  });
});