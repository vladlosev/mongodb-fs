'use strict';

var chai = require('chai');
var mongodb = require('mongodb');
var path = require('path');
var MongoDbFs = require('../lib/server');

var logConfig = {
  level: process.env.LOG_LEVEL || 'warn',
  category: path.basename(__filename)
};

// Chai uses properties rather than methods for assertions.
/* eslint-disable no-unused-expressions */

describe('Multi-instance support', function() {
  var databaseOne = {
    collectionOne: [{_id: new mongodb.ObjectId(), key: 'value'}]
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

  var clientOne;
  var clientTwo;
  var collectionOne;
  var collectionTwo;

  before(function(done) {
    serverOne.start(function(error) {
      if (error) return done(error);
      serverTwo.start(function(error) {
        if (error) {
          return serverOne.stop(function() { done(error); });
        }

        mongodb.MongoClient.connect(
          'mongodb://localhost:27027/fakedbone',
          function(error, client) {
            if (error) {
              return serverTwo.stop(function() {
                serverOne.stop(function() { done(error); });
              });
            }
            clientOne = client;
            collectionOne = client.db('fakedbone').collection('collectionOne');
            mongodb.MongoClient.connect(
              'mongodb://localhost:27028/fakedbtwo',
              function(error, client) {
                if (error) {
                  return clientOne.close(function() {
                    serverTwo.stop(function() {
                      serverOne.stop(function() { done(error); });
                    });
                  });
                }
                clientTwo = client;
                collectionTwo = client.db('fakedbtwo').collection('collectionTwo');
                done();
            });
        });
      });
    });
  });

  after(function(done) {
    clientOne.close(function() {
      clientTwo.close(function() {
        serverOne.stop(function() {
          serverTwo.stop(done);
        });
      });
    });
  });

  it('supports multiple server instances', function(done) {
    chai.expect(databaseTwo.collectionTwo).to.not.exist;
    collectionOne.findOne({}, function(error, result) {
      if (error) return done(error);
      collectionTwo.insert(result, function(error) {
        if (error) return done(error);
        chai.expect(databaseTwo.collectionTwo).to.have.length(1);
        chai.expect(databaseTwo.collectionTwo[0]).
          to.have.property('key', 'value');
        done();
      });
    });
  });
});
