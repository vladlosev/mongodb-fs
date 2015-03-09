'use strict';

var _ = require('lodash');
var util = require('util');

var BaseCommand = require('./base');

function CreateIndexes(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(CreateIndexes, 'createIndexes');

function getIndexName(index) {
  if (index.name) {
    return index.name;
  }
  return _.map(index.key, function(value, key) {
    return util.format('%s_%s', key, value);
  }).join('_');
}

CreateIndexes.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var collection = this.getCollection(clientReqMsg);
  var databaseAndCollectionName = this.getDbAndCollectionName(clientReqMsg);
  var databaseName = databaseAndCollectionName.database;
  var collectionName = databaseAndCollectionName.collection;
  var fullCollectionName = util.format('%s.%s', databaseName, collectionName);
  var collectionExisted = this.server.collectionExists(
    databaseName,
    collectionName);
  var indexCollection = this.server.getCollection(
    databaseName,
    'system.indexes');
  var indicesForCollection = _.filter(
    indexCollection,
    'ns',
    fullCollectionName);
  var indicesBefore = indicesForCollection.length;
  var errorRaised;

  if (indicesForCollection.length === 0) {
    var idIndex = {v: 1, key: {_id: 1}, name: '_id_', ns: fullCollectionName};
    indexCollection.push(idIndex);
    indicesForCollection.push(idIndex);
    indicesBefore = 1;
  }

  _.forEach(clientReqMsg.query.indexes, function(index) {
    var keys = index.key;
    var errorMessage;

    // An insert operation started via Node MongoDB driver will add _id field.
    // We just get rid of them.
    delete index._id;

    if (_.contains(keys, 'hashed') && Object.keys(keys).length > 1) {
      errorMessage =
        'exception: Currently only single field hashed index supported.';
      errorRaised = {
        createdCollectionAutomatically: false,
        numIndexesBefore: indicesForCollection.length,
        errmsg: errorMessage,
        code: 16763,
        ok: 0
      };
      return false;  // Terminate the loop.
    }
    var indexName = getIndexName(index);
    var existingIndex = _.find(indicesForCollection, function(existingIndex) {
      return _.isEqual(index.key, existingIndex.key);
    });
    var indexToInsert = _.defaults(
      {},
      index,
      {v: 1, name: indexName, ns: fullCollectionName});
    if (existingIndex) {
      // Mongoose sets {unique: false} unless {unique: true} is specified; we
      // have to account for that when comparing.
      var optionsMatch = _.isEqual(
        _.defaults({}, indexToInsert, {unique: false}),
        _.defaults({}, existingIndex, {unique: false}));
      //console.log('Options', optionsMatch ? 'do' : 'do not', 'match for', index, 'vs.', existingIndex);
      if (!optionsMatch) {
        errorMessage = util.format(
          'Index with pattern: %s already exists with different options',
          util.inspect(index.key, {depth: null}));
        errorRaised = {ok: 0, errmsg: errorMessage, code: 85};
        return false;  // Terminate the loop.
      }
      return true;  // Index already exists; continue to the next one.
    }
    var indexWithSameName = _.find(
      indicesForCollection,
      function(existingIndex) { return indexName === getIndexName(existingIndex); });
    if (indexWithSameName) {
      errorMessage = util.format(
        'Trying to create an index with same name %s ' +
        'with different key spec %s vs existing spec %s',
        indexName,
        util.inspect(keys, {depth: null}),
        util.inspect(indexWithSameName.key, {depth: null}));
      errorRaised = {ok: 0, errmsg: errorMessage, code: 86};
      return false;  // Terminate the loop.
    }
    indexCollection.push(indexToInsert);
    indicesForCollection.push(indexToInsert);
  });
  if (errorRaised) {
    return {documents: errorRaised};
  }

  this.ensureCollection(clientReqMsg, collection);
  this.server.ensureCollection(databaseName, 'system.indexes', indexCollection);

  if (indicesForCollection.length === indicesBefore) {
    return {
      documents: {
        numIndexesBefore: indicesBefore,
        note: 'all indexes already exist',
        ok: 1
      }
    };
  } else {
    return {
      documents: {
        createdCollectionAutomatically: !collectionExisted,
        numIndexesBefore: indicesBefore,
        numIndexesAfter: indicesForCollection.length,
        ok: 1
      }
    };
  }
};

module.exports = CreateIndexes;
