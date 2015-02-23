'use strict';

var _ = require('lodash');

var filter = require('../filter');
var protocol = require('../protocol');
var utils = require('../utils');
var ElementWrapper = require('../element-wrapper');

var wrapElementForAccess = ElementWrapper.wrapElementForAccess;

module.exports = function doDistinct(socket, clientReqMsg) {
  var key = clientReqMsg.query.key;
  var query = clientReqMsg.query.query;

  this.logger.info('doDistinct');
  var dbName = clientReqMsg.fullCollectionName.replace('.$cmd', '');
  var collectionName = clientReqMsg.query.distinct;
  var collection = this.getCollection(dbName + '.' + collectionName);
  var docs = filter.filterItems(collection, query);

  var results = [];
  if (_.isString(key)) {
    var elements = _(docs).map(function(doc) {
      return wrapElementForAccess(doc, key).getValue();
    }).reject(_.isUndefined).value();

    _.forEach(elements, function(element) {
      var found = _.find(results, function(value) {
        return utils.isEqual(element, value);
      });
      if (!found) {
        results.push(element);
      }
    });
  }
  var reply = {documents: {ok: true, values: results}};
  var replyBuf = protocol.toOpReplyBuf(clientReqMsg, reply);
  socket.write(replyBuf);
};
