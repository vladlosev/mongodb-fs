'use strict';

var _ = require('lodash');
var util = require('util');

var filter = require('../filter');
var utils = require('../utils');
var BaseCommand = require('./base');
var ElementWrapper = require('../element-wrapper');

var wrapElementForAccess = ElementWrapper.wrapElementForAccess;

function Distinct(server) {
  BaseCommand.call(this, server);
  this.logger = this.server.logger;
}
util.inherits(Distinct, BaseCommand);
Distinct.commandName = 'distinct';
Distinct.prototype.commandName = 'distinct';

Distinct.prototype.handle = function(clientReqMsg) {
  var key = clientReqMsg.query.key;
  var query = clientReqMsg.query.query;

  this.logger.info('doDistinct');
  var collection = this.getCollection(clientReqMsg);
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
  return {documents: {ok: true, values: results}};
};

module.exports = Distinct;
