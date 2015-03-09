'use strict';

var _ = require('lodash');

var filter = require('../filter');
var utils = require('../utils');
var BaseCommand = require('./base');
var ElementWrapper = require('../element-wrapper');

var wrapElementForAccess = ElementWrapper.wrapElementForAccess;

function Distinct(server) {
  BaseCommand.call(this, server);
}
BaseCommand.setUpCommand(Distinct, 'distinct');

Distinct.prototype.handle = function(clientReqMsg) {
  this.logger.debug(this.commandName);

  var key = clientReqMsg.query.key;
  var query = clientReqMsg.query.query;
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
