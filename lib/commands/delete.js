'use strict';

var _ = require('lodash');

var filter = require('../filter');

module.exports = function doDelete(socket, clientReqMsg) {
  this.logger.info('doDelete');
  var collection = this.getCollection(clientReqMsg);
  this.affectedDocuments = 0;
  var i = 0;
  var docs = filter.filterItems(collection, clientReqMsg.selector);
  while (i < collection.length) {
    var item = collection[i];
    if (docs.indexOf(item) !== -1) {
      collection.splice(i, 1);
      this.affectedDocuments++;
    } else {
      i++;
    }
  }
};
