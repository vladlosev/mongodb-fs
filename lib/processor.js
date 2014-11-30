var _ = require('lodash');
var util = require('util')
  , net = require('net')
  , helper = require('./helper')
  , filter = require('./filter')
  , logger;
var ObjectID = require('bson').ObjectID;

function getCollection(clientReqMsg) {
  return eval('that.mocks.' + clientReqMsg.fullCollectionName);
}

// Determines whethe value is an atomic value (i.e., not an object or an
// array).
function isAtomic(value) {
  if (typeof value === 'number') {
    return true;
  } else if (typeof value === 'string') {
    return true;
  } else if (value instanceof Date) {
    return true;
  } else if (value instanceof ObjectID) {
    return true;
  } else if (value === null) {
    return true;
  }
  return false;
}

// Determines whether a value is an operator (i.e., starts with a $ sign).
// Assumes the value is a string.
function isOperator(value) {
  return value.length > 0 && value[0] === '$';
}

// Assuming source is a MongoDB selector document, retrieves values from
// equality comparisons (e,g, {a: 1}) and set them in destination.  If source
// contains the $and operator, recurses over its arguments. E.g.,
//   var doc = {};
//   copyEqualityValues(doc, {a: 1, $and: [{b: 2}, {$and: [c: 5, 'd.e': 6]}]});
// will make doc equal {a: 1, b: 2, c: 5, d: {e: 6}}.
//
function copyEqualityValues(destination, source) {
  _.forOwn(source, function(value, key) {
    if (!isOperator(key)) {
      if (isAtomic(value)) {
        if (!isOperator(value)) {
          wrapElementForAccess(destination, key).setValue(value);
        }
      } else {
        copyEqualityValues(destination, value);
      }
    } else if (key === '$and') {
      _.forEach(value, function(elem) {
        copyEqualityValues(destination, elem);
      });
    }
  });
}

function InputDataError(message) {
  this.message = message;
}
InputDataError.prototype = new Error();

// Wraps an element of doc accessible via path in the dot notation
// (http://docs.mongodb.org/manual/core/document/#document-dot-notation) in a
// handler object that allows getting or setting the value of the element.  For
// example, wrapElementForAccess({a: {b: 1}}, 'a.b').getValue() will return 1.
// When getting a value, if leaf or intermediate children do not exist, the
// result will be undefined. For example, wrapElementForAccess({a}, 'b') will
// return undefined.  When setting a value which parent does not exists, the
// parent will be created. For example, after running this code:
//   var doc = {a: 1};
//   wrapElementForAccess(doc, 'b.c').setValue(5);
// doc will be {a: 1, b: {c: 5}}.
//
function wrapElementForAccess(doc, path) {
  function newDocTraversalError(selector) {
    return new InputDataError(util.format(
      'cannot use the part (%s of %s) to traverse the element (%s)',
      selector, path, util.format(doc)));
  };
  var wrapElement = function(parentElem, selector) {
    var doc = parentElem.getValue();

    return {
      getValue: function() {
        return _.isUndefined(doc) ? doc : doc[selector];
      },
      setValue: function(value) {
        if (_.isArray(doc)) {
          if (selector.match(/^[0-9]$/)) {
            selector = parseInt(selector);
            while (doc.length <= selector) {
              doc.push(null);
            }
          } else {
            throw newDocTraversalError(selector);
          }
        } else if (isAtomic(doc) && !_.isUndefined(doc)) {
          throw newDocTraversalError(selector);
        }
        if (_.isUndefined(doc)) {
          doc = {};
          parentElem.setValue(doc);
        }
        doc[selector] = value;
      },
      deleteValue: function() {
        if (_.isArray(doc) && selector.match(/^[0-9]$/)) {
          selector = parseInt(selector);
          if (selector < doc.length) {
            doc[selector] = null;
          }
        } else if (_.isPlainObject(doc)) {
          delete doc[selector];
        }
      }
    };
  }

  var wrapElementAtPath = function(parentElem, selectors) {
    if (selectors.length === 0) {
      return parentElem;
    }
    var currentElem = wrapElement(parentElem, selectors[0]);
    if (currentElem.error) {
      return currentElem;
    }
    return wrapElementAtPath(currentElem, selectors.slice(1));
  };

  return wrapElementAtPath({getValue: function() { return doc; }}, path.split('.'));
}

function arrayPull(arr, value) {
  var valuesEqual = function(a, b) {
    if (a instanceof ObjectID) {
      return a.equals(b);
    } else if (b instanceof ObjectID) {
      return b.equals(a);
    }
    return _.isEqual(a, b);
  }
  var i = 0;
  while (i < arr.length) {
    if (valuesEqual(value, arr[i])) {
      arr.splice(i, 1);
    } else {
      ++i;
    }
  }
}

that = {
  mocks: null,
  init: function (mocks) {
    logger = require('../lib/log').getLogger();
    that.mocks = mocks;
    filter.init();
  },
  process: function (socket) {
    logger.trace('new socket');
    var processSocketData = function (buf) {
      var header, clientReqMsg;
      header = helper.fromMsgHeaderBuf(buf);
      switch (header.opCode) {
        case helper.OP_QUERY:
          clientReqMsg = helper.fromOpQueryBuf(header, buf);
          if (clientReqMsg.fullCollectionName.match(/\.\$cmd$/)) {
            that.doCmdQuery(socket, clientReqMsg);
          } else {
            that.doQuery(socket, clientReqMsg);
          }
          break;
        case helper.OP_INSERT:
          clientReqMsg = helper.fromOpInsertBuf(header, buf);
          that.doInsert(socket, clientReqMsg);
          break;
        case helper.OP_DELETE:
          clientReqMsg = helper.fromOpDeleteBuf(header, buf);
          that.doDelete(socket, clientReqMsg);
          break;
        case helper.OP_UPDATE:
          clientReqMsg = helper.fromOpUpdateBuf(header, buf);
          that.doUpdate(socket, clientReqMsg);
          break;
        default:
          throw new Error('not supported');
      }
      if (buf.bytesRead < buf.length) {
        processSocketData(buf.slice(buf.bytesRead));
      }
    };
    socket.on('data', function(socket) {
      try {
        processSocketData(socket);
      } catch (err) {
        logger.error('Uncaught exception processing socket data: ', err);
      }
    });
    socket.on('end', function () {
      logger.trace('socket disconnect');
    });
  },
  doCmdQuery: function (socket, clientReqMsg) {
    var reply, replyBuf;
    logger.trace('doCmdQuery');
    if (clientReqMsg.query['ismaster']) {
      reply = {
        documents: { 'ismaster': true, 'ok': true }
      };
      replyBuf = helper.toOpReplyBuf(clientReqMsg, reply);
      socket.write(replyBuf);
    } else if (clientReqMsg.query['getlasterror']) {
      reply = {
        documents: { 'ok': true }
      };
      if ('affectedDocuments' in that) {
        reply.documents.n = that.affectedDocuments;
        delete that.affectedDocuments;
      }
      if ('lastError' in that) {
        reply.documents.ok = false;
        reply.documents.err = that.lastError;
        logger.trace('Reporting lasterror:', that.lastError);
        delete that.lastError;
      }
      replyBuf = helper.toOpReplyBuf(clientReqMsg, reply);
      socket.write(replyBuf);
    } else if (clientReqMsg.query['count']) {
      that.doCount(socket, clientReqMsg);
    } else {
      logger.error('clientReqMsg :', clientReqMsg);
      throw new Error('not supported');
    }
  },
  doCount: function (socket, clientReqMsg) {
    logger.trace('doCount');
    var dbName = clientReqMsg.fullCollectionName.replace('.$cmd', '');
    var collection = that.mocks[dbName][clientReqMsg.query.count];
    var query = clientReqMsg.query.query;
    var docs;
    if (query && !helper.isEmpty(query)) {
      docs = filter.filterItems(collection, query);
    } else {
      docs = collection;
    }
    var reply = {documents: {ok: true, n: docs.length}};
    var replyBuf = helper.toOpReplyBuf(clientReqMsg, reply);
    socket.write(replyBuf);
  },
  doQuery: function (socket, clientReqMsg) {
    var collection, docs, replyBuf;
    logger.trace('doQuery');
    collection = getCollection(clientReqMsg);
    var query = clientReqMsg.query;
    if ('$query' in query) {
      query = query.$query;
    }
    try {
      if (query && !helper.isEmpty(query)) {
        docs = filter.filterItems(collection, query);
      } else {
        docs = collection || [];
      }
    } catch (err) {
      if (err.message.match(/^BadValue /)) {
        var reply = {documents: [{'$err': err.message }]};
        var replyBuf = helper.toOpReplyBuf(clientReqMsg, reply);
        socket.write(replyBuf);
        return;
      } else {
        throw err;
      }
    }
    var skip = clientReqMsg.numberToSkip;
    var limit = clientReqMsg.numberToReturn || docs.length;
    if (limit < 0) {
      limit = -limit;
    }
    docs = docs.slice(skip, skip + limit);

    if (clientReqMsg.returnFieldSelector) {
      docs.forEach(function (document) {
        for (var key in document) {
          if (!clientReqMsg.returnFieldSelector[key]) {
            delete document[key];
          }
        }
      });
    }
    delete that.affectedDocuments;
    replyBuf = helper.toOpReplyBuf(clientReqMsg, { documents: docs });
    socket.write(replyBuf);
  },
  doInsert: function (socket, clientReqMsg) {
    var collection;
    logger.trace('doInsert');
    collection = getCollection(clientReqMsg);
    that.affectedDocuments = 0;
    clientReqMsg.documents.forEach(function (doc) {
      collection.push(doc);
      that.affectedDocuments++;
    });
  },
  doDelete: function (socket, clientReqMsg) {
    var collection, i, item, key, match, docs;
    logger.trace('doDelete');
    collection = getCollection(clientReqMsg);
    that.affectedDocuments = 0;
    i = 0;
    if (clientReqMsg.selector && !helper.isEmpty(clientReqMsg.selector)) {
      docs = filter.filterItems(collection, clientReqMsg.selector);
    } else {
      docs = collection;
    }
    while (i < collection.length) {
      item = collection[i];
      if (docs.indexOf(item) !== -1) {
        collection.splice(i, 1);
        that.affectedDocuments++;
      } else {
        i++;
      }
    }
  },
  doUpdate: function (socket, clientReqMsg) {
    var collection, docs, updateKey, propKey;
    logger.trace('doUpdate');
    collection = getCollection(clientReqMsg);
    if (clientReqMsg.selector && !helper.isEmpty(clientReqMsg.selector)) {
      docs = filter.filterItems(collection, clientReqMsg.selector);
    } else {
      docs = collection;
    }
    that.affectedDocuments = 0;
    var updateContainsOperators = _.any(
      clientReqMsg.update,
      function(value, key) { return isOperator(key); });
    var updateContainsOnlyOperators = _.every(
      clientReqMsg.update,
      function(value, key) { return isOperator(key); });

    if (clientReqMsg.flags.multiUpdate && !updateContainsOnlyOperators) {
      that.lastError = 'multi update only works with $ operators';
      return;
    }

    var literalSubfield = _.findKey(
      clientReqMsg.update,
      function(value, key) {
        return !isOperator(key) && _.contains(key, '.');
      });
    if (literalSubfield) {
      that.lastError = util.format(
        "can't have . in field names [%s]",
        literalSubfield);
      return;
    }
    var upsertedDoc;
    if (docs.length === 0 && clientReqMsg.flags.upsert) {
      upsertedDoc = {};
      if (updateContainsOperators) {
        copyEqualityValues(upsertedDoc, clientReqMsg.selector);
      }
      collection.push(upsertedDoc);
      docs = [upsertedDoc];
      // Now allow the update loop to update the newly inserted element.
    }
    try {
      _.forEach(docs, function (doc, index) {
        if (!clientReqMsg.flags.multiUpdate && index > 0) {
          // multi is off and we have already updated one document.
          return false;  // Exit the loop.
        } else if (that.lastError) {
          return false;
        }
        that.affectedDocuments++;
        var value;
        // The document contains no operators, so its contents must be replaced
        // entirely (see
        // http://docs.mongodb.org/manual/reference/method/db.collection.update/#replace-a-document-entirely).
        // Remove here fields that have no corresponding fields in the update
        // document.
        if (!updateContainsOperators) {
          _.forOwn(doc, function(value, key) {
            if (key !== '_id' && !(key in clientReqMsg.update)) {
              delete doc[key];
            }
          });
        }
        for (updateKey in clientReqMsg.update) {
          if (updateKey === '$setOnInsert' && !upsertedDoc) {
            continue;
          }
          if (updateKey === '$set' ||
              updateKey === '$unset' ||
              updateKey === '$setOnInsert' ||
              updateKey === '$inc' ||
              updateKey === '$pull') {
            for (propKey in clientReqMsg.update[updateKey]) {
              var property = wrapElementForAccess(doc, propKey);
              value = clientReqMsg.update[updateKey][propKey];
              if (updateKey == '$inc') {
                property.setValue(property.getValue() + value);
              } else if (updateKey === '$unset') {
                property.deleteValue();
              } else if (updateKey === '$pull') {
                // TODO(vladlosev): Support queries in $pull,
                // e.g. db.collection.update({name: 'joe'}, {$pull: {scores: {$lt : 50}}})
                var arr = property.getValue();
                if (_.isUndefined(arr)) continue;
                if (!_.isArray(arr)) {
                  throw new InputDataError(
                    'Cannot apply $pull to a non-array value');
                }
                arrayPull(arr, value);
              } else {
                property.setValue(value);
              }
            }
          } else if (updateKey === '$pushAll') {
            for (propKey in clientReqMsg.update[updateKey]) {
              var property = wrapElementForAccess(doc, propKey);
              var arr = property.getValue();
              if (_.isUndefined(arr)) {
                arr = [];
                property.setValue(arr);
              } else if (_.isArray(arr)) {
                var values = clientReqMsg.update[updateKey][propKey];
                values.forEach(function(element) { arr.push(element); });
              } else {
                throw new InputDataError(
                  "The field '" + propKey + "' must be an array.");
              }
            }
          } else if (isOperator(updateKey)) {
            throw new Error('update value "' + updateKey + '" not supported');
          } else {
            // Literal value to set.
            var property = wrapElementForAccess(doc, updateKey);
            property.setValue(clientReqMsg.update[updateKey]);
          }
        }
      });
      if (upsertedDoc && !('_id' in upsertedDoc)) {
        upsertedDoc._id = new ObjectID();
      }
    } catch (error) {
      if (error instanceof InputDataError) {
        that.lastError = error.message;
      } else {
        throw e;
      }
    }
  }
};

module.exports = that;
