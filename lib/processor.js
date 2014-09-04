var _ = require('lodash');
var util = require('util')
  , net = require('net')
  , helper = require('./helper')
  , filter = require('./filter')
  , logger;

function getCollection(clientReqMsg) {
  return eval('that.mocks.' + clientReqMsg.fullCollectionName);
}

function formatValue(value) {
  if (typeof value === 'string') {
    return util.format('"%s"', value);
  } else if (value instanceof Date) {
    return util.format('new Date("%s")', value);
  } else {
    return  util.format(value);
  }
}

function isAtomic(value) {
  if (typeof value === 'number') {
    return true;
  } else if (typeof value === 'string') {
    return true;
  } else if (value instanceof Date) {
    return true;
  } else if (value === null) {
    return true;
  }
  return false;
}

function wrapElementForAcces(doc, path) {
  var formatDocTraversErrorMessage = function(selector) {
    return 'cannot use the part (' +
      selector + ' of + ' + path + ') to traverse the document ' + doc;
  };
  var wrapElement = function(parentElem, selector) {
    var doc = parentElem.getValue();

    if (Array.isArray(doc)) {
      if (selector.match(/^[0-9]$/)) {
        selector = parseInt(selector);
        while (doc.length <= selector) {
          doc.push(null);
        }
      } else {
        throw new Error(formatDocTraversErrorMessage(selector));
      }
    } else if (isAtomic(doc)) {
        throw new Error(formatDocTraversErrorMessage(selector));
    } else if (typeof doc === 'undefined') {
      parentElem.setValue({});
    }
    return {
      getValue: function() { return doc[selector]; },
      setValue: function(value) { doc[selector] = value; }
    };
  }

  var wrapElementAt = function(parentElem, selectors) {
    if (selectors.length === 0) {
      return parentElem;
    }
    return wrapElementAt(
      wrapElement(parentElem, selectors[0]),
      selectors.slice(1));
  };

  return wrapElementAt({getValue: function() { return doc; }}, path.split('.'));
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
    if (clientReqMsg.query && !helper.isEmpty(clientReqMsg.query)) {
      docs = filter.filterItems(collection, clientReqMsg.query);
    } else {
      docs = collection;
    }
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
    clientReqMsg.documents.forEach(function (document) {
      collection.push(document);
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
    docs.forEach(function (doc) {
      that.affectedDocuments++;
      var value;
      for (updateKey in clientReqMsg.update) {
        if (updateKey === '$set' ||
            updateKey === '$inc' ||
            updateKey === '$pull') {
          for (propKey in clientReqMsg.update[updateKey]) {
            var property = wrapElementForAcces(doc, propKey);
            value = clientReqMsg.update[updateKey][propKey];
            if (updateKey == '$inc') {
              property.setValue(property.getValue() + value);
            } else if (updateKey === '$pull') {
              // TODO(vladlosev): Support queries in $pull,
              // e.g. db.collection.update({name: 'joe'}, {$pull: {scores: {$lt : 50}}})
              var arr = property.getValue();
              if (typeof arr === 'undefined') continue;
              if (!Array.isArray(arr)) {
                that.lastError = 'Cannot apply $pull to a non-array value';
                break;
              }
              _.pull(arr, value);
            } else {
              property.setValue(value);
            }
          }
        } else if (updateKey === '$pushAll') {
          for (propKey in clientReqMsg.update[updateKey]) {
            var property = wrapElementForAcces(doc, propKey);
            var arr = property.getValue();
            if (typeof arr === 'undefined') {
              arr = [];
              property.setValue(arr);
            } else if (Array.isArray(arr)) {
              var values = clientReqMsg.update[updateKey][propKey];
              values.forEach(function(element) { arr.push(element); });
            } else {
              that.lastError = "The field '" + propKey + "' must be an array.";
              break;
            }
          }
        } else {
          throw new Error('update value "' + updateKey + '" not supported');
        }
      }
    });
  }
};

module.exports = that;
