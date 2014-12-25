var _ = require('lodash');
var util = require('util');
var net = require('net');
var ObjectID = require('bson').ObjectID;

var helper = require('./helper');
var filter = require('./filter');
var update = require('./update');

var logger;

function getCollection(clientReqMsg) {
  return eval('that.mocks.' + clientReqMsg.fullCollectionName) || [];
}

function keyIsOperator(value, key) {
  return helper.isOperator(key);
}

function cloneDocuments(doc) {
  return _.cloneDeep(
    doc,
    function(value) {
      return value instanceof ObjectID ? new ObjectID(value) : undefined;
    });
}

that = {
  mocks: null,
  init: function(mocks) {
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
            if (clientReqMsg.query && clientReqMsg.query.findandmodify) {
              that.doFindAndModify(socket, clientReqMsg);
            } else {
              that.doCmdQuery(socket, clientReqMsg);
            }
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
    logger.trace('doUpdate');
    var collection = getCollection(clientReqMsg);
    var docs;
    if (clientReqMsg.selector && !helper.isEmpty(clientReqMsg.selector)) {
      docs = filter.filterItems(collection, clientReqMsg.selector);
    } else {
      docs = collection;
    }
    that.affectedDocuments = 0;
    var updateContainsOperators = _.any(clientReqMsg.update, keyIsOperator);
    var updateContainsOnlyOperators = _.every(
      clientReqMsg.update,
      keyIsOperator);

    if (clientReqMsg.flags.multiUpdate && !updateContainsOnlyOperators) {
      that.lastError = 'multi update only works with $ operators';
      return;
    }

    var literalSubfield = _.findKey(
      clientReqMsg.update,
      function(value, key) {
        return !helper.isOperator(key) && _.contains(key, '.');
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
      collection.push(upsertedDoc);
      docs = [upsertedDoc];
    }
    update(
      docs,
      clientReqMsg.selector,
      clientReqMsg.update,
      clientReqMsg.flags.multiUpdate,
      upsertedDoc,
      that);
  },
  doFindAndModify: function(socket, clientReqMsg) {
    function writeErrorReply(clientReqMsg, errorMessage) {
      var reply = {documents: [{ok: false, errmsg: errorMessage}]};
      var replyBuf = helper.toOpReplyBuf(clientReqMsg, reply);
      socket.write(replyBuf);
    }

    logger.trace('doFindAndModify');
    var dbName = clientReqMsg.fullCollectionName.replace('.$cmd', '');
    var collectionName = clientReqMsg.query.findandmodify;
    var collection = that.mocks[dbName][collectionName] || [];
    var docs;
    if (clientReqMsg.query && !helper.isEmpty(clientReqMsg.query.query)) {
      docs = filter.filterItems(collection, clientReqMsg.query.query);
    } else {
      docs = collection;
    }
    var updateKeys = Object.keys(clientReqMsg.query.update || {});
    var firstOperatorIndex = _.findIndex(updateKeys,helper.isOperator);
    var firstNonOperatorIndex = _.findIndex(updateKeys, function(key) {
      return !helper.isOperator(key);
    });
    if (firstOperatorIndex >= 0 && firstNonOperatorIndex >= 0) {
      var errorMessage;
      if (firstOperatorIndex < firstNonOperatorIndex) {
        errorMessage = util.format(
          "exception: Unknown modifier: %s",
          updateKeys[firstNonOperatorIndex]);
      } else {
        errorMessage = util.format(
          "exception: The dollar ($) prefixed field '%s' " +
          "in '%s' is not valid for storage.",
          updateKeys[firstOperatorIndex],
          updateKeys[firstOperatorIndex]);
      }
      writeErrorReply(clientReqMsg, errorMessage);
      return;
    }
    var literalSubfield = _.findKey(
      clientReqMsg.query.update,
      function(value, key) {
        return !helper.isOperator(key) && _.contains(key, '.');
      });
    if (literalSubfield) {
      writeErrorReply(
        clientReqMsg,
        util.format(
          "exception: The dotted field '%s' in '%s' is not valid for storage.",
          literalSubfield,
          literalSubfield));
      return;
    }
    var returnedDoc = null;
    if (!clientReqMsg.query.new) {
      returnedDoc = cloneDocuments(docs[0]);
    }
    var upsertedDoc;
    if (clientReqMsg.query.update) {
      if (docs.length === 0 && clientReqMsg.query.upsert) {
        upsertedDoc = {};
        docs = [upsertedDoc];
      }
      update(
        docs,
        clientReqMsg.query.query,
        clientReqMsg.query.update,
        false,
        upsertedDoc,
        that);
    } else if (clientReqMsg.query.remove) {
      if (clientReqMsg.query.new) {
        writeErrorReply(clientReqMsg, "remove and returnNew can't co-exist");
        return;
      }
      if (docs.length > 0) {
        _.pull(collection, docs[0]);
      }
    } else {
      that.lastError = 'need remove or update';
    }
    if (that.lastError) {
      writeErrorReply(clientReqMsg, that.lastError);
      delete that.lastError;
      return;
    }
    if (upsertedDoc) {
      if (!that.mocks[dbName][collectionName]) {
        that.mocks[dbName][collectionName] = docs;
      } else {
        collection.push(upsertedDoc);
      }
    }
    if (clientReqMsg.query.new) {
      returnedDoc = docs.length > 0 ? docs[0] : null;
    }
    var reply = {documents: {value: returnedDoc}};
    var replyBuf = helper.toOpReplyBuf(clientReqMsg, reply);
    socket.write(replyBuf);
  }
};

module.exports = that;
