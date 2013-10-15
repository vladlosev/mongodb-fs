var BSON = require('bson').BSONPure.BSON
  , Long = require('bson').Long
  , ObjectID = require('bson').ObjectID
  , Binary = require('bson').Binary
  , Code = require('bson').Code
  , DBRef = require('bson').DBRef
  , Symbol = require('bson').Symbol
  , Double = require('bson').Double
  , MaxKey = require('bson').MaxKey
  , MinKey = require('bson').MinKey
  , Timestamp = require('bson').Timestamp
  , that;

that = {
  OP_REPLY: 1,
  OP_UPDATE: 2001,
  OP_INSERT: 2002,
  OP_QUERY: 2004,
  OP_DELETE: 2006,
  copyProperties: function (src, dest, onlyExisting, exclude) {
    var key;
    if (typeof onlyExisting === 'undefined') {
      onlyExisting = typeof dest !== 'undefined';
    }
    if (typeof dest === 'undefined') {
      dest = {};
    }
    for (key in src) {
      if (exclude && exclude.indexOf(key) !== -1) {
        continue;
      }
      if (src.hasOwnProperty(key) && (!onlyExisting || dest.hasOwnProperty(key))) {
        if (typeof src[key] === 'object' && typeof dest[key] === 'object' && !(dest[key] instanceof Array) && dest[key] !== null) {
          if (src[key] === null) {
            dest[key] = src[key];
          } else {
            that.copyProperties(src[key], dest[key], onlyExisting);
          }
        } else {
          dest[key] = src[key];
        }
      }
    }
    return dest;
  },
  isEmpty: function (obj) {
    for (var prop in obj) {
      if (obj.hasOwnProperty(prop)) {
        return false;
      }
    }
    return true;
  },
  clone: function (src) {
    return JSON.parse(JSON.stringify(src));
  },
  bitIsSet: function (num, bit) {
    return ((num >> bit) % 2 != 0)
  },
  fromMsgHeaderBuf: function (buf) {
    var i, msgHeader, headerNames, int32Value;
    headerNames = ['messageLength', 'requestID', 'responseTo', 'opCode'];
    i = 0;
    msgHeader = {};
    for (var key in headerNames) {
      var headerName, int32Value;
      headerName = headerNames[key];
      if (buf.length >= i + 4) {
        msgHeader[headerName] = buf.readInt32LE(i);
        i += 4;
      }
    }
    return msgHeader;
  },
  fromOpQueryBuf: function (header, buf) {
    var opQuery, i;
    i = 4 * 4;
    opQuery = {};
    opQuery.header = header;
    (function () {
      var flags = buf.readInt32LE(i);
      opQuery.flags = {
        tailableCursor: that.bitIsSet(flags, 1),
        slaveOk: that.bitIsSet(flags, 2),
        oplogReplay: that.bitIsSet(flags, 3),
        noCursorTimeout: that.bitIsSet(flags, 4),
        awaitData: that.bitIsSet(flags, 5),
        exhaust: that.bitIsSet(flags, 6),
        partial: that.bitIsSet(flags, 7)
      };
      i += 4;
    })();
    (function () {
      var j = i;
      while (buf[j] !== 0) j++;
      opQuery.fullCollectionName = buf.toString('utf-8', i, j);
      i = j + 1;
    })();
    (function () {
      opQuery.numberToSkip = buf.readInt32LE(i);
      i += 4;
    })();
    (function () {
      opQuery.numberToReturn = buf.readInt32LE(i);
      i += 4;
    })();
    (function () {
      var bson, docs;
      docs = [];
      bson = new BSON();
      while (i < header.messageLength) {
        i = bson.deserializeStream(buf, i, 1, docs, docs.length);
      }
      opQuery.query = docs[0];
      opQuery.returnFieldSelector = docs[1];
    })();
    return opQuery;
  },
  fromOpInsertBuf: function (header, buf) {
    var opInsert, i;
    i = 4 * 4;
    opInsert = {};
    opInsert.header = header;
    (function () {
      var flags = buf.readInt32LE(i);
      opInsert.flags = {
        continueOnError: that.bitIsSet(flags, 1)
      };
      i += 4;
    })();
    (function () {
      var j = i;
      while (buf[j] !== 0) j++;
      opInsert.fullCollectionName = buf.toString('utf-8', i, j);
      i = j + 1;
    })();
    (function () {
      var bson, docs;
      docs = [];
      bson = new BSON();
      while (i < header.messageLength) {
        i = bson.deserializeStream(buf, i, 1, docs, docs.length);
      }
      opInsert.documents = docs;
    })();
    return opInsert;
  },
  fromOpDeleteBuf: function (header, buf) {
    var opDelete, i;
    i = 4 * 4;
    opDelete = {};
    opDelete.header = header;
    i += 4;
    (function () {
      var j = i;
      while (buf[j] !== 0) j++;
      opDelete.fullCollectionName = buf.toString('utf-8', i, j);
      i = j + 1;
    })();
    (function () {
      var flags = buf.readInt32LE(i);
      opDelete.flags = {
        singleRemove: that.bitIsSet(flags, 1)
      };
      i += 4;
    })();
    (function () {
      var bson, docs;
      docs = [];
      bson = new BSON();
      bson.deserializeStream(buf, i, 1, docs, 0);
      opDelete.selector = docs[0];
    })();
    return opDelete;
  },
  fromOpUpdateBuf: function (header, buf) {
    var opUpdate, i;
    i = 4 * 4;
    opUpdate = {};
    opUpdate.header = header;
    i += 4;
    (function () {
      var j = i;
      while (buf[j] !== 0) j++;
      opUpdate.fullCollectionName = buf.toString('utf-8', i, j);
      i = j + 1;
    })();
    (function () {
      var flags = buf.readInt32LE(i);
      opUpdate.flags = {
        upsert: that.bitIsSet(flags, 1),
        multiUpdate: that.bitIsSet(flags, 2)
      };
      i += 4;
    })();
    (function () {
      var bson, docs;
      docs = [];
      bson = new BSON();
      while (i < header.messageLength) {
        i = bson.deserializeStream(buf, i, 1, docs, docs.length);
      }
      opUpdate.selector = docs[0];
      opUpdate.update = docs[1];
    })();
    return opUpdate;
  },
  toOpReplyBuf: function (opQuery, reply) {
    var i, buffers, buf, documents;
    reply.header = reply.header || {};
    reply.header.requestID = reply.header.requestID || 0;
    reply.header.responseTo = reply.header.responseTo || opQuery.header.requestID;
    reply.header.opCode = reply.header.opCode || that.OP_REPLY;
    reply.responseFlags = reply.responseFlags || {};
    reply.responseFlags.cursorNotFound = reply.responseFlags.cursorNotFound || false;
    reply.responseFlags.queryFailure = reply.responseFlags.queryFailure || false;
    reply.responseFlags.shardConfigStale = reply.responseFlags.shardConfigStale || false;
    reply.responseFlags.awaitCapable = reply.responseFlags.awaitCapable || false;
    reply.cursorID = reply.cursorID || 0;
    reply.startingFrom = reply.startingFrom || 0;
    documents = reply.documents instanceof Array ? reply.documents : [ reply.documents ];
    if (typeof documents[0] === 'undefined') {
      documents.splice(0, 1);
      //documents[0] = { $err: 'Collection not found !' };
    }
    //console.log('documents :', documents);
    reply.documents = documents;
    reply.numberReturned = reply.documents.length;
    buffers = [ new Buffer(36) ];
    buf = buffers[0];
    i = 4;
    (function () {
      buf.writeUInt32LE(reply.header.requestID, i);
      i += 4;
    })();
    (function () {
      buf.writeUInt32LE(reply.header.responseTo, i);
      i += 4;
    })();
    (function () {
      buf.writeUInt32LE(reply.header.opCode, i);
      i += 4;
    })();
    (function () {
      var nibble;
      nibble = reply.responseFlags.cursorNotFound ? 1 : 0
        || reply.responseFlags.queryFailure ? 2 : 0
        || reply.responseFlags.shardConfigStale ? 4 : 0
        || reply.responseFlags.awaitCapable ? 8 : 0;
      buf.writeUInt32LE(nibble, i);
      i += 4;
    })();
    (function () {
      buf.writeUInt32LE(reply.cursorID, i);
      i += 4;
      buf.writeUInt32LE(reply.cursorID << 32, i);
      i += 4;
    })();
    (function () {
      buf.writeUInt32LE(reply.startingFrom, i);
      i += 4;
    })();
    (function () {
      buf.writeUInt32LE(reply.numberReturned, i);
      i += 4;
    })();
    (function () {
      var bson, serDoc;
      bson = new BSON();
      reply.documents.forEach(function (document) {
        serDoc = bson.serialize(document);
        buffers.push(serDoc);
      });
    })();
    (function () {
      buf = Buffer.concat(buffers);
      buf.writeUInt32LE(buf.length, 0);
    })();
    return buf;
  },
  safeCallback: function (callback) {
    return callback || function () {
    };
  }
};

module.exports = that;
