'use strict';

var _ = require('lodash');
var BSON = require('bson').BSONPure.BSON;

var that = {
  OP_REPLY: 1,
  OP_UPDATE: 2001,
  OP_INSERT: 2002,
  OP_QUERY: 2004,
  OP_DELETE: 2006,

  bitIsSet: function(num, bit) {
    return (num & (1 << bit)) !== 0;
  },

  readCollectionName: function(buf, i, message) {
    var j = i;
    while (buf[j] !== 0) {
      j++;
    }
    message.fullCollectionName = buf.toString('utf-8', i, j);
    return j + 1;
  },

  fromMsgHeaderBuf: function(buf) {
    var headerNames = ['messageLength', 'requestID', 'responseTo', 'opCode'];
    var i = 0;
    var msgHeader = {};
    for (var key in headerNames) {
      var headerName = headerNames[key];
      if (buf.length >= i + 4) {
        msgHeader[headerName] = buf.readInt32LE(i);
        i += 4;
      }
    }
    return msgHeader;
  },

  fromOpQueryBuf: function(header, buf) {
    var i = 4 * 4;
    var opQuery = {header: header};
    var flags = buf.readInt32LE(i);
    i += 4;

    opQuery.flags = {
      tailableCursor: that.bitIsSet(flags, 1),
      slaveOk: that.bitIsSet(flags, 2),
      oplogReplay: that.bitIsSet(flags, 3),
      noCursorTimeout: that.bitIsSet(flags, 4),
      awaitData: that.bitIsSet(flags, 5),
      exhaust: that.bitIsSet(flags, 6),
      partial: that.bitIsSet(flags, 7)
    };

    i = that.readCollectionName(buf, i, opQuery);

    opQuery.numberToSkip = buf.readInt32LE(i);
    i += 4;
    opQuery.numberToReturn = buf.readInt32LE(i);
    i += 4;

    var docs = [];
    var bson = new BSON();

    while (i < header.messageLength) {
      var bsonSize = buf.readInt32LE(i);
      i = bson.deserializeStream(buf.slice(0, i + bsonSize), i, 1, docs, docs.length);
    }
    opQuery.query = docs[0];
    opQuery.returnFieldSelector = docs[1];

    buf.bytesRead = i;
    return opQuery;
  },

  fromOpInsertBuf: function(header, buf) {
    var i = 4 * 4;
    var opInsert = {header: header};

    var flags = buf.readInt32LE(i);
    opInsert.flags = {
      continueOnError: that.bitIsSet(flags, 1)
    };
    i += 4;

    i = that.readCollectionName(buf, i, opInsert);

    var docs = [];
    var bson = new BSON();

    while (i < header.messageLength) {
      i = bson.deserializeStream(buf, i, 1, docs, docs.length);
    }
    opInsert.documents = docs;

    buf.bytesRead = i;
    return opInsert;
  },

  fromOpDeleteBuf: function(header, buf) {
    var i = 4 * 4;
    var opDelete = {header: header};
    i += 4;

    i = that.readCollectionName(buf, i, opDelete);

    var flags = buf.readInt32LE(i);
    opDelete.flags = {
      singleRemove: that.bitIsSet(flags, 1)
    };
    i += 4;

    var docs = [];
    var bson = new BSON();
    i = bson.deserializeStream(buf, i, 1, docs, 0);
    opDelete.selector = docs[0];

    buf.bytesRead = i;
    return opDelete;
  },

  fromOpUpdateBuf: function(header, buf) {
    var i = 4 * 4;
    var opUpdate = {header: header};
    i += 4;

    i = that.readCollectionName(buf, i, opUpdate);

    var flags = buf.readInt32LE(i);
    opUpdate.flags = {
      upsert: that.bitIsSet(flags, 0),
      multiUpdate: that.bitIsSet(flags, 1)
    };
    i += 4;

    var docs = [];
    var bson = new BSON();
    while (i < header.messageLength) {
      i = bson.deserializeStream(buf, i, 1, docs, docs.length);
    }
    opUpdate.selector = docs[0];
    opUpdate.update = docs[1];

    buf.bytesRead = i;
    return opUpdate;
  },

  toOpReplyBuf: function(opQuery, reply) {
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
    var documents = reply.documents instanceof Array ? reply.documents : [ reply.documents ];
    if (_.isUndefined(documents[0])) {
      documents.splice(0, 1);
    }
    reply.documents = documents;
    reply.numberReturned = reply.documents.length;
    var buf = new Buffer(36);
    var i = 4;

    buf.writeUInt32LE(reply.header.requestID, i);
    i += 4;
    buf.writeUInt32LE(reply.header.responseTo, i);
    i += 4;
    buf.writeUInt32LE(reply.header.opCode, i);
    i += 4;

    var nibble = reply.responseFlags.cursorNotFound ? 1 : 0
      || reply.responseFlags.queryFailure ? 2 : 0
      || reply.responseFlags.shardConfigStale ? 4 : 0
      || reply.responseFlags.awaitCapable ? 8 : 0;
    buf.writeUInt32LE(nibble, i);
    i += 4;

    buf.writeUInt32LE(reply.cursorID, i);
    i += 4;
    buf.writeUInt32LE(reply.cursorID << 32, i);
    i += 4;
    buf.writeUInt32LE(reply.startingFrom, i);
    i += 4;
    buf.writeUInt32LE(reply.numberReturned, i);
    i += 4;

    var bson = new BSON();
    var buffers = [buf].concat(reply.documents.map(function(document) {
      return bson.serialize(document);
    }));

    buf = Buffer.concat(buffers);
    buf.writeUInt32LE(buf.length, 0);
    return buf;
  }
};

module.exports = that;
