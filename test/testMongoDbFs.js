var util = require('util')
  , chai = require('chai')
  , path = require('path')
  , nodeunit = require('nodeunit')
  , mongodbFs = require('../lib/mongodb-fs')
  , mongoose = require('mongoose')
  , Profess = require('profess')
  , log = require('../lib/log')
  , helper = require('../lib/helper')
  , mocks = require('./mocks')
  , config, logger, schema, dbConfig, dbOptions, Item, Unknown;

var logLevel = process.env.LOG_LEVEL || 'WARN';

config = {
  port: 27027,
  mocks: {fakedb: {items: []}},
  fork: true,
  log: {
    log4js: {
      appenders: [
        {
          type: 'console',
          category: path.basename(__filename)
        }
      ]
    },
    category: path.basename(__filename),
    level: logLevel
  }
};

log.init(config.log);
logger = log.getLogger();

schema = {
  field1: String,
  field2: {
    field3: Number,
    field4: String
  },
  field5: Array
};

dbConfig = {
  name: 'fakedb'
};
dbConfig.url = util.format('mongodb://localhost:%d/%s', config.port, dbConfig.name);

dbOptions = {
  server: { poolSize: 1 }
};

mongoose.model('Item', schema);
mongoose.model('Unknown', { name: String });

describe('MongoDB-Fs', function() {
  before(function(done) {
    var profess;
    profess = new Profess();
    profess.
      do(function () {
        //return profess.next();
        if (!mongodbFs.isRunning()) {
          mongodbFs.init(config);
          logger.trace('init');
          mongodbFs.start(profess.next);
          nodeunit.on('complete', function () {
            mongodbFs.stop();
          });
        } else {
          profess.next();
        }
      }).
      then(function () {
        logger.trace('connect to db');
        mongoose.connect(dbConfig.url, dbOptions, profess.next);
        if (logLevel == 'TRACE') {
          mongoose.set('debug', true);
        }
        //test.ok(mongoose.connection.readyState);
      }).
      then(function () {
        Item = mongoose.connection.model('Item');
        Unknown = mongoose.connection.model('Unknown');
        profess.next();
      }).
      then(done);
  });

  after(function(done) {
    logger.trace('disconnect');
    mongoose.disconnect(function() {
      mongodbFs.stop(done);
    });
  });

  beforeEach(function(done) {
    // Restore the database to the original state to make tests
    // order-independent.
    Item.remove({}, function(err) {
      if (err) done(err);
      Item.collection.insert(mocks.fakedb.items, done);
    });
  });

  describe('find', function() {
    it('finds all documents', function(done) {
      Item.find(function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(3);
        done();
      });
    });

    it('finds no unknown documents', function(done) {
      Unknown.find(function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(0);
        done();
      });
    });
  });

  describe('filters', function() {
    it('$all', function(done) {
      Item.find({ 'field5': { $all: ['a', 'c'] } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0].toObject())
          .to.have.property('field5')
          .that.is.deep.equal(['a', 'b', 'c']);
        done();
      });
    });

    it('$gt', function(done) {
      Item.find({ 'field2.field3': { $gt: 32 } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0].toObject())
          .to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$gte', function(done) {
      Item.find({ 'field2.field3': { $gte: 32 } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        done();
      });
    });

    it('$in', function(done) {
      Item.find({ 'field2.field3': { $in: [32, 33] } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 32);
        chai.expect(items[1]).to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$lt', function(done) {
      Item.find({ 'field2.field3': { $lt: 32 } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('$lte', function(done) {
      Item.find({ 'field2.field3': { $gte: 32 } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        done();
      });
    });

    it('$ne', function(done) {
      Item.find({ 'field2.field3': { $ne: 32 } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        chai.expect(items[1]).to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$nin', function(done) {
      Item.find({ 'field2.field3': { $nin: [32, 33] } }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('$or', function(done) {
      Item.find({ $or: [
        { field1: 'value1' },
        { 'field2.field3': 32 }
      ]}, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        chai.expect(items[0]).to.have.property('field1', 'value1');
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        chai.expect(items[1]).to.have.property('field1', 'value11');
        chai.expect(items[1]).to.have.deep.property('field2.field3', 32);
        done();
      });
    });

    it('simple filter', function(done) {
      Item.find({ 'field2.field3': 32 }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 32);
        done();
      });
    });

    it('2 fields filter', function(done) {
      Item.find({ field1: 'value1', 'field2.field3': 31 }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.property('field1', 'value1');
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('string filter', function(done) {
      Item.find({ 'field2.field4': 'value24' }, function (err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field4', 'value24');
        done();
      });
    });
  });

  it('findById', function(done) {
    Item.findOne({field1: 'value1'}, function (err, item) {
      chai.expect(err).to.not.exist;
      chai.expect(item).to.have.property('id');
      logger.trace('item :', item);
      var itemId = item.id;
      logger.trace('itemId :', itemId);
      Item.findById(itemId, function (err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.not.be.empty;
        done();
      });
    });
  });

  xit('findByIdAndUpdate', function(done) {
    Item.findOne({field1: 'value1'}, function (err, item) {
      chai.expect(err).to.not.exist;
      chai.expect(item).to.have.property('id');
      logger.trace('item :', item);
      var itemId = item.id;
      logger.trace('itemId :', itemId);
      Item.findByIdAndUpdate(itemId, {field1: 'value1Modified'}, function (err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.not.be.empty;
        chai.expect(item).to.have.property('field1', 'value1Modified');
      });
    });
  });

  it('remove', function(done) {
    Item.findOne({ 'field1': 'value11' }, function (err, item) {
      chai.expect(err).to.not.exist;
      chai.expect(item).to.exist;
      item.remove(function (err) {
        chai.expect(err).to.not.exist;
        // TODO(vladlosev): verify that item no longer loads.
        done();
      });
    });
  });

  it('crud methods work', function(done) {
    var noItems, item;
    var profess = new Profess();
    var errorHandler = profess.handleError(done);
    profess.
      do(function () { // load all items
        Item.find(errorHandler);
      }).
      then(function (items) { // check
        chai.expect(items).to.not.be.empty;
        noItems = items.length;
        profess.next();
      }).
      then(function () { // insert item
        item = new Item({
          field1: 'value101',
          field2: {
            field3: 1031,
            field4: 'value104'
          }
        });
        item.save(errorHandler);
      }).
      then(function (item) { // check
        chai.expect(item).to.exist;
        profess.next();
      }).
      then(function (item) { // find item
        Item.findOne({ 'field2.field3': 1031 }, errorHandler);
      }).
      then(function (savedItem) { // check saved item
        chai.expect(item).to.have.property('field1', savedItem.field1);
        chai.expect(item)
          .to.have.deep.property('field2.field3', savedItem.field2.field3);
        chai.expect(item)
          .to.have.deep.property('field2.field4', savedItem.field2.field4);
        profess.next();
      }).
      then(function () { // load all items
        Item.find(errorHandler);
      }).
      then(function (items) { // check
        chai.expect(items).to.have.length(noItems + 1);
        profess.next();
      }).
      then(function () { // update item
        item.field2.field3 = 2031;
        item.save(errorHandler);
      }).
      then(function (item) { // check
        chai.expect(item).to.exist;
        profess.next();
      }).
      then(function () { // remove item
        Item.remove({_id: item._id }, errorHandler);
      }).
      then(function () { // load all items
        Item.find(errorHandler);
      }).
      then(function (items) { // check
        chai.expect(items).to.have.length(noItems);
        profess.next();
      }).
      then(done);
  });

  it('insert', function(done) {
    var item = new Item({
      field1: 'value101',
      field2: {
        field3: 1031,
        field4: 'value104'
      },
      field5: ['h', 'i', 'j']
    });
    item.save(function (err, savedItem) {
      chai.expect(err).to.not.exist;
      chai.expect(item).to.exist;
      item.remove(function(err) {
        chai.expect(err).to.not.exist;
        done();
      });
    });
  });
});
