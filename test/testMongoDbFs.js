var util = require('util')
  , _ = require('lodash')
  , chai = require('chai')
  , path = require('path')
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
    mongodbFs.init(config);
    if (logLevel == 'TRACE') {
      mongoose.set('debug', true);
    }
    logger.trace('init');
    mongodbFs.start(function(err) {
      if (err) return done(err);
      logger.trace('connect to db');
      mongoose.connect(dbConfig.url, dbOptions, function(err) {
        if (err) {
          mongodbFs.stop(function() { done(err); });
          return;
        }
        Item = mongoose.connection.model('Item');
        Unknown = mongoose.connection.model('Unknown');
        done();
      });
    });
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
      // Use copies of the original mock objects to avoid one test affecting
      // others by modifying objects in the database.
      Item.collection.insert(_.cloneDeep(mocks.fakedb.items, function(value) {
        if (value instanceof mongoose.Types.ObjectId) {
          return new mongoose.Types.ObjectId(value.toString());
        } else {
          return undefined;
        }
      }), done);
    });
  });

  describe('filters', function() {
    it('$all', function(done) {
      Item.find({'field5': {$all: ['a', 'c']}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0].toObject())
          .to.have.property('field5')
          .that.is.deep.equal(['a', 'b', 'c']);
        done();
      });
    });

    it('$gt', function(done) {
      Item.find({'field2.field3': {$gt: 32}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0].toObject())
          .to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$gte', function(done) {
      Item.find({'field2.field3': {$gte: 32}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        done();
      });
    });

    it('$in', function(done) {
      Item.find({'field2.field3': {$in: [32, 33]}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 32);
        chai.expect(items[1]).to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$lt', function(done) {
      Item.find({'field2.field3': { $lt: 32}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('$lte', function(done) {
      Item.find({'field2.field3': {$gte: 32}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        done();
      });
    });

    it('$ne', function(done) {
      Item.find({'field2.field3': {$ne: 32}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(2);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        chai.expect(items[1]).to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$nin', function(done) {
      Item.find({'field2.field3': {$nin: [32, 33]}}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('$or', function(done) {
      Item.find({$or: [
        {field1: 'value1'},
        {'field2.field3': 32}
      ]}, function(err, items) {
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
      Item.find({'field2.field3': 32}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field3', 32);
        done();
      });
    });

    it('2 fields filter', function(done) {
      Item.find({field1: 'value1', 'field2.field3': 31}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.property('field1', 'value1');
        chai.expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('string filter', function(done) {
      Item.find({'field2.field4': 'value24'}, function(err, items) {
        chai.expect(err).to.not.exist;
        chai.expect(items).to.have.length(1);
        chai.expect(items[0]).to.have.deep.property('field2.field4', 'value24');
        done();
      });
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

    it('findById', function(done) {
      Item.findOne({field1: 'value1'}, function(err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.have.property('id');
        logger.trace('item :', item);
        var itemId = item.id;
        logger.trace('itemId :', itemId);
        Item.findById(itemId, function(err, item) {
          chai.expect(err).to.not.exist;
          chai.expect(item).to.not.be.empty;
          done();
        });
      });
    });

    xit('findByIdAndUpdate', function(done) {
      Item.findOne({field1: 'value1'}, function(err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.have.property('id');
        logger.trace('item :', item);
        var itemId = item.id;
        logger.trace('itemId :', itemId);
        Item.findByIdAndUpdate(itemId, {field1: 'value1Modified'}, function(err, item) {
          chai.expect(err).to.not.exist;
          chai.expect(item).to.not.be.empty;
          chai.expect(item).to.have.property('field1', 'value1Modified');
        });
      });
    });
  });

  describe('insert', function() {
    var newItemFields = {
      field1: 'value101',
      field2: {
        field3: 1031,
        field4: 'value104'},
      field5: ['h', 'i', 'j']};

    it('saves document to collection', function(done) {
      var item = new Item(newItemFields);
      item.save(function(err, savedItem) {
        chai.expect(err).to.not.exist;
        chai.expect(savedItem).to.exist;
        Item.findById(savedItem._id, function(err, newItem) {
          chai.expect(err).to.not.exist;
          chai.expect(newItem).to.exist;
          chai.expect(newItem.toObject())
            .to.deep.equal(savedItem.toObject());
          Item.collection.count({}, function(err, count) {
            chai.expect(err).to.not.exist;
            chai.expect(count).to.equal(mocks.fakedb.items.length + 1);
            done();
          });
        });
      });
    });

    it('changes document count', function(done) {
      var item = new Item(newItemFields);
      item.save(function(err, savedItem) {
        chai.expect(err).to.not.exist;
        chai.expect(savedItem).to.exist;
        Item.collection.count({}, function(err, count) {
          chai.expect(err).to.not.exist;
          chai.expect(count).to.equal(mocks.fakedb.items.length + 1);
          done();
        });
      });
    });
  });

  describe('remove', function() {
    it('removes document from collection', function(done) {
      Item.findOne({'field1': 'value11'}, function(err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.exist;
        item.remove(function(err) {
          chai.expect(err).to.not.exist;
          Item.findById(item._id, function(err, noItem) {
            chai.expect(err).to.not.exist;
            chai.expect(noItem).to.not.exist;
            done();
          });
        });
      });
    });

    it('changes documents count', function(done) {
      Item.findOne({'field1': 'value11'}, function(err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.exist;
        item.remove(function(err) {
          Item.collection.count({}, function(err, count) {
            chai.expect(err).to.not.exist;
            chai.expect(count).to.equal(mocks.fakedb.items.length - 1);
            done();
          });
        });
      });
    });

    it('removes documents by query', function(done) {
      Item.remove({'field2.field3': {$gt: 31}}, function(err, numAffected) {
        chai.expect(err).to.not.exist;
        chai.expect(numAffected).to.equal(2);
        Item.find({}, function(err, items) {
          chai.expect(err).to.not.exist;
          chai.expect(items).to.have.length(1);
          chai.expect(items[0].toObject())
            .to.deep.equal(mocks.fakedb.items[0]);
          done();
        });
      });
    });
  });

  describe('update', function() {
    it('updates top-level fields', function(done) {
      Item.findOne({'field1': 'value11'}, function(err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.exist;
        item.field1 = 'new value';
        item.save(function(err) {
          chai.expect(err).to.not.exist;
          Item.findById(item._id, function(err, newItem) {
            chai.expect(err).to.not.exist;
            chai.expect(newItem).to.exist;
            chai.expect(newItem.toObject()).to.deep.equal(item.toObject());
            done();
          });
        });
      });
    });

    it('does not change document count', function(done) {
      Item.findOne({}, function(err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.exist;
        item.field1 = 'new value';
        item.save(function(err) {
          Item.collection.count({}, function(err, count) {
            chai.expect(err).to.not.exist;
            chai.expect(count).to.equal(mocks.fakedb.items.length);
            done();
          });
        });
      });
    });

    it('updates fields in subdocuments', function(done) {
      Item.findOne({'field1': 'value11'}, function(err, item) {
        chai.expect(err).to.not.exist;
        chai.expect(item).to.have.deep.property('field2.field3');
        item.field2.field3 = 424242;
        item.save(function(err) {
          chai.expect(err).to.not.exist;
          Item.findById(item._id, function(err, newItem) {
            chai.expect(err).to.not.exist;
            chai.expect(newItem).to.exist;
            chai.expect(newItem.toObject()).to.deep.equal(item.toObject());
            done();
          });
        });
      });
    });

    it('updates documents by query', function(done) {
      Item.update(
        {'field2.field3': {$gt: 31}},
        {$set: {field1: 'new value'}},
        {multi: true},
        function(err, numAffected) {
          chai.expect(err).to.not.exist;
          chai.expect(numAffected).to.equal(2);
          Item.find({}, function(err, items) {
            chai.expect(err).to.not.exist;
            chai.expect(items)
              .to.have.deep.property('[0].field1', 'value1');
            chai.expect(items)
              .to.have.deep.property('[1].field1', 'new value');
            chai.expect(items)
              .to.have.deep.property('[2].field1', 'new value');
            done();
          });
        });
    });
  });
});
