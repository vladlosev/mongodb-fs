var _ = require('lodash');
var chai = require('chai');
var log4js = require('log4js');
var mongodbFs = require('../lib/mongodb-fs');
var mongoose = require('mongoose');
var path = require('path');
var util = require('util');

var TestHarness = require('./test_harness');

var Item;
var NonExistent;

var mocks = {
  fakedb: {
    items: [
      {
        _id: new mongoose.Types.ObjectId(),
        field1: 'value1',
        field2: {
          field3: 31,
          field4: 'value4'
        },
        field5: ['a', 'b', 'c']
      },
      {
        _id: new mongoose.Types.ObjectId(),
        field1: 'value11',
        field2: {
          field3: 32,
          field4: 'value14'
        },
        field5: ['a', 'b', 'd']
      },
      {
        _id: new mongoose.Types.ObjectId(),
        field1: 'value21',
        field2: {
          field3: 33,
          field4: 'value24'
        },
        field5: ['a', 'e', 'f']
      }
    ]
  }
};

describe('MongoDB-Fs', function() {
  var expect = chai.expect;
  var harness = new TestHarness(mocks);

  before(function(done) {
    harness.config.fork = true;
    harness.setUp(function(error) {
      if (error) return done(error);

      var itemSchema = {
        field1: String,
        field2: {
          field3: Number,
          field4: String
        },
        field5: Array
      };
      delete mongoose.connection.models.Item;
      Item = mongoose.model('Item', itemSchema);
      NonExistent = mongoose.model('NonExistent', { name: String });
      done();
    });
  });

  after(function(done) {
    harness.tearDown(done);
  });

  beforeEach(function(done) {
    // Restore the database to the original state to make tests
    // order-independent.
    Item.remove({}, function(error) {
      if (error) return done(error);
      // Use copies of the original mock objects to avoid one test affecting
      // others by modifying objects in the database.
      var itemsCopy = _.cloneDeep(mocks.fakedb.items, function(value) {
        if (value instanceof mongoose.Types.ObjectId) {
          return new mongoose.Types.ObjectId(value.toString());
        } else {
          return undefined;
        }
      });
      Item.collection.insert(itemsCopy, done);
    });
  });

  describe('filters', function() {
    it('$all', function(done) {
      Item.find({'field5': {$all: ['a', 'c']}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(1);
        expect(items[0].toObject())
          .to.have.property('field5')
          .that.is.deep.equal(['a', 'b', 'c']);
        done();
      });
    });

    it('$gt', function(done) {
      Item.find({'field2.field3': {$gt: 32}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(1);
        expect(items[0].toObject())
          .to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$gte', function(done) {
      Item.find({'field2.field3': {$gte: 32}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(2);
        done();
      });
    });

    it('$in', function(done) {
      Item.find({'field2.field3': {$in: [32, 33]}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(2);
        expect(items[0]).to.have.deep.property('field2.field3', 32);
        expect(items[1]).to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$lt', function(done) {
      Item.find({'field2.field3': { $lt: 32}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(1);
        expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('$lte', function(done) {
      Item.find({'field2.field3': {$gte: 32}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(2);
        done();
      });
    });

    it('$ne', function(done) {
      Item.find({'field2.field3': {$ne: 32}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(2);
        expect(items[0]).to.have.deep.property('field2.field3', 31);
        expect(items[1]).to.have.deep.property('field2.field3', 33);
        done();
      });
    });

    it('$nin', function(done) {
      Item.find({'field2.field3': {$nin: [32, 33]}}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(1);
        expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('$or', function(done) {
      Item.find({$or: [
        {field1: 'value1'},
        {'field2.field3': 32}
      ]}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(2);
        expect(items[0]).to.have.property('field1', 'value1');
        expect(items[0]).to.have.deep.property('field2.field3', 31);
        expect(items[1]).to.have.property('field1', 'value11');
        expect(items[1]).to.have.deep.property('field2.field3', 32);
        done();
      });
    });

    it('simple filter', function(done) {
      Item.find({'field2.field3': 32}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(1);
        expect(items[0]).to.have.deep.property('field2.field3', 32);
        done();
      });
    });

    it('2 fields filter', function(done) {
      Item.find({field1: 'value1', 'field2.field3': 31}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(1);
        expect(items[0]).to.have.property('field1', 'value1');
        expect(items[0]).to.have.deep.property('field2.field3', 31);
        done();
      });
    });

    it('string filter', function(done) {
      Item.find({'field2.field4': 'value24'}, function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(1);
        expect(items[0]).to.have.deep.property('field2.field4', 'value24');
        done();
      });
    });

    it('reports invalid queries', function(done) {
      Item.find({'$eq': 'value24'}, function(error, items) {
        expect(error).to.match(/BadValue unknown top level operator: [$]eq/);
        expect(items).to.not.exist;
        done();
      });
    });
  });

  describe('find', function() {
    it('finds all documents', function(done) {
      Item.find(function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(3);
        done();
      });
    });

    it('finds no unknown documents', function(done) {
      NonExistent.find(function(error, items) {
        if (error) return done(error);
        expect(items).to.have.length(0);
        done();
      });
    });

    it('supports skip', function(done) {
      Item.find({}).skip(1).exec(function(error, items) {
        if (error) return done(error);
        expect(_.pluck(items, 'field1')).to.deep.equal(['value11', 'value21']);
        done();
      });
    });

    it('supports limit', function(done) {
      Item.find({}).limit(2).exec(function(error, items) {
        if (error) return done(error);
        expect(_.pluck(items, 'field1')).to.deep.equal(['value1', 'value11']);
        done();
      });
    });

    it('supports skip together with limit', function(done) {
      Item.find({}).skip(1).limit(1).exec(function(error, items) {
        if (error) return done(error);
        expect(_.pluck(items, 'field1')).to.deep.equal(['value11']);
        done();
      });
    });

    it('findById', function(done) {
      Item.findOne({field1: 'value1'}, function(error, item) {
        if (error) return done(error);
        expect(item).to.have.property('id');
        var itemId = item.id;
        Item.findById(itemId, function(error, item) {
          if (error) return done(error);
          expect(item).to.not.be.empty;
          done();
        });
      });
    });

    it('findByIdAndUpdate', function(done) {
      Item.findOne({field1: 'value1'}, function(error, item) {
        if (error) return done(error);
        expect(item).to.have.property('id');
        var itemId = item.id;
        Item.findByIdAndUpdate(
          itemId,
          {'$set': {field1: 'value1Modified'}},
          function(error, item) {
            if (error) return done(error);
            expect(item).to.have.property('field1', 'value1Modified');
            done();
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
      item.save(function(error, savedItem) {
        if (error) return done(error);
        expect(savedItem).to.exist;
        Item.findById(savedItem._id, function(error, newItem) {
        if (error) return done(error);
          expect(newItem).to.exist;
          expect(newItem.toObject())
            .to.deep.equal(savedItem.toObject());
          Item.collection.count({}, function(error, count) {
            if (error) return done(error);
            expect(count).to.equal(mocks.fakedb.items.length + 1);
            done();
          });
        });
      });
    });

    it('changes document count', function(done) {
      var item = new Item(newItemFields);
      item.save(function(error, savedItem) {
        if (error) return done(error);
        expect(savedItem).to.exist;
        Item.collection.count({}, function(error, count) {
          if (error) return done(error);
          expect(count).to.equal(mocks.fakedb.items.length + 1);
          done();
        });
      });
    });
  });

  describe('remove', function() {
    it('removes document from collection', function(done) {
      Item.findOne({'field1': 'value11'}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist;
        item.remove(function(error) {
          if (error) return done(error);
          Item.findById(item._id, function(error, noItem) {
            if (error) return done(error);
            expect(noItem).to.not.exist;
            done();
          });
        });
      });
    });

    it('changes documents count', function(done) {
      Item.findOne({'field1': 'value11'}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist;
        item.remove(function(error) {
          Item.collection.count({}, function(error, count) {
            if (error) return done(error);
            expect(count).to.equal(mocks.fakedb.items.length - 1);
            done();
          });
        });
      });
    });

    it('removes documents by query', function(done) {
      Item.remove({'field2.field3': {$gt: 31}}, function(error, numAffected) {
        if (error) return done(error);
        expect(numAffected).to.equal(2);
        Item.find({}, function(error, items) {
        if (error) return done(error);
          expect(items).to.have.length(1);
          expect(items[0].toObject())
            .to.deep.equal(mocks.fakedb.items[0]);
          done();
        });
      });
    });
  });

  describe('update', function() {
    it('updates top-level fields', function(done) {
      Item.findOne({'field1': 'value11'}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist;
        item.field1 = 'new value';
        item.save(function(error) {
          if (error) return done(error);
          Item.findById(item._id, function(error, newItem) {
            if (error) return done(error);
            expect(newItem).to.exist;
            expect(newItem.toObject()).to.deep.equal(item.toObject());
            done();
          });
        });
      });
    });

    it('does not change document count', function(done) {
      Item.findOne({}, function(error, item) {
        if (error) return done(error);
        expect(item).to.exist;
        item.field1 = 'new value';
        item.save(function(error) {
          Item.collection.count({}, function(error, count) {
            if (error) return done(error);
            expect(count).to.equal(mocks.fakedb.items.length);
            done();
          });
        });
      });
    });

    it('updates fields in subdocuments', function(done) {
      Item.findOne({'field1': 'value11'}, function(error, item) {
        if (error) return done(error);
        expect(item).to.have.deep.property('field2.field3');
        item.field2.field3 = 424242;
        item.save(function(error) {
          if (error) return done(error);
          Item.findById(item._id, function(error, newItem) {
            if (error) return done(error);
            expect(newItem).to.exist;
            expect(newItem.toObject()).to.deep.equal(item.toObject());
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
        function(error, numAffected) {
          if (error) return done(error);
          expect(numAffected).to.equal(2);
          Item.find({}, function(error, items) {
            if (error) return done(error);
            expect(items)
              .to.have.deep.property('[0].field1', 'value1');
            expect(items)
              .to.have.deep.property('[1].field1', 'new value');
            expect(items)
              .to.have.deep.property('[2].field1', 'new value');
            done();
          });
        });
    });
  });
});
