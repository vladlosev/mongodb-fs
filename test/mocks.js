var mongoose = require('mongoose')
  , mocks;

mocks = {
  fakedb: {
    items: [
      {
        _id: new mongoose.Types.ObjectId,
        field1: 'value1',
        field2: {
          field3: 31,
          field4: 'value4'
        },
        field5: ['a', 'b', 'c']
      },
      {
        _id: new mongoose.Types.ObjectId,
        field1: 'value11',
        field2: {
          field3: 32,
          field4: 'value14'
        },
        field5: ['a', 'b', 'd']
      },
      {
        _id: new mongoose.Types.ObjectId,
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

module.exports = mocks;