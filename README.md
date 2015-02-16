[![Build Status](https://travis-ci.org/vladlosev/mongodb-fs.svg?branch=stable)](https://travis-ci.org/vladlosev/mongodb-fs)
## What's mongodb-fs?

  mongodb-fs is a fake, lightweight, seamlessly mongodb server for nodejs (fs stands for 'Fake Server', or 'File System').

  Its goal is to act as a normal mongodb server, but with a simple json file instead of a database.

  Because it's not so simple to work with mocks on the client side, this solution gives you mocks on the server side.

  mongodb-fs can nicely replace a real mongodb server for unit tests phase.

  All you have to do is to provide a mocks module when you start the db server, and go on with your client code.

## Installation

    $ npm install mongodb-fs


## Usage

```javascript
var mongoose = require('mongoose')
  , mongodbFs = require('mongodb-fs');

// Usual mongoose code to define a schema for contact entities
mongoose.model('Contact', {
  firstName: String,
  lastName: String
});

// Initialize the server
mongodbFs.init({
  port: 27027, // Feel free to match your settings
  mocks: { // The all database is here...
    fakedb: { // database name
      contacts: [ // a collection
        {firstName: 'John', lastName: 'Doe'},
        {firstName: 'Forrest', lastName: 'Gump'}
      ]
    }
  },
  log: {
    // 'warn' is default; specify 'info' or 'debug' to see more info.  But
    // never, ever specify 'trace' here.  Default is 'warn'.
    level: 'warn',
    // Optional log4js category to use when logging events.  A trailing period
    // instructs individual modules to append their module names to the
    // category.  Default is 'mongodb-fs.'
    category: 'mongodb-fs',
    // You may supply your own logger to use instead of a default one.  The
    // logger must implement the following methods: error, warn, info, debug,
    // trace.
    logger: undefined
  }
});

// Start the fake server
mongodbFs.start(function (err) {
  mongoose.connect('mongodb://localhost:27027/fakedb', // 'fakedb' should be available in mocks
    {server: {poolSize: 1}}, // usual options
    function (err) {
      // Usual mongoose code to retreive all the contacts
      var Contact;
      Contact = mongoose.connection.model('Contact');
      Contact.find(function (err, contacts) {
        //
        console.log('contacts :', contacts);
        //
        mongoose.disconnect(function (err) { // clean death
          mongodbFs.stop(function (err) {
            console.log('bye!');
          });
        });
      });
    });
});

```

For a more complete example : have a look at the
[unit test](https://github.com/openhoat/mongodb-fs/tree/master/test/testMongoDbFs.js)

## Limitations

Supported operations : all queries, update, remove, insert

Only a few mongodb operators are supported by the moment (gt, gte, lt, gte, ne).


Enjoy !
