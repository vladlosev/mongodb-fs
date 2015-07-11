'use strict';

var mongoose = require('mongoose');
var util     = require('util');

var mongodbFs = require('../lib/mongodb-fs');

/* eslint-disable no-console */

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
    // never, ever specify 'trace'.  Default is 'warn'.
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

function reportError(when, error) {
  console.error(util.format('Error %s: %s\n', when, error));
}

mongodbFs.start(function(err) {
  if (err) return reportError('starting server', err);
  mongoose.connect(
    'mongodb://localhost:27027/fakedb',
    {server: {poolSize: 1}},
    function(err) {
      if (err) return reportError('opening connection', err);
      // Usual mongoose code to retreive all the contacts
      var Contact;
      Contact = mongoose.connection.model('Contact');
      Contact.find(function(err, contacts) {
        if (err) return reportError('loading document', err);
        console.info('contacts :', contacts);
        mongoose.disconnect(function(err) { // clean death
          if (err) return reportError('closing connection', err);
          mongodbFs.stop(function(err) {
            if (err) return reportError('stopping server', err);
            console.info('bye!');
          });
        });
      });
    });
});
