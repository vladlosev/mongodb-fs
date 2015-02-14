var path = require('path')
  , mongoose = require('mongoose')
  , mongodbFs = require('../lib/mongodb-fs');

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
    // 'warn' is default; specify 'info' or 'debug' to see more info.
    // Or you may specify the `logger` property us pass in your own logger.
    // But never, ever specify 'trace' here.
    level: 'warn'
  }
});

mongodbFs.start(function(err) {
  mongoose.connect('mongodb://localhost:27027/fakedb', {server: {poolSize: 1}}, function(err) {
    // Usual mongoose code to retreive all the contacts
    var Contact;
    Contact = mongoose.connection.model('Contact');
    Contact.find(function(err, contacts) {
      //
      console.log('contacts :', contacts);
      //
      mongoose.disconnect(function(err) { // clean death
        mongodbFs.stop(function(err) {
          console.log('bye!');
        });
      });
    });
  });
});
