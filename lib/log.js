var util = require('util');
var winston = require('winston');

module.exports = {
  init: function(config) {
    this.config = config || {};
  },

  getLogger: function() {
    if (this.config.logger) {
      return this.config.logger;
    } else {
      var logger = new winston.Logger({
        transports: [
          new winston.transports.Console({
            level: this.config.level || 'warn',
            formatter: function(options) {
              return util.format(
                '[%s] %s mongodb-fs %s',
                new Date().toJSON(),
                options.level.toUpperCase(),
                options.message || '');
            }
          })
        ],
        levels: {
          trace: 0,
          debug: 1,
          info:  2,
          warn:  3,
          error: 4
        }
      });
      return logger;
    }
  }
};
