var util = require('util');
var log4js = require('log4js');

var consoleAppender;

module.exports = {
  init: function(config) {
    this.config = config || {};
    if (!this.config.logger) {
      this.category = this.config.category || 'mongodb-fs.';
      this.level = this.config.level;
    }
  },

  getLogger: function(category) {
    if (this.config.logger) {
      return this.config.logger;
    } else {
      if (/[.]$/.test(this.category)) {
        if (category) {
          category = this.category + category;
        } else {
          category = this.category.substring(0, this.category.length - 1);
        }
      } else {
        category = this.category;
      }
      var logger = log4js.getLogger(category);
      logger.setLevel(this.level);
      return logger;
    }
  }
};
