'use strict';

var gulp         = require('gulp');
var plugins      = require('gulp-load-plugins')();

var SRC = {
  LIB:   ['lib/**/*.js'],
  TESTS: ['test/**/*.js', 'test/*.js'],
  BIN:   ['bin/*.js']
};

gulp.task('lint', function() {
  return gulp.src([].concat(SRC.LIB, SRC.TESTS, SRC.BIN))
    .pipe(plugins.eslint({configFile: '.eslintrc.json'}))
    .pipe(plugins.eslint.format())
    .pipe(plugins.eslint.failOnError());
});

gulp.task('watch', function() {
  gulp.watch([].concat(SRC.LIB, SRC.TESTS), ['server-lint']);
});

gulp.task('default',  ['lint']);
