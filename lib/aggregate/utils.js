'use strict';

function getErrorReply(errorMessage, code) {
  var error = {ok: false, errmsg: errorMessage};
  if (code) {
    error.code = code;
  }
  return {documents: [error]};
}

module.exports = {
  getErrorReply: getErrorReply
};
