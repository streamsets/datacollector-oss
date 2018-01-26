/*

Oxygen WebHelp Plugin
Copyright (c) 1998-2017 Syncro Soft SRL, Romania.  All rights reserved.

*/

var Level ={
  'NONE':10,
  'INFO':0,
  'DEBUG':1,
  'WARN':2,
  'ERROR':3
};
/**
 * Enable console by specifying logging level as defined above
 * Level.NONE - no message will be logged
 * Level.INFO - 'info' message and above will be logged
 * Level.DEBUG - only 'debug' messages and above will be logged
 * Level.WARN - only 'warn' messages and above will be logged
 * Level.ERROR - only 'error' messages will be logged
 * 
 */ 
var log= new Log(Level.NONE);

/**
 * debug message queue object
 */
function Log(level) {
  this.level = level;
}

Log.prototype.setLevel = function(level) {
  this.level = level;
};

Log.prototype.debug = function(msg, obj) {
  if (this.level == Level.NONE || this.level > Level.DEBUG)
    return;
  if (typeof obj != 'undefined' && this.level == 1) {
    console.log(msg, obj);
  } else {
    console.log(msg);
  }
};

Log.prototype.info = function(msg, obj) {
  if (this.level == Level.NONE || this.level > Level.INFO)
    return;
  if (typeof obj != 'undefined') {
    console.info(msg, obj);
  } else {
    console.info(msg);
  }
};

Log.prototype.error = function(msg, obj) {
  if (this.level == Level.NONE || this.level > Level.ERROR)
    return;
  if (typeof obj != 'undefined') {
    console.error(msg, obj);
  } else {
    console.error(msg);
  }
};

Log.prototype.warn = function(msg, obj) {
  if (this.level == Level.NONE || this.level > Level.WARN)
    return;
  if (typeof obj != 'undefined' && this.level == 2) {
    console.warn(msg, obj);
  } else {
    console.warn(msg);
  }
};

/**
 * Debug functions
 */
function debug(msg, obj) {
  log.debug(msg, obj);
}

function info(msg, obj) {
  log.info(msg, obj);
}

function error(msg, obj) {
  log.error(msg, obj);
}

function warn(msg, obj) {
  log.warn(msg, obj);
}