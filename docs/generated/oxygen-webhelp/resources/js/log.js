/*

Oxygen WebHelp Plugin
Copyright (c) 1998-2016 Syncro Soft SRL, Romania.  All rights reserved.

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
  this.queue= new Array(); 
  this.terms= new Array(); 
  this.level=level;  
  this.INFO=0;
  this.DEBUG=1;
  this.WARN=2;
  this.ERROR=3;
  this.flushDebug=null;
  this.debugWindow = null;  
  this.joinLogs=null;
}
function joinLogs(){
  $.ajax({
    type : "POST",
    url : "./oxygen-webhelp/resources/php/joinLogs.php",
    data : "",
    success : function(data_response) {
      if(data_response!=""){
        error("[PHP]: "+data_response);			
      }
    }
  });
}

Log.prototype.setLevel = function(level){   
  this.level=level;
  if (level<=this.ERROR){
    this.openDebugWindow();  
    this.flushDebug=setTimeout('list()', 3000);
    this.joinLogs=setInterval("joinLogs",3000);
  }else{
    if (this.joinLogs!==null){
      clearInterval(this.joinLogs);
    }
  }
}
Log.prototype.debug = function(msg,obj){   
  if (this.level<=this.DEBUG){      
    this.add(this.DEBUG,msg,obj);
  }
}
Log.prototype.info = function(msg,obj){ 
  if (this.level<=this.INFO){             
    this.add(this.INFO,msg,obj);
  }
}
Log.prototype.warn = function(msg,obj){ 
  if (this.level<=this.WARN){             
    this.add(this.WARN,msg,obj);
  }
}
Log.prototype.error = function(msg,obj){ 
  if (this.level<=this.ERROR){             
    this.add(this.ERROR,msg,obj);
  }
}

Log.prototype.openDebugWindow = function() {
  //  var url = "./debug.html";
  this.debugWindow = open( '', 'oXygenWHDebug', "left=50, width=700,height=400,titlebar=0, resizable=1, location=0,status=yes,toolbar=no,scrollbars=yes,menubar=no" );
  if( !this.debugWindow || this.debugWindow.closed || !this.debugWindow.writeDebug ) {
    this.debugWindow = window.open( '', 'oXygenWHDebug', "left=50,width=600,height=400,titlebar=0,resizable=1, location=0,status=yes,toolbar=no,scrollbars=yes,menubar=no" );
    this.debugWindow.document.write('<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\n\
      <html>\n\
<head>\n\
<title>Debug window for oXygen Web Help</title>\n\
<script type="text/javascript" src="./oxygen-webhelp/resources/js/jquery-1.11.3.min.js"> </script>\n\
<style type="text/css">\n\
#debug{\n\
width: 100%;\n\
height: 100%; \n\
background: white;\n\
font-size: 0.9em;\n\
font-family: monospace, arial, helvetica, sans-serif;\n\
font-size: 10pt;}\n\
.row:hover{\n\
background: #ffffcc;\n\
}\n\
.row{\n\
background: white;\n\
font-size: 0.9em;\n\
}\n\
.mark{\n\
background: #ffff99;\n\
font-size: 0.9em;\n\
}\n\
.log{}\n\
.logInfo{color:gray;}\n\
.logDebug{color:black;}\n\
.logWarn{color:orange;}\n\
.logError{color:red;}</style>\n\
</head>\n\
<body>\n\
<script type="text/javascript">\n\
self.focus();\n\
function selectDebugText() {\n\
if (document.selection) {\n\
var range = document.body.createTextRange();\n\
range.moveToElementText(document.getElementById("debug"));\n\
range.select();\n\
} else if (window.getSelection) {\n\
var range = document.createRange();\n\
range.selectNode(document.getElementById("debug"));\n\
window.getSelection().addRange(range);\n\
}\n\
}\n\
function writeDebug(msg,classDiv) {\n\
var div = document.createElement("DIV");\n\
var divClass="row";\n\
if (typeof classDiv === "string" ){\n\
  divClass=classDiv;\n\
}\n\
if (msg.indexOf($("#markTerm").val())>0 && $("#markTerm").val().length>0){\n\
    divClass="row mark";\n\
}\n\
div.setAttribute("class", divClass); \n\
div.innerHTML = msg;\n\
document.getElementById("debug").appendChild(div);\n\
div.scrollIntoView(true);\n\
}\n\
function clearDiv(){\n\
document.getElementById("debug").innerHTML="";\n\
return false;\n\
}\n\
function infoT(){\n\
if ($("#ck_info").is(":checked")){\n\
  $("#debug .logInfo").show();\n\
}else{\n\
  $("#debug .logInfo").hide();\n\
}\n\
}\n\
function debugT(){\n\
if ($("#ck_debug").is(":checked")){\n\
  $("#debug .logDebug").show();\n\
}else{\n\
  $("#debug .logDebug").hide();\n\
}\n\
}\n\
function warnT(){\n\
if ($("#ck_warn").is(":checked")){\n\
  $("#debug .logWarn").show();\n\
}else{\n\
  $("#debug .logWarn").hide();\n\
}\n\
}\n\
function errorT(){\n\
if ($("#ck_error").is(":checked")){\n\
  $("#debug .logError").show();\n\
}else{\n\
  $("#debug .logError").hide();\n\
}\n\
}</script>\n\
<div style="position:fixed; width:100%; top:0px; right:0px; background: #ddd; border-bottom:2px solid #ccc;">\n\
<input type="button" value="Select All" onclick="selectDebugText();">\n\
<input type="button" value="Clear" onclick="clearDiv();">\n\
<input type="checkbox" checked="checked"  onclick="infoT();" id="ck_info"><span class="logInfo">Info</span>\n\
<input type="checkbox" checked="checked"  onclick="debugT();" id="ck_debug"><span class="logDebug">Debug</span>\n\
<input type="checkbox" checked="checked"  onclick="warnT();" id="ck_warn"><span class="logWarn">Warn</span>\n\
<input type="checkbox" checked="checked"  onclick="errorT();" id="ck_error"><span class="logError">Error</span>\n\
&nbsp; highlight &nbsp;<input type="text" id="markTerm">\n\
</div>\n\
<div id="debug">\n\
<u>oXygen XML Web Help debug window</u><br>\n\
</div>\n\
</body>\n\
</html>');
    this.debugWindow.focus();
    this.debugWindow.creator=self;   
  } else this.debugWindow.focus();
}
function list(){
  log.list();
}
window.onerror = function (msg, url, line) {
  log.error("[JS]: "+msg+" in page: "+url+" al line: "+ line);   
  return true;
}     

if (log.level<=Level.ERROR){    
  log.openDebugWindow();  
  log.flushDebug=setTimeout('list()', 3000);  
  this.joinLogs=setInterval("joinLogs",3000);
}else{
  if (this.joinLogs!==null){
    clearInterval(this.joinLogs);
  }
}

Log.prototype.highlightTerm = function(term) {  
  this.terms.push(term);
};

Log.prototype.isToBeHighlight = function(txt){  
  var toReturn=false;
  for (var i=0;i<this.terms.length;i++){
    if (txt.indexOf(this.terms[i])>0){
      toReturn=true;
      break;
    }
  }
  return toReturn;
}
Log.prototype.list = function() {  
  clearTimeout(this.flushDebug);
  if(this.debugWindow && !this.debugWindow.closed && this.debugWindow.writeDebug){      
    for(var i=0; i<this.queue.length;i++){
      var line=this.queue.shift();    
      if (this.isToBeHighlight(line)){        
        this.debugWindow.writeDebug(line,"mark");
      }else{
        this.debugWindow.writeDebug(line);
    }  
    }  
  }else{
    this.flushDebug=setTimeout('list()', 3000);
  }
};
Log.prototype.add = function(type,msg,obj) {  
  var date = new Date();
  var dateStr=date.getFullYear();
  dateStr=dateStr+":"+date.getMonth();
  dateStr=dateStr+":"+date.getDate();
  dateStr=dateStr+" "+date.getHours();
  dateStr=dateStr+":"+date.getMinutes();
  dateStr=dateStr+":"+date.getSeconds();
  dateStr=dateStr+".";
  var ms=date.getMilliseconds();
  if (ms<10){
    dateStr=dateStr+"00";
  }else if (ms<100){
    dateStr=dateStr+"0";
  }
  dateStr=dateStr+ms;  
  var logMsg=type;
  switch(type){
    case this.INFO:
      logMsg='<span class="logInfo">'+dateStr+' INFO ';
    break;
    case this.DEBUG:
      logMsg='<span class="logDebug">'+dateStr+' DEBUG ';
    break;
    case this.WARN:
      logMsg='<span class="logWarn">'+dateStr+' WARN ';
    break;
    case this.ERROR:
      logMsg='<span class="logError">'+dateStr+' ERROR ';
    break;    
    default:
      logMsg='<span class="log">'+dateStr+' ';
  }
  if (typeof msg!=="undefined"){
    if (typeof msg==="object"){
      logMsg=logMsg+" "+this.objStr(msg);    
    }else{
      logMsg=logMsg+" "+msg;  
    }
  }
  if (typeof obj!=="undefined"){
    if (typeof obj==="object"){
      logMsg=logMsg+" "+this.objStr(obj);    
    }else{
      logMsg=logMsg+" "+obj;  
    }
  }  
  logMsg=logMsg+"</span>";
  this.queue.push(logMsg);   
  this.list();
  
};
Log.prototype.isEnabled= function(){ 
  return this.level>=this.INFO;
}
Log.prototype.objStr = function(obj){  
  var keys = "";
  if (typeof obj !== "object"){
    keys=obj;
  }else{
    for(var key in obj){     
      keys=keys+';'+key+'='+obj[key];
    }
    keys='['+keys.substr(1)+']';
  }
  return keys;
}

// Call stack code
function showCallStack(last){
  var f=showCallStack,result="Call stack:\n";
  if (typeof last ==='undefined'){
    last=1;
  }
  var deep=0;
  while((f=f.caller)!==null && deep<=last){
    deep++;
    var sFunctionName = f.toString().match(/^function (\w+)\(/)
    sFunctionName = (sFunctionName) ? sFunctionName[1] : 'anonymous function';
    result += sFunctionName;
    if (sFunctionName!='done'){
      result += getArguments(f.toString(), f.arguments);
    }
    result += "\n";

  }
  return result;
}


function getArguments(sFunction, a) {
  var i = sFunction.indexOf(' ');
  var ii = sFunction.indexOf('(');
  var iii = sFunction.indexOf(')');
  var aArgs = sFunction.substr(ii+1, iii-ii-1).split(',')
  var sArgs = '';
  for(var i=0; i<a.length; i++) {
    var q = ('string' == typeof a[i]) ? '"' : '';
    sArgs+=((i>0) ? ', ' : '')+(typeof a[i])+' '+aArgs[i]+':'+q+a[i]+q+'';
  }
  return '('+sArgs+')';
}