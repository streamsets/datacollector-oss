/*

David Cramer
<david AT thingbag DOT net>

Kasun Gajasinghe
<kasunbg AT gmail DOT com>

Copyright © 2008-2012 Kasun Gajasinghe, David Cramer

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

1. The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

2. Except as contained in this notice, the names of individuals credited with contribution to this software shall not be used in advertising or otherwise to promote the sale, use or other dealings in this Software without prior written authorization from the individuals in question.

3. Any stylesheet derived from this Software that is publicly distributed will be identified with a different name and the version strings in any derived Software will be changed so that no possibility of confusion between the derived package and this Software will exist.

Warranty: THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL DAVID CRAMER, KASUN GAJASINGHE, OR ANY OTHER CONTRIBUTOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/


/*
  List of modifications added by the Oxygen Webhelp plugin:
   
  1. Make sure the space-separated words from the search query are 
     passed to the search function realSearch() in all cases (the 
	 total number of words can be less than or greater than 10 words).
	 
  2. Accept as valid search words a sequence of two words separated 
     by ':', '.' or '-'.
	 
  3. Convert the search query to lowercase before executing the search.
  
  4. Do not omit words between angle brackets from the title of the 
     search results.

  5. Normalize search results HREFs and add '#' for no-frames webhelp

  6. Keep custom footer in TOC after searching some text

*/

// Return true if "word1" starts with "word2"  
function startsWith(word1, word2) {
    var prefix = false;
    if (word1 !== null && word2 !== null) {
        if (word2.length <= word1.length) {
            prefix = true;
            for (var i = 0; i < word2.length; i++) {
              if (word1.charAt(i) !== word2.charAt(i)) {
                  prefix = false;
                  break;
              }
            }
        }
    } else {
        if (word1 !== null) {
            prefix = true;
        }
    }
    return prefix;
}


if (!("console" in window) || !("firebug" in console)) {
 var names = ["log", "debug", "info", "warn", "error", "assert", "dir", "dirxml", "group", "groupEnd", "time", "timeEnd", "count", "trace", "profile", "profileEnd"];
 window.console = {};
 for (var i = 0, len = names.length; i < len; ++i) {
 window.console[names[i]] = function(){};
 }
}

function logLocal(msg){
  console.log(msg);
}


if (typeof debug !== 'function') {
  function debug(msg,obj){
    if (top !== self){
      if (typeof parent.debug !== 'function') {
        logLocal(msg);
      }else{
        if (typeof msg!=="undefined"){
          if (typeof msg==="object"){
            parent.debug('['+src+']',msg);
          }else{
            parent.debug('['+src+']'+msg,obj);
          }
        }
      }
    }else{
      logLocal(msg);
    }
  }
}

var htmlfileList = "htmlFileList.js";
var htmlfileinfoList = "htmlFileInfoList.js";
var useCJKTokenizing = false;

var w = new Object();
var scoring = new Object();

var searchTextField = '';
var no = 0;
var noWords = 0;
var partialSearch1 = "There is no page containing all the search terms.";
var partialSearch2 = "Partial results:";
var warningMsg = '<div style="padding: 5px;margin-right:5px;;background-color:#FFFF00;">';
warningMsg+='<b>Please note that due to security settings, Google Chrome does not highlight';
warningMsg+=' the search results.</b><br>';
warningMsg+='This happens only when the WebHelp files are loaded from the local file system.<br>';
warningMsg+='Workarounds:';
warningMsg+='<ul>';
warningMsg+='<li>Try using another web browser.</li>';
warningMsg+='<li>Deploy the WebHelp files on a web server.</li>';
warningMsg+='</div>';
var txt_filesfound = 'Results';
var txt_enter_at_least_1_char = "You must enter at least one character.";
var txt_enter_more_than_10_words = "Only first 10 words will be processed.";
var txt_browser_not_supported = "Your browser is not supported. Use of Mozilla Firefox is recommended.";
var txt_please_wait = "Please wait. Search in progress...";
var txt_results_for = "Results for:";

/**
 * This function find the all matches using the search term
 */
function SearchToc(ditaSearch_Form) {
  debug('SearchToc(..)');
  //START - EXM-30790
  var footer = $("#searchResults .footer");
  //END - EXM-30790
  // Check browser compatibitily
  if (navigator.userAgent.indexOf("Konquerer") > -1) {
    alert(getLocalization(txt_browser_not_supported));
    return;
  }

  searchTextField = trim(document.searchForm.textToSearch.value);
  
  // Eliminate the cross site scripting possibility.
  searchTextField = searchTextField.replace(/</g, " ");
  searchTextField = searchTextField.replace(/>/g, " ");
  searchTextField = searchTextField.replace(/\"/g, " ");
  searchTextField = searchTextField.replace(/\'/g, " ");
  searchTextField = searchTextField.replace(/=/g, " ");
  searchTextField = searchTextField.replace(/\(/g, " ");
  searchTextField = searchTextField.replace(/\)/g, " ");
  searchTextField = searchTextField.replace(/0\\/g, " ");
  searchTextField = searchTextField.replace(/\\/g, " ");
  searchTextField = searchTextField.replace(/\//g, " ");
 
  var expressionInput = searchTextField;
  debug('Search for: '+expressionInput);

  if (expressionInput.length < 1) {
    // expression is invalid
    alert(getLocalization(txt_enter_at_least_1_char));
    document.searchForm.textToSearch.focus();
  }else {
    var splitSpace = searchTextField.split(" ");
    noWords = splitSpace;
    if (noWords.length > 9) {
      // Allow to search maximum 10 words
      alert(getLocalization(txt_enter_more_than_10_words));
      expressionInput = '';
      for (var x = 0 ; x < 10 ; x++){
        expressionInput = expressionInput + " " + noWords[x];
      }
      realSearch(expressionInput);
      document.searchForm.textToSearch.focus();
    } else {
          
      // START - EXM-20996
      expressionInput = '';
      for (var x = 0 ; x < noWords.length ; x++) {
        expressionInput = expressionInput + " " + noWords[x];
      }
      // END - EXM-20996
             
      realSearch(expressionInput);
      document.searchForm.textToSearch.focus();
    }
  }
  debug('End SearchToc(..)');

  // START - EXM-29420
  $('.searchresult li a').each(function () {
      var old = $(this).attr('href');
      var newHref = '#' + normalizeLink(old);
      /* If with frames */
      if (top != self) {
          newHref = normalizeLink(old);
      }
      if (old == 'javascript:void(0)') {
          $(this).attr('href', '#!_' + $(this).text());
      } else {
          $(this).attr('href', newHref);
          info('alter link:' + $(this).attr('href') + ' from ' + old);
      }
  });
  //END - EXM-29420
  //START - EXM-30790
  $("#searchResults").append(footer);
  //END - EXM-30790
}

var stemQueryMap = new Array();  // A hashtable which maps stems to query words

var scriptLetterTab=null;
/**
 * This function parses the search expression,
 * loads the indices and displays the results
 */
function realSearch(expressionInput) {
  debug('realSearch('+expressionInput+')');
  /**
   *data initialisation
   */
  scriptLetterTab = new Scriptfirstchar(); // Array containing the first letter of each word to look for
  var searchFor = "";       // expression in lowercase and without special caracters
  var wordsList = new Array(); // Array with the words to look for
  var finalWordsList = new Array(); // Array with the words to look for after removing spaces
  var linkTab = new Array();
  var fileAndWordList = new Array();
  var txt_wordsnotfound = "";

  /* EXM-26412 */
  expressionInput = expressionInput.toLowerCase();
  /*  START - EXM-20414 */
  searchFor = expressionInput.replace(/<\//g, "_st_").replace(/\$_/g, "_di_").replace(/%2C|%3B|%21|%3A|@|\/|\*/g, " ").replace(/(%20)+/g, " ").replace(/_st_/g, "</").replace(/_di_/g, "%24_");
  /*  END - EXM-20414 */

  searchFor = searchFor.replace(/  +/g, " ");
  searchFor = searchFor.replace(/ $/, "").replace(/^ /, "");

  wordsList = searchFor.split(" ");
  wordsList.sort();
  debug('words from search:',wordsList);
  //set the tokenizing method
  if(typeof indexerLanguage != "undefined" && (indexerLanguage=="zh" || indexerLanguage=="ja" ||indexerLanguage=="ko")){
    useCJKTokenizing=true;
  } else {
    useCJKTokenizing=false;
  }
  debug('useCJKTokenizing='+useCJKTokenizing);
  //If Lucene CJKTokenizer was used as the indexer, then useCJKTokenizing will be true. Else, do normal tokenizing.
  // 2-gram tokenizinghappens in CJKTokenizing,
  // If doStem then make tokenize with Stemmer
  var finalArray;
  if (doStem){
    debug('doStem='+doStem);
    if(useCJKTokenizing){
      finalWordsList = cjkTokenize(wordsList);
      finalArray = finalWordsList;
    } else {
      finalWordsList = tokenize(wordsList);
    }
  } else if(useCJKTokenizing){
    finalWordsList = cjkTokenize(wordsList);
    finalArray = finalWordsList;
   debug('CJKTokenizing, finalWordsList: ' + finalWordsList);
  }
  if(!useCJKTokenizing){
    /**
     * Compare with the indexed words (in the w[] array), and push words that are in it to tempTab.
     */
    var tempTab = new Array();
            
    var splitedValues = expressionInput.split(" ");
    finalWordsList = finalWordsList.concat(splitedValues);
    finalArray = finalWordsList;
    finalArray = removeDuplicate(finalArray);
    var wordsArray = '';
    for (var t in finalWordsList) {
      if (!contains(stopWords, finalWordsList[t])) {
          if (doStem){
            if (w[finalWordsList[t].toString()] == undefined) {
              txt_wordsnotfound += finalWordsList[t] + " ";
            } else {
              tempTab.push(finalWordsList[t]);
            }
          } else {
            var searchedValue = finalWordsList[t].toString();
            var listOfWordsStartWith = wordsStartsWith(searchedValue);
            if (listOfWordsStartWith != undefined) {
              wordsArray += listOfWordsStartWith;
            }
         }
      }
    }
    wordsArray = wordsArray.substr(0, wordsArray.length - 1);
    if (!doStem){
      finalWordsList = wordsArray.split(",");
    } else {
      finalWordsList = tempTab;
    }
    txt_wordsnotfound = expressionInput;
    finalWordsList = removeDuplicate(finalWordsList);
  }

  debug('finalWordsList  ' + finalWordsList);

  if (finalWordsList.length) {
    //search 'and' and 'or' one time
    fileAndWordList = SortResults(finalWordsList);
    var cpt = 0;
    if (fileAndWordList != undefined) {
      cpt = fileAndWordList.length;
      var maxNumberOfWords = fileAndWordList[0][0].motsnb;
    }
    if (cpt > 0) {
      var searchedWords = noWords.length;
      var foundedWords  = fileAndWordList[0][0].motslisteDisplay.split(",").length;
      
      // EXM-27607 START Check if a stop word was in search string.
      var stopWordsPresent = 0;
      for (var searchWord = 0; searchWord < wordsList.length; searchWord++) {
        for (var stopWord = 0; stopWord < stopWords.length; stopWord++) {
          if (wordsList[searchWord] == stopWords[stopWord]) {
              stopWordsPresent = 1;
              break;
          }
        }
      }
      // EXM-27607 STOP
      
      if ((searchedWords != foundedWords) && (stopWordsPresent == 0)) {
        linkTab.push("<font class=\"highlightText\">" + getLocalization(partialSearch1) + "<br>" + getLocalization(partialSearch2) + "</font>");
      }
    }
    
    for (var i = 0; i < cpt; i++) {
      var hundredProcent = fileAndWordList[i][0].scoring + 100 * fileAndWordList[i][0].motsnb;
      var ttScore_first = fileAndWordList[i][0].scoring;
      var numberOfWords = fileAndWordList[i][0].motsnb;
      if (fileAndWordList[i] != undefined) {
        linkTab.push("<p>" + getLocalization(txt_results_for) + " " + "<span class=\"searchExpression\">" + fileAndWordList[i][0].motslisteDisplay + "</span>" + "</p>");

        linkTab.push("<ul class='searchresult'>");
        for (t in fileAndWordList[i]) {
          var ttInfo = fileAndWordList[i][t].filenb;
          // Get scoring
          var ttScore = fileAndWordList[i][t].scoring;

          debug('score for'+t+' = '+ttScore);

          var tempInfo = fil[ttInfo];
          var pos1 = tempInfo.indexOf("@@@");
          var pos2 = tempInfo.lastIndexOf("@@@");
          var tempPath = tempInfo.substring(0, pos1);
          // EXM-27709 START
          // Display words between '<' and '>' in title of search results.
          var tempTitle = tempInfo.substring(pos1 + 3, pos2)
                .replace(/</g, "&lt;").replace(/>/g, "&gt;");
          // EXM-27709 END
          var tempShortdesc = tempInfo.substring(pos2 + 3, tempInfo.length);

          if (tempPath == 'toc.html'){
            continue;
          }
          var split = fileAndWordList[i][t].motsliste.split(",");
          arrayString = 'Array(';
          for(var x in finalArray){
            if (finalArray[x].length > 2 || useCJKTokenizing){
              arrayString+= "'" + finalArray[x] + "',";
            }
          }
          arrayString = arrayString.substring(0,arrayString.length - 1) + ")";
//          /*  START - EXM-20414 */
//          arrayString = arrayString.replace(".", "\\\\.");
//          /*  END - EXM-20414 */
          var idLink = 'foundLink' + no;
          var link = 'return openAndHighlight(\'' + tempPath + '\', ' + arrayString + '\)';
          var linkString = '<li><a id="' + idLink + '" href="' + tempPath + '" class="foundResult" onclick="'+link+'">' + tempTitle + '</a>';
          var starWidth = (ttScore * 100/ hundredProcent)/(ttScore_first/hundredProcent) * (numberOfWords/maxNumberOfWords);
          starWidth = starWidth < 10 ? (starWidth + 5) : starWidth;
          // Keep the 5 stars format
          if (starWidth > 85){
            starWidth = 85;
          }
          // Also check if we have a valid description
          if ((tempShortdesc != "null" && tempShortdesc != '...')) {
            linkString += "\n<div class=\"shortdesclink\">" + tempShortdesc + "</div>";
          }
                    
          linkString += "</li>";
          linkTab.push(linkString);
          no++;
        }
        linkTab.push("</ul>");
      }
    }
  }

  var results = "";
  if (linkTab.length > 0) {
    results = "<p>";
    for (t in linkTab) {
      results += linkTab[t].toString();
    }
    results += "</p>";
  } else {
    results = "<p>" + getLocalization("Search no results") + " " + "<span class=\"searchExpression\">" + txt_wordsnotfound + "</span>" + "</p>";
  }
    
  // Verify if the browser is Google Chrome and the WebHelp is used on a local machine
  // If browser is Google Chrome and WebHelp is used on a local machine a warning message will appear
  // Highlighting will not work in this conditions. There is 2 workarounds
  if (notLocalChrome){
    document.getElementById('searchResults').innerHTML = results;
  } else {
    document.getElementById('searchResults').innerHTML = warningMsg + results;
  }
  debug('end realSearch('+expressionInput+')');
}

// Return true if "word" value is an element of "arrayOfWords"  
function contains(arrayOfWords, word) {
    var found = true;
    if (word.length > 2) {
        found = false;
        for (var w in arrayOfWords) {
            if (arrayOfWords[w] === word) {
                found = true;
                break;
            }
        }
    }
    return found;
}

// Look for elements that start with searchedValue.
function wordsStartsWith(searchedValue){
  var toReturn = '';
  for (var sv in w) {
      if (sv.toLowerCase().indexOf(searchedValue.toLowerCase()) == 0){
          toReturn += sv + ","; 
    }
  }
  return toReturn.length > 0 ? toReturn : undefined;
}

function tokenize(wordsList){
  debug('tokenize('+wordsList+')');
  var stemmedWordsList = new Array(); // Array with the words to look for after removing spaces
  var cleanwordsList = new Array(); // Array with the words to look for
  for(var j in wordsList){
    var word = wordsList[j];
    if(typeof stemmer != "undefined" && doStem){
      stemQueryMap[stemmer(word)] = word;
    } else {
      stemQueryMap[word] = word;
    }
  }
  debug('scriptLetterTab in tokenize: ', scriptLetterTab);
  scriptLetterTab.add('s');
  //stemmedWordsList is the stemmed list of words separated by spaces.
  for (var t in wordsList) {
    wordsList[t] = wordsList[t].replace(/(%22)|^-/g, "");
    if (wordsList[t] != "%20") {
      var firstChar=wordsList[t].charAt(0);
      scriptLetterTab.add(firstChar);
      cleanwordsList.push(wordsList[t]);
    }
  }

  if(typeof stemmer != "undefined" && doStem){
    //Do the stemming using Porter's stemming algorithm
    for (var i = 0; i < cleanwordsList.length; i++) {
      var stemWord = stemmer(cleanwordsList[i]);
      stemmedWordsList.push(stemWord);
    }
  } else {
    stemmedWordsList = cleanwordsList;
  }
  return stemmedWordsList;
}

//Invoker of CJKTokenizer class methods.
function cjkTokenize(wordsList){
  var allTokens= new Array();
  var notCJKTokens= new Array();
  var j=0;
  for(j=0;j<wordsList.length;j++){
    var word = wordsList[j];
    if(getAvgAsciiValue(word) < 127){
      notCJKTokens.push(word);
    } else {
      var tokenizer = new CJKTokenizer(word);
      var tokensTmp = tokenizer.getAllTokens();
      allTokens = allTokens.concat(tokensTmp);
    }
  }
  allTokens = allTokens.concat(tokenize(notCJKTokens));
  return allTokens;
}

//A simple way to determine whether the query is in english or not.
function getAvgAsciiValue(word){
  var tmp = 0;
  var num = word.length < 5 ? word.length:5;
  for(var i=0;i<num;i++){
    if(i==5) break;
    tmp += word.charCodeAt(i);
  }
  return tmp/num;
}

//CJKTokenizer
function CJKTokenizer(input){
  this.input = input;
  this.offset=-1;
  this.tokens = new Array();
  this.incrementToken = incrementToken;
  this.tokenize = tokenize;
  this.getAllTokens = getAllTokens;
  this.unique = unique;

  function incrementToken(){
    if(this.input.length - 2 <= this.offset){
      return false;
    }
    else {
      this.offset+=1;
      return true;
    }
  }

  function tokenize(){
    return this.input.substring(this.offset,this.offset+2);
  }

  function getAllTokens(){
    while(this.incrementToken()){
      var tmp = this.tokenize();
      this.tokens.push(tmp);
    }
    return this.unique(this.tokens);
  }

  function unique(a){
    var r = new Array();
      o:for(var i = 0, n = a.length; i < n; i++){
        for(var x = 0, y = r.length; x < y; x++){
          if(r[x]==a[i]) continue o;
        }
        r[r.length] = a[i];
      }
    return r;
  } 
}


/* Scriptfirstchar: to gather the first letter of index js files to upload */
function Scriptfirstchar() {
  this.strLetters = "";
}

Scriptfirstchar.prototype.add=function(caract) {
  if (typeof this.strLetters == 'undefined') {
    this.strLetters = caract;
  } else if (this.strLetters.indexOf(caract) < 0) {
    this.strLetters += caract;
  }

  return 0;
}

/* end of scriptfirstchar */


// Array.unique( strict ) - Remove duplicate values
function unique(tab) {
  var a = new Array();
  var i;
  var l = tab.length;

  if (tab[0] != undefined) {
    a[0] = tab[0];
  }
  else {
    return -1;
  }

  for (i = 1; i < l; i++) {
    if (indexof(a, tab[i], 0) < 0) {
      a.push(tab[i]);
    }
  }
  return a;
}
function indexof(tab, element, begin) {
  for (var i = begin; i < tab.length; i++) {
    if (tab[i] == element) {
      return i;
    }
  }
  return -1;

}
/* end of Array functions */


/**
 * This function creates an hashtable:
 *  - The key is the index of a html file which contains a word to look for.
 *  - The value is the list of all words contained in the html file.
 *
 * @param words - list of words to look for.
 * @return the hashtable fileAndWordList
 */
function SortResults(words) {

  var fileAndWordList = new Object();
  if (words.length == 0 || words[0].length == 0) {
    return null;
  }
    
  // In generated js file we add scoring at the end of the word
  // Example word1*scoringForWord1,word2*scoringForWord2 and so on
  // Split after * to obtain the right values
  var scoringArr = Array();
  for (var t in words) {
    // get the list of the indices of the files.
    var listNumerosDesFicStr = w[words[t].toString()];
    var tab = listNumerosDesFicStr.split(",");
    //for each file (file's index):
    for (var t2 in tab) {
      var tmp = '';
      var idx = '';
      var temp = tab[t2].toString();
      if (temp.indexOf('*') != -1){
        idx = temp.indexOf('*');
        tmp = temp.substring(idx + 3, temp.length);
        temp = temp.substring(0,idx);
      }
      scoringArr.push(tmp);
      if (fileAndWordList[temp] == undefined) {
        fileAndWordList[temp] = "" + words[t];
      } else {
        fileAndWordList[temp] += "," + words[t];
      }
    }
  }
  var fileAndWordListValuesOnly = new Array();
  // sort results according to values
  var temptab = new Array();
  finalObj = new Array();
  for (t in fileAndWordList) {
    finalObj.push(new newObj(t,fileAndWordList[t]));
  }
  finalObj = removeDerivates(finalObj);
  for (t in finalObj) {
    tab = finalObj[t].wordList.split(',');
    var tempDisplay = new Array();
    for (var x in tab) {
      if(stemQueryMap[tab[x]] != undefined && doStem){
        tempDisplay.push(stemQueryMap[tab[x]]); //get the original word from the stem word.
      } else {
        tempDisplay.push(tab[x]); //no stem is available. (probably a CJK language)
      }
    }
    var tempDispString = tempDisplay.join(", ");
    var index;
    for (x in fileAndWordList) {
      if (x === finalObj[t].filesNo) {
        index = x;
        break;
      }
    }
    var scoring = findRating(fileAndWordList[index], index);
    temptab.push(new resultPerFile(finalObj[t].filesNo, finalObj[t].wordList, tab.length, tempDispString, scoring));
    fileAndWordListValuesOnly.push(finalObj[t].wordList);
  }
  fileAndWordListValuesOnly = unique(fileAndWordListValuesOnly);
  fileAndWordListValuesOnly = fileAndWordListValuesOnly.sort(compareWords);

  var listToOutput = new Array();
  for (var j in fileAndWordListValuesOnly) {
    for (t in temptab) {
      if (temptab[t].motsliste == fileAndWordListValuesOnly[j]) {
        if (listToOutput[j] == undefined) {
          listToOutput[j] = new Array(temptab[t]);
        } else {
          listToOutput[j].push(temptab[t]);
        }
      }
    }
  }
  // Sort results by scoring, descending on the same group
  for (var i in listToOutput) {
    listToOutput[i].sort(function(a, b){
      return b.scoring - a.scoring;
    });
  }
  // If we have groups with same number of words, 
  // will sort groups by higher scoring of each group
  for (var i = 0; i < listToOutput.length - 1; i++) {
    for (var j = i + 1; j < listToOutput.length; j++) {
      if (listToOutput[i][0].motsnb < listToOutput[j][0].motsnb 
        || (listToOutput[i][0].motsnb == listToOutput[j][0].motsnb
          && listToOutput[i][0].scoring < listToOutput[j][0].scoring)
        ) {
        var x = listToOutput[i];
        listToOutput[i] = listToOutput[j];
        listToOutput[j] = x;
      }
    }
  }

  return listToOutput;
}

// Remove derivates words from the list of words
function removeDerivates(obj){
  var toResultObject = new Array(); 
  for (i in obj){
    var filesNo  = obj[i].filesNo;
    var wordList = obj[i].wordList;
    var wList = wordList.split(",");    
    var searchedWords = searchTextField.toLowerCase().split(" ");
    for (var k = 0 ; k < searchedWords.length ; k++){
      for (var j = 0 ; j < wList.length ; j++){       
        if (startsWith(wList[j], searchedWords[k])){
          wList[j] = searchedWords[k];
        }
      }
    }
    wList = removeDuplicate(wList);
    var recreateList = '';
    for(var x in wList){
      recreateList+=wList[x] + ",";
    }
    recreateList = recreateList.substr(0, recreateList.length - 1);
    toResultObject.push(new newObj(filesNo, recreateList));
  }
  return toResultObject;
}

function newObj(filesNo, wordList){
  this.filesNo = filesNo;
  this.wordList = wordList;
}


// Object.
// Add a new parameter - scoring.
function resultPerFile(filenb, motsliste, motsnb, motslisteDisplay, scoring, group) {
  //10 - spring,time - 2 - spring, time - 55 - 3
  this.filenb = filenb;
  this.motsliste = motsliste;
  this.motsnb = motsnb;
  this.motslisteDisplay= motslisteDisplay;
  this.scoring = scoring;
}

function findRating(words, nr){
  var sum = 0;
  var xx = words.split(',');
  for (var jj = 0 ; jj < xx.length ; jj++){
    var wrd = w[xx[jj]].split(',');
    for (var ii = 0 ; ii < wrd.length ; ii++){
      var wrdno = wrd[ii].split('*');
      if (wrdno[0] == nr){
        sum+=parseInt(wrdno[1]);
      }
    }
  }
  return sum;
}

function compareWords(s1, s2) {
  var t1 = s1.split(',');
  var t2 = s2.split(',');
  if (t1.length == t2.length) {
    return 0;
  } else if (t1.length > t2.length) {
    return 1;
  } else {
    return -1;
  }
}
// return false if browser is Google Chrome and WebHelp is used on a local machine, not a web server 
function verifyBrowser(){
  var returnedValue = true;
  BrowserDetect.init();
  var browser = BrowserDetect.browser;
  var addressBar = window.location.href;
  if (browser == 'Chrome' && addressBar.indexOf('file://') === 0){
    returnedValue = false;
  }
    
  return returnedValue;
}

// Remove duplicate values from an array
function removeDuplicate(arr) {
  var r = new Array();
    o:for(var i = 0, n = arr.length; i < n; i++) {
      for(var x = 0, y = r.length; x < y; x++) {
        if(r[x]==arr[i]) continue o;
      }
      r[r.length] = arr[i];
    }
  return r;
}

function trim(str, chars) {
  return ltrim(rtrim(str, chars), chars);
}
 
function ltrim(str, chars) {
  chars = chars || "\\s";
  return str.replace(new RegExp("^[" + chars + "]+", "g"), "");
}
 
function rtrim(str, chars) {
  chars = chars || "\\s";
  return str.replace(new RegExp("[" + chars + "]+$", "g"), "");
}