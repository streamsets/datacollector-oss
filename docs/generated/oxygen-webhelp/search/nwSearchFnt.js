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

 7. Accept as valid search words that contains only 2 characters

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
        window.console[names[i]] = function () {
        };
    }
}

function logLocal(msg){
  console.log(msg);
}

if (typeof debug !== 'function') {
    function debug(msg, obj) {
        if ( withFrames ){
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

var useCJKTokenizing = false;

var w = {};

var searchTextField = '';
var no = 0;
var noWords = [];
var partialSearch2 = "excluded.terms";
var warningMsg = '<div style="padding: 5px;margin-right:5px;;background-color:#FFFF00;">';
warningMsg += '<b>Please note that due to security settings, Google Chrome does not highlight';
warningMsg += ' the search results.</b><br>';
warningMsg += 'This happens only when the WebHelp files are loaded from the local file system.<br>';
warningMsg += 'Workarounds:';
warningMsg += '<ul>';
warningMsg += '<li>Try using another web browser.</li>';
warningMsg += '<li>Deploy the WebHelp files on a web server.</li>';
warningMsg += '</div>';
var txt_filesfound = 'Results';
var txt_enter_at_least_1_char = "Search string can't be void";
var txt_enter_at_least_2_char = "Search string must have at least two characters";
var txt_enter_more_than_10_words = "Only first 10 words will be processed.";
var txt_browser_not_supported = "Your browser is not supported. Use of Mozilla Firefox is recommended.";
var txt_please_wait = "Please wait. Search in progress...";
var txt_results_for = "Results for:";

var result=new Pages([]);
var excluded=[];
var realSearchQuery;

var defaultOperator = "or";
var stackResults = [];

/**
 * @description This function make a search request. If all necessary resources are loaded search occurs
 *              otherwise search will be delayed until all resources are loaded.
 */
function searchRequest() {
    $('#search').trigger('click');
    var ditaSearch_Form = document.getElementById('searchForm');
    var ready = setInterval(function () {
        if (searchLoaded) {
            $('#loadingError').remove();
            SearchToc(ditaSearch_Form);
            clearInterval(ready);
        } else {
            if ($('#loadingError').length < 1) {
                $('#searchResults').prepend('<span id="loadingError">' + getLocalization('Loading, please wait ...') + '</span>');
            }
        }
    }, 100);
}

/**
 * List of all known operators
 * @type {string[]}
 */
var knownOperators = ["and", "or", "not"];

/**
 * @description Execute the search and display results after splitting a given query into different arrays that will contain search terms and
 * operators
 * @param searchQuery
 */
function searchAndDisplayResults(searchQuery) {
    var searchTerms = searchQuery.toLowerCase().split(" ");
    var toSearch = [];
    var operators = [];
    for (var i=0;i<searchTerms.length;i++) {
        if (!inArray(searchTerms[i], knownOperators)) {
            toSearch.push(searchTerms[i]);
        } else {
            operators.push(searchTerms[i]);
        }
    }

    searchQuery = normalizeQuery(searchQuery);

    computeResults(searchQuery);

    displayResults(stackResults[0]);
}

/**
 * @description Normalize query so that we have an operator between each two adjacent search terms. We'll add the defaultOperator if the
 * operator is missing.
 * e.g: If the defaultOperator is "and" the "iris flower" query will be "iris and flower"
 * @param query Search query
 * @return {string} Normalized query
 */
function normalizeQuery(query) {
    debug("normalizeQuery("+query+")");

    query = query.trim();
    query = query.replace(/\( /g, "(");
    query = query.replace(/ \)/g, ")");
    query = query.replace(/ -/g, "-");
    query = query.replace(/- /g, "-");
    query = query.replace(/-/g, " and ");
    query = query.replace(/ and or /g, " or ");
    query = query.replace(/^not /g, "");
    query = query.replace(/ or and /g, " or ");
    var regex =  " " + defaultOperator + " " + defaultOperator + " ";
    query = query.replace(new RegExp(regex,"g"), " " + defaultOperator + " ");
    query = query.replace(/ not and /g, " not ");
    query = query.replace(/ and not /g, " not ");
    query = query.replace(/ or not /g, " not ");

    query = query.trim();
    query = query.indexOf("or ")==0 ? query.substring(3) : query;
    query = query.indexOf("and ")==0 ? query.substring(4) : query;
    query = query.indexOf("not ")==0 ? query.substring(4) : query;

    query = (query.lastIndexOf(" or")==query.length-3 && query.lastIndexOf(" or")!=-1) ? query.substring(0,query.lastIndexOf(" or")) : query;
    query = (query.lastIndexOf(" and")==query.length-4 && query.lastIndexOf(" and")!=-1) ? query.substring(0,query.lastIndexOf(" and")) : query;
    query = (query.lastIndexOf(" not")==query.length-4 && query.lastIndexOf(" not")!=-1) ? query.substring(0,query.lastIndexOf(" not")) : query;

    var result = "";

    try {
        result = query.toLowerCase().trim().replace(/ /g, " " + defaultOperator + " ");

        result = result.replace(/ or and or /g, " and ");
        result = result.replace(/ or not or /g, " not ");
        result = result.replace(/ or or or /g, " or ");
        result = result.replace(/ and and and /g, " and ");

        result = result.replace(/ and or /g, " or ");
        result = result.replace(/ not or /g, " not ");
        result = result.replace(/ or not /g, " not ");
        result = result.replace(/ or and /g, " or ");

        result = result.replace(/ not and /g, " not ");
        result = result.replace(/ and not /g, " not ");
    } catch (e) {
        debug(e);
    }

    return result;
}

/**
 * @description Convert search expression from infix notation to reverse polish notation (RPN): iris and flower => iris flower and
 * @param {string} search Search expression to be converted. e.g.: iris and flower or (gerbera not salvia)
 */
function computeResults(search) {
    debug("computeResults("+search+")");
    var stringToStore="";
    var stack=[];
    var item = "";
    var items = [];
    for (var i=0;i<search.length;i++) {
        if (search[i]!=" " && search[i]!="(" && search[i]!=")") {
            item+=search[i];
        }
        if (search[i]==" ") {
            if (item!="") {
                items.push(item);
                item = "";
            }
        }
        if (search[i]=="(") {
            if (item!="") {
                items.push(item);
                items.push("(");
                item = "";
            } else {
                items.push("(");
            }
        }
        if (search[i]==")") {
            if (item!="") {
                items.push(item);
                items.push(")");
                item = "";
            } else {
                items.push(")");
            }
        }
    }

    if (item!="") {
        items.push(item);
        item="";
    }

    for (i=0;i<items.length;i++) {
        if (isTerm(items[i])) {
            stringToStore+=items[i] + " ";
        }
        if (inArray(items[i], knownOperators)) {
            while (stack.length>0 && inArray(stack[stack.length-1],knownOperators)) {
                stringToStore+=stack.pop() + " ";
            }
            stack.push(items[i]);
        } else if (items[i]=="(") {
            stack.push(items[i]);
        } else if (items[i]==")") {
            var popped = stack.pop();
            while (popped!="(") {
                stringToStore+=popped + " ";
                popped = stack.pop();
            }
        }
    }

    while (stack.length > 0) {
        stringToStore+=stack.pop() + " ";
    }

    calculateRPN(stringToStore);
}

/**
 * @description Compute results from a RPN expression
 * @param {string} rpn Expression in Reverse Polish notation
 */
function calculateRPN(rpn) {
    debug("calculate("+rpn+")");
    var lastResult;
    var rpnTokens = trim(rpn);
    rpnTokens = rpnTokens.split(' ');
    var result;
    for(var i=0;i<rpnTokens.length;i++) {
        var token = rpnTokens[i];

        if (isTerm(token)) {
            result = realSearch(token);

            if (result.length > 0) {
                stackResults.push(new Pages(result[0]));
            } else {
                stackResults.push(new Pages([]));
            }
        } else {
            switch (token) {
                case "and":
                    lastResult = stackResults.pop();
                    if (lastResult.value !== undefined) {
                        stackResults.push(stackResults.pop().and(lastResult));
                    }
                    break;
                case "or":
                    lastResult = stackResults.pop();
                    if (lastResult.value !== undefined) {
                        stackResults.push(stackResults.pop().or(lastResult));
                    }
                    break;
                case "not":
                    lastResult = stackResults.pop();
                    if (lastResult.value !== undefined) {
                        stackResults.push(stackResults.pop().not(lastResult));
                    }
                    break;
                default:
                    debug("Error in calculateRPN(string) Method!");
                    break;
            }
        }
    }
}

/**
 * @description Tests if a given string is a valid search term or not
 * @param {string} string String to look for in the known operators list
 * @return {boolean} TRUE if the search string is a search term
 *                   FALSE if the search string is not a search term
 */
function isTerm(string) {
    return !inArray(string, knownOperators) && string.indexOf("(") == -1 && string.indexOf(")") == -1;
}

/**
 * @description Search for an element into an array
 * @param needle Searched element
 * @param haystack Array of elements
 * @return {boolean} TRUE if the searched element is part of the array
 *                   FALSE otherwise
 */
function inArray(needle, haystack) {
    var length = haystack.length;
    for(var i = 0; i < length; i++) {
        if(haystack[i] == needle) return true;
    }

    return false;
}

/**
 * @description This function find all matches using the search term
 * @param {HTMLObjectElement} ditaSearch_Form The search form from WebHelp page as HTML Object
 */
function SearchToc(ditaSearch_Form) {
    debug('SearchToc(..)');

    result = new Pages([]);
    noWords = [];
    excluded = [];
    stackResults = [];

    //START - EXM-30790
    var $searchResults = $("#searchResults");
    var footer = $searchResults.find(".footer");
    //END - EXM-30790
    // Check browser compatibility
    if (navigator.userAgent.indexOf("Konquerer") > -1) {
        alert(getLocalization(txt_browser_not_supported));
        return;
    }

    searchTextField = trim(ditaSearch_Form.textToSearch.value);
    // Eliminate the cross site scripting possibility.
    searchTextField = searchTextField.replace(/</g, " ")
        .replace(/>/g, " ")
        .replace(/"/g, " ")
        .replace(/'/g, " ")
        .replace(/=/g, " ")
        .replace(/0\\/g, " ")
        .replace(/\\/g, " ")
        .replace(/\//g, " ")
        .replace(/  +/g, " ");

    var expressionInput = searchTextField;
    debug('Search for: ' + expressionInput);

    var wordsArray = [];
    var splittedExpression = expressionInput.split(" ");
    for (var t in splittedExpression) {
        if (!contains(stopWords, splittedExpression[t]) || contains(knownOperators, splittedExpression[t])) {
            wordsArray.push(splittedExpression[t]);
        } else {
            excluded.push(splittedExpression[t]);
        }
    }
    expressionInput = wordsArray.join(" ");

    realSearchQuery = expressionInput;

    if (expressionInput.trim().length > 0 || excluded.length > 0) {
        searchAndDisplayResults(expressionInput);

        //START - EXM-30790
        $searchResults.append(footer);
        $searchResults.scrollTop(0);
        //END - EXM-30790
    }
    
    clearHighlights();
    ditaSearch_Form.textToSearch.focus();
}

var stemQueryMap = [];  // A hashtable which maps stems to query words

var scriptLetterTab = null;

/**
 * @description This function parses the search expression and loads the indices.
 * @param {String} expressionInput A single search term to search for.
 * @return {Array} Array with the resulted pages and indices.
 */
function realSearch(expressionInput) {
    expressionInput = trim(expressionInput);
    debug('realSearch("' + expressionInput + '")');
    /**
     *data initialisation
     */
    scriptLetterTab = new Scriptfirstchar(); // Array containing the first letter of each word to look for
    var finalWordsList = []; // Array with the words to look for after removing spaces
    var fileAndWordList = [];
    var txt_wordsnotfound = "";

    /* EXM-26412 */
    expressionInput = expressionInput.toLowerCase();
    /*  START - EXM-20414 */
    var searchFor = expressionInput.replace(/<\//g, "_st_").replace(/\$_/g, "_di_").replace(/%2C|%3B|%21|%3A|@|\/|\*/g, " ").replace(/(%20)+/g, " ").replace(/_st_/g, "</").replace(/_di_/g, "%24_");
    /*  END - EXM-20414 */

    searchFor = searchFor.replace(/  +/g, " ");
    searchFor = searchFor.replace(/ $/, "").replace(/^ /, "");

    var wordsList = [searchFor];
    debug('words from search:', wordsList);
    // set the tokenizing method
    useCJKTokenizing = !!(typeof indexerLanguage != "undefined" && (indexerLanguage == "zh" || indexerLanguage == "ko"));
    //If Lucene CJKTokenizer was used as the indexer, then useCJKTokenizing will be true. Else, do normal tokenizing.
    // 2-gram tokenizing happens in CJKTokenizing,
    // If doStem then make tokenize with Stemmer
    //var finalArray;
    if (doStem) {
        if (useCJKTokenizing) {
            // Array of words
            finalWordsList = cjkTokenize(wordsList);
        } else {
            // Array of words
            finalWordsList = tokenize(wordsList);
        }
    } else if (useCJKTokenizing) {
        // Array of words
        finalWordsList = cjkTokenize(wordsList);
        debug('CJKTokenizing, finalWordsList: ' + finalWordsList);
    } else {
        finalWordsList = [searchFor];
    }
    if (!useCJKTokenizing) {
        /**
         * Compare with the indexed words (in the w[] array), and push words that are in it to tempTab.
         */
        var tempTab = [];

        var wordsArray = '';
        for (var t in finalWordsList) {
            if (!contains(stopWords, finalWordsList[t])) {
                if (doStem || finalWordsList[t].toString().length == 2) {
                    if (w[finalWordsList[t].toString()] == undefined) {
                        txt_wordsnotfound += finalWordsList[t] + " ";
                    } else {
                        tempTab.push(finalWordsList[t]);
                    }
                } else {
                    var searchedValue = finalWordsList[t].toString();
                        var listOfWordsStartWith = wordsStartsWith(searchedValue);
                    if (listOfWordsStartWith != undefined) {
                        listOfWordsStartWith = listOfWordsStartWith.substr(0, listOfWordsStartWith.length - 1);
                        wordsArray = listOfWordsStartWith.split(",");
                        for (var i in wordsArray) {
                            tempTab.push(wordsArray[i]);
                        }
                    }
                }
            }
        }
        finalWordsList = tempTab;
        finalWordsList = removeDuplicate(finalWordsList);
    }

    for (var word in txt_wordsnotfound.split(" ")) {
        if (!inArray(word, noWords) && word.length>1) {
            noWords.push(word);
        }
    }

    if (finalWordsList.length) {
        fileAndWordList = SortResults(finalWordsList, expressionInput);
    } else {
        noWords.push(expressionInput);
    }

    return fileAndWordList;
}

/**
 * @description Display results in HTML format
 * @param {Array} fileAndWordList Array with pages and indices that will be displayed
 */
function displayResults(fileAndWordList) {
    var linkTab = [];

    var results = "";

    var txt_wordsnotfound = "";

    for (var i = 0; i < excluded.length; i++) {
        txt_wordsnotfound += excluded[i] + " ";
    }

    if (fileAndWordList.value !== undefined) {
        var allPages = fileAndWordList.sort().value;

        if (excluded.length > 0) {
            var tempString = "<p>" + getLocalization(partialSearch2) + " " + txt_wordsnotfound + "</p>";
            linkTab.push(tempString);
        }

        if (realSearchQuery.length > 0) {
            linkTab.push("<p>" + getLocalization(txt_results_for) + " " + "<span class=\"searchExpression\">" + realSearchQuery + "</span>" + "</p>");
        }
        linkTab.push("<ul class='searchresult'>");
        var ttScore_first = 1;
        if (allPages.length > 0) {
            ttScore_first = allPages[0].scoring;
        }
        for (var page = 0; page < allPages.length; page++) {
            debug("Page number: " + page);

            var hundredPercent = allPages[page].scoring + 100 * allPages[page].motsnb;
            var numberOfWords = allPages[page].motsnb;
            debug("hundredPercent: " + hundredPercent + "; ttScore_first: " + ttScore_first + "; numberOfWords: " + numberOfWords);

            var ttInfo = allPages[page].filenb;
            // Get scoring
            var ttScore = allPages[page].scoring;

            debug('score for' + allPages[page].motslisteDisplay + ' = ' + ttScore);

            var tempInfo = fil[ttInfo];
            var pos1 = tempInfo.indexOf("@@@");
            var pos2 = tempInfo.lastIndexOf("@@@");
            var tempPath = tempInfo.substring(0, pos1);
            // EXM-27709 START
            // Display words between '<' and '>' in title of search results.
            var tempTitle = tempInfo.substring(pos1 + 3, pos2)
                .replace(/</g, "&lt;").replace(/>/g, "&gt;");
            // EXM-27709 END
            var tempShortDesc = tempInfo.substring(pos2 + 3, tempInfo.length);

            if (tempPath == 'toc.html') {
                continue;
            }
            //var split = allPages[page].motsliste.split(",");
            var finalArray = allPages[page].motsliste.split(", ");
            debug(finalArray);
            var arrayString = 'Array(';
            for (var x in finalArray) {
                if (finalArray[x].length >= 2 || useCJKTokenizing || (indexerLanguage == "ja" && finalArray[x].length >= 1)) {
                    arrayString += "'" + finalArray[x] + "',";
                }
            }
            arrayString = arrayString.substring(0, arrayString.length - 1) + ")";

            var idLink = 'foundLink' + page;
            var link = 'return openAndHighlight(\'' + tempPath + '\', ' + arrayString + '\)';
            var linkString = '<li><a id="' + idLink + '" href="' + tempPath + '" class="foundResult" onclick="' + link + '">' + tempTitle + '</a>';
            // Fake value
            var maxNumberOfWords = allPages[page].motsnb;
            var starWidth = (ttScore * 100 / hundredPercent) / (ttScore_first / hundredPercent) * (numberOfWords / maxNumberOfWords);
            starWidth = starWidth < 10 ? (starWidth + 5) : starWidth;
            // Keep the 5 stars format
            if (starWidth > 85) {
                starWidth = 85;
            }
            // Also check if we have a valid description
            if ((tempShortDesc != "null" && tempShortDesc != '...')) {
                linkString += "\n<div class=\"shortdesclink\">" + tempShortDesc + "</div>";
            }

            try {
                if (webhelpSearchRanking) {
                    // Add rating values for scoring at the list of matches
                    linkString += "<div id=\"rightDiv\">";
                    linkString += "<div id=\"star\">";
                    linkString += "<div id=\"star0\" class=\"star\">";
                    linkString += "<div id=\"starCur0\" class=\"curr\" style=\"width: " + starWidth + "px;\">&nbsp;</div>";
                    linkString += "</div>";
                    linkString += "<br style=\"clear: both;\">";
                    linkString += "</div>";
                    linkString += "</div>";
                }
            } catch (e) {
                debug(e);
            }

            linkString += "</li>";
            linkTab.push(linkString);
        }


        linkTab.push("</ul>");

        if (linkTab.length > 2) {
            results = "<p>";
            for (var t in linkTab) {
                results += linkTab[t].toString();
            }
            results += "</p>";
        } else {
            results = "<p>" + getLocalization("Search no results") + " " + "<span class=\"searchExpression\">" + txt_wordsnotfound + "</span>" + "</p>";
        }
    } else {
        results = "<p>" + getLocalization("Search no results") + " " + "<span class=\"searchExpression\">" + txt_wordsnotfound + "</span>" + "</p>";
    }

    // Verify if the browser is Google Chrome and the WebHelp is used on a local machine
    // If browser is Google Chrome and WebHelp is used on a local machine a warning message will appear
    // Highlighting will not work in this conditions. There is 2 workarounds
    if (notLocalChrome) {
        document.getElementById('searchResults').innerHTML = results;
    } else {
        document.getElementById('searchResults').innerHTML = warningMsg + results;
    }

    $("#search").trigger('click');
}

// Return true if "word" value is an element of "arrayOfWords"  
function contains(arrayOfWords, word) {
    var found = true;
    if (word.length >= 2 || (indexerLanguage == "ja" && word.length >= 1)) {
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
function wordsStartsWith(searchedValue) {
    var toReturn = '';
    for (var sv in w) {
        if (sv.toLowerCase().indexOf(searchedValue.toLowerCase()) == 0) {
            toReturn += sv + ",";
        }
    }
    return toReturn.length > 0 ? toReturn : undefined;
}

function tokenize(wordsList) {
    debug('tokenize(' + wordsList + ')');
    var stemmedWordsList = []; // Array with the words to look for after removing spaces
    var cleanwordsList = []; // Array with the words to look for
    for (var j in wordsList) {
        var word = wordsList[j];
        if (typeof stemmer != "undefined" && doStem) {
            stemQueryMap[stemmer(word)] = word;
        } else {
            stemQueryMap[word] = word;
        }
    }
    debug('scriptLetterTab in tokenize: ', scriptLetterTab);
    scriptLetterTab.add('s');
    //stemmedWordsList is the stemmed list of words separated by spaces.
    for (var t in wordsList) {
        if (wordsList.hasOwnProperty(t)) {
            wordsList[t] = wordsList[t].replace(/(%22)|^-/g, "");
            if (wordsList[t] != "%20") {
                var firstChar = wordsList[t].charAt(0);
                scriptLetterTab.add(firstChar);
                cleanwordsList.push(wordsList[t]);
            }
        }
    }

    if (typeof stemmer != "undefined" && doStem) {
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
function cjkTokenize(wordsList) {
    var allTokens = [];
    var notCJKTokens = [];
    debug('in cjkTokenize(), wordsList: ', wordsList);
    for (var j = 0; j < wordsList.length; j++) {
        var word = wordsList[j];
        debug('in cjkTokenize(), next word: ', word);
        if (getAvgAsciiValue(word) < 127) {
            notCJKTokens.push(word);
        } else {
            debug('in cjkTokenize(), use CJKTokenizer');
            var tokenizer = new CJKTokenizer(word);
            var tokensTmp = tokenizer.getAllTokens();
            allTokens = allTokens.concat(tokensTmp);
            debug('in cjkTokenize(), found new tokens: ', allTokens);
        }
    }
    allTokens = allTokens.concat(tokenize(notCJKTokens));
    return allTokens;
}

//A simple way to determine whether the query is in english or not.
function getAvgAsciiValue(word) {
    var tmp = 0;
    var num = word.length < 5 ? word.length : 5;
    for (var i = 0; i < num; i++) {
        if (i == 5) break;
        tmp += word.charCodeAt(i);
    }
    return tmp / num;
}

//CJKTokenizer
function CJKTokenizer(input) {
    this.input = input;
    this.offset = -1;
    this.tokens = [];
    this.incrementToken = incrementToken;
    this.tokenize = tokenize;
    this.getAllTokens = getAllTokens;
    this.unique = unique;

    function incrementToken() {
        if (this.input.length - 2 <= this.offset) {
            return false;
        } else {
            this.offset += 1;
            return true;
        }
    }

    function tokenize() {
        return this.input.substring(this.offset, this.offset + 2);
    }

    function getAllTokens() {
        while (this.incrementToken()) {
            var tmp = this.tokenize();
            this.tokens.push(tmp);
        }
        return this.unique(this.tokens);
    }

    function unique(a) {
        var r = [];
        o:for (var i = 0, n = a.length; i < n; i++) {
            for (var x = 0, y = r.length; x < y; x++) {
                if (r[x] == a[i]) continue o;
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

Scriptfirstchar.prototype.add = function (caract) {
    if (typeof this.strLetters == 'undefined') {
        this.strLetters = caract;
    } else if (this.strLetters.indexOf(caract) < 0) {
        this.strLetters += caract;
    }

    return 0;
};

/* end of scriptfirstchar */


// Array.unique( strict ) - Remove duplicate values
function unique(tab) {
    debug("unique(",tab,")");
    var a = [];
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
 *  - The key is the index of an HTML file which contains a word to look for.
 *  - The value is the list of all words contained in the HTML file.
 *
 * @param {Array} words - list of words to look for.
 * @param {String} searchedWord - search term typed by user
 * @return {Array} - the hashtable fileAndWordList
 */
function SortResults(words, searchedWord) {

    var fileAndWordList = {};
    if (words.length == 0 || words[0].length == 0) {
        return null;
    }

    // In generated js file we add scoring at the end of the word
    // Example word1*scoringForWord1,word2*scoringForWord2 and so on
    // Split after * to obtain the right values
    var scoringArr = [];
    for (var t in words) {
        // get the list of the indices of the files.
        var listNumerosDesFicStr = w[words[t].toString()];
        var tab = listNumerosDesFicStr.split(",");
        //for each file (file's index):
        for (var t2 in tab) {
            var tmp = '';
            var idx;
            var temp = tab[t2].toString();
            if (temp.indexOf('*') != -1) {
                idx = temp.indexOf('*');
                tmp = temp.substring(idx + 3, temp.length);
                temp = temp.substring(0, idx);
            }
            scoringArr.push(tmp);
            if (fileAndWordList[temp] == undefined) {
                fileAndWordList[temp] = "" + words[t];
            } else {
                fileAndWordList[temp] += "," + words[t];
            }
        }
    }

    var fileAndWordListValuesOnly = [];
    // sort results according to values
    var temptab = [];
    var finalObj = [];
    for (t in fileAndWordList) {
        finalObj.push(new newObj(t, fileAndWordList[t]));
    }
    finalObj = removeDerivates(finalObj, searchedWord);

    for (t in finalObj) {
        tab = finalObj[t].wordList.split(',');
        var tempDisplay = [];
        for (var x in tab) {
            if (stemQueryMap[tab[x]] != undefined && doStem) {
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
    debug("fileAndWordListValuesOnly: ", fileAndWordListValuesOnly);
    fileAndWordListValuesOnly = unique(fileAndWordListValuesOnly);
    fileAndWordListValuesOnly = fileAndWordListValuesOnly.sort(compareWords);

    var listToOutput = [];
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
        listToOutput[i].sort(function (a, b) {
            return b.scoring - a.scoring;
        });
    }
    // If we have groups with same number of words,
    // will sort groups by higher scoring of each group
    for (i = 0; i < listToOutput.length - 1; i++) {
        for (j = i + 1; j < listToOutput.length; j++) {
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

/**
 * @description Remove derivatives words from the list of words
 * @param {Array} obj Array that contains results for searched words
 * @param {String} searchedWord search term typed by user
 * @return {Array} Clean array results without duplicated and derivatives words
 */
function removeDerivates(obj, searchedWord) {
    debug("removeDerivatives(",obj,")");
    var toResultObject = [];
    for (var i in obj) {
        var filesNo = obj[i].filesNo;
        var wordList = obj[i].wordList;
        var wList = wordList.split(",");

        for (var j = 0; j < wList.length; j++) {
            if (startsWith(wList[j], searchedWord)) {
                wList[j] = searchedWord;
            }
        }

        wList = removeDuplicate(wList);
        var recreateList = '';
        for (var x in wList) {
            recreateList += wList[x] + ",";
        }
        recreateList = recreateList.substr(0, recreateList.length - 1);
        toResultObject.push(new newObj(filesNo, recreateList));
    }

    return toResultObject;
}

function newObj(filesNo, wordList) {
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
    this.motslisteDisplay = motslisteDisplay;
    this.scoring = scoring;
}

function findRating(words, nr) {
    var sum = 0;
    var xx = words.split(',');
    for (var jj = 0; jj < xx.length; jj++) {
        var wrd = w[xx[jj]].split(',');
        for (var ii = 0; ii < wrd.length; ii++) {
            var wrdno = wrd[ii].split('*');
            if (wrdno[0] == nr) {
                sum += parseInt(wrdno[1]);
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
function verifyBrowser() {
    var returnedValue = true;
    BrowserDetect.init();
    var browser = BrowserDetect.browser;
    var addressBar = window.location.href;
    if (browser == 'Chrome' && addressBar.indexOf('file://') === 0) {
        returnedValue = false;
    }

    return returnedValue;
}

// Remove duplicate values from an array
function removeDuplicate(arr) {
    var r = [];
    o:for (var i = 0, n = arr.length; i < n; i++) {
        for (var x = 0, y = r.length; x < y; x++) {
            if (r[x] == arr[i]) continue o;
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

/**
 * PATCH FOR BOOLEAN SEARCH
 */

/**
 * @description Object with resulted pages as array
 * @param array Array that contains partial results
 * @constructor
 */
function Pages(array) {
    this.value = array;

    this.toString = function() {
        var stringResult = "";

        stringResult += "INDEX\t|\tfilenb\t|\tscoring\n";
        for (var i=0;i<this.value.length;i++) {
            stringResult += i + ".\t\t|\t" + this.value[i].filenb + "\t\t|\t" + this.value[i].scoring + "\n";
        }

        return stringResult;
    };

    this.writeIDs = function() {
        var stringResult = "";

        for (var i=0;i<this.value.length;i++) {
            stringResult += this.value[i].filenb + " | ";
        }

        return stringResult;
    };

    this.and = function and(newArray) {
        var result = [];

        for (var x=0; x<this.value.length; x++) {
            var found = false;
            for (var y=0; y<newArray.value.length; y++) {
                if (this.value[x].filenb == newArray.value[y].filenb) {
                    this.value[x].motsliste += ", " + newArray.value[y].motsliste;
                    this.value[x].scoring += newArray.value[y].scoring;
                    found = true;
                    break;
                }
            }
            if (found) {
                result.push(this.value[x]);
            }
        }

        this.value = result;

        return this;
    };

    this.or = function or(newArray) {
        this.value = this.value.concat(newArray.value);
        var result = [];

        for (var i=0;i<this.value.length;i++) {
            var unique = true;
            for (var j=0;j<result.length;j++) {
                if (this.value[i].filenb == result[j].filenb) {
                    result[j].motsliste += ", " + this.value[i].motsliste;
                    var numberOfWords = result[j].motsliste.split(", ").length;
                    result[j].scoring = numberOfWords*(this.value[i].scoring + result[j].scoring);
                    unique = false;
                    break;
                }
            }
            if (unique) {
                result.push(this.value[i]);
            }
        }

        this.value = result;

        return this;
    };

    this.not = function not(newArray) {
        var result = [];

        for (var x=0; x<this.value.length; x++) {
            var found = false;
            for (var y=0; y<newArray.value.length; y++) {
                if (this.value[x].filenb == newArray.value[y].filenb) {
                    found = true;
                }
            }
            if (!found) {
                result.push(this.value[x]);
            }
        }

        this.value = result;

        return this;
    };

    this.sort = function() {
        for (var i = 0; i < this.value.length; i++) {
            for (var j = i; j > 0; j--) {
                if ((this.value[j].scoring - this.value[j - 1].scoring) > 0) {
                    var aux = this.value[j];
                    this.value[j] = this.value[j-1];
                    this.value[j-1] = aux;
                }
            }
        }

        return this;
    };
}

if(typeof String.prototype.trim !== 'function') {
    String.prototype.trim = function() {
        return $.trim(this);
    }
}