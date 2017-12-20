/*

Oxygen Webhelp Plugin
Copyright (c) 1998-2017 Syncro Soft SRL, Romania.  All rights reserved.

*/

var iframeDir = "";
var wh = parseUri(window.location);
var whUrl = wh.protocol + '://' + wh.authority + wh.directory;
var islocal = wh.protocol == 'file';
var pageName = wh.file;
var loaded = false;
var ws = false;
var searchedWords = "";
var resizeTimer;
var lastLoadedPage = "";
var showAll = true;
var currentReq = null;
var noFoldableNodes = 0;
var tabsInitialized = false;
var tocWidth;
var navLinksWidth;
var breadCrumbWidth;
var navLinksWidthMin;

var searchLoaded = false;

if ( top != self) {
    searchLoaded = true;
}

// If page is local and is viewed in Chrome
var notLocalChrome = verifyBrowser();

/**
 * @description Returns all available parameters or empty object if no parameters in URL
 * @return {Object} Object containing {key: value} pairs where key is the parameter name and value is the value of parameter
 */
function getParameter(parameter) {
    var whLocation = "";

    try {
        whLocation = window.location;
        var p = parseUri(whLocation);

        for (var param in p.queryKey) {
            if (p.queryKey.hasOwnProperty(param) && parameter.toLowerCase() == param.toLowerCase()){
                return p.queryKey[param];
            }
        }
    } catch (e) {
        error(e);
    }
}

/**
 * @description Get all GET parameters and keep all unknown parameters
 * @return {String} Query with all parameters
 */
function parseParameters() {
    debug("parseParameters()...");
    // All parameters in this list have to be lower case
    var excludeParameters = ["contextid", "appname"];
    var whLocation = "";
    var query = "?";

    var p = {};

    try {
        whLocation = window.location;
        p = parseUri(whLocation);
    } catch (e) {
        error(e);
    }

    var parameters = p.queryKey;

    for (var para in parameters) {
        if ($.inArray(para.toLowerCase(), excludeParameters) == -1) {
            query += para + "=" + parameters[para] + "&";
        }
    }

    query = query.substr(0, query.length - 1);

    debug("QUERY: "+ query);

    return query;
}

// Used to inject search form where we need it
var searchForm = '<form name="searchForm" id="searchForm" onsubmit="return executeQuery();">' +
	'<input type="text" id="textToSearch" name="textToSearch" class="textToSearch" size="30" placeholder="Search" />' +
	'</form>';

/**
 * @description Search using Google Search if it is available, otherwise use our search engine to execute the query
 * @return {boolean} Always return false
 */
function executeQuery() {
	var input = document.getElementById('textToSearch');
	try {
		var element = google.search.cse.element.getElement('searchresults-only0');
	} catch (e) {
		error(e);
	}
	if (element != undefined) {
		if (input.value == '') {
			element.clearAllResults();
		} else {
			element.execute(input.value);
		}
	} else {
		searchRequest('wh-classic');
	}
	
	return false;
}
	
function openTopic(anchor) {
    $("#contentBlock ul").css("background-color", $("#splitterContainer #leftPane").css('background-color'));
    $("#contentBlock li").css("background-color", "transparent");
    if ($(anchor).attr('target') === undefined) {
        /* Remove the old selection and add selection to the clicked item */
        $('#contentBlock li span').removeClass('menuItemSelected');
        $(anchor).parent('li span').addClass('menuItemSelected');

        /* Calculate index of selected item and write value to cookie */
        var findIndexOf = $(anchor).closest('li');
        var index = $('#contentBlock li').index(findIndexOf);

        if ( wh.protocol == 'https' ) {
            $.cookie('wh_pn', index, { secure: true });
        } else {
            $.cookie('wh_pn', index);
        }

        $('#contentBlock .menuItemSelected').parent('li').first().css('background-color', $('#contentBlock .menuItemSelected').css('background-color'));

        /* Redirect to requested page */
        redirect($(anchor).attr('href'));
    } else {
        window.open($(anchor).attr('href'), $(anchor).attr('target'));
    }

    try {
        recomputeBreadcrumb(-1);
    } catch (e) {
        debug(e);
    }
}


$(document).ready(function () {
    $('#preload').hide();

    var query = parseParameters();

    if ( !withFrames ) {
        $('.framesLink').after(searchForm);
    } else {
        $('#searchBlock').prepend(searchForm);
        $('#searchForm').addClass('frameset');
        $('#textToSearch').addClass('frameset');
        $('search').trigger('click');
    }

    /**
     * {Refactored}
     * @description Selects the clicked item
     */
    $('#contentBlock li a').click(function(ev) {
        if (!ev.altKey && !ev.ctrlKey && !ev.shiftKey && ev.button==0) {
            ev.preventDefault();
        if ($(this).attr('href').indexOf('#!_') == 0) {
            // expand topichead
            toggleItem($(this));
            // find first descendant that is a topicref with href attribute
            // and open that topicref
                $(this).parents('li').first().find('li a').each(function () {
                if ($(this).attr('href').indexOf('#!_') != 0) {
                     openTopic($(this));
                     return false;
                }
                return true;
            });
        } else {
             openTopic($(this));
        }
        return false;
        } else {
            if ($(this).attr('href').indexOf('#!_') == 0) {
                // expand topichead
                toggleItem($(this));
                // find first descendant that is a topicref with href attribute
                // and open that topicref
                $(this).parents('li').first().find('li a').each(function () {
                    if ($(this).attr('href').indexOf('#!_') != 0) {
                        openTopic($(this));
                        return false;
                    }
                    return true;
    });
                return false;
            }
        }
    });
    
    var contextId = getParameter('contextId');
    var appname = getParameter('appname');

    var q = getParameter('q');

    if (contextId !== undefined && contextId != "") {
        var scriptTag = document.createElement("script");
        scriptTag.type = "text/javascript";
        scriptTag.src = "context-help-map.js";
        document.getElementsByTagName('head')[0].appendChild(scriptTag);

        var ready = setInterval(function () {
            if (helpContexts != undefined) {
                for (var i = 0; i < helpContexts.length; i++) {
                    var ctxt = helpContexts[i];
                    if (contextId == ctxt["appid"] && (appname == undefined || appname == ctxt["appname"])) {
                        var path = ctxt["path"];
                        if (path != undefined && path!="") {
                            if (withFrames) {
                                try {
                                    var newLocation = whUrl + path;
                                    window.parent.contentwin.location.href = newLocation;
                                } catch (e) {
                                    error(e);
                                }
                            } else {
                                var newLocation = window.location.protocol + '//' + window.location.host;
                                
                                newLocation+= window.location.pathname + query + '#' + path;
                                window.location=newLocation;
                            }
                        }
                        break;
                    }
                }
                clearInterval(ready);
            }
        }, 100);
    } else {
        try {
            var p = parseUri(parent.location);
        } catch (e) {
            error(e);
            var p = parseUri(window.location);
        }
        if (withFrames) {
            if (q != undefined) {
                try {
                	var link = p.protocol + '://' + p.host + ':' + p.port + q;
	                window.parent.contentwin.location.href = link;
                } catch (e) {
                    error(e);
                }
            } else {
								openTopic($('#tree a').first());
            }
        }
    }

    var searchQuery = getParameter("searchQuery");
    debug("Search for: " + searchQuery);
    if (searchQuery!=undefined && searchQuery!="") {
        $("#textToSearch").val(decodeURI(searchQuery));
        if (!withFrames) {
            loadSearchResources();
        }
        $("#searchForm").submit();
    }

    // Show clicked TAB
    $('.tab').click(function () {
        showMenu($(this).attr('id'));
        if ( !withFrames ) {
            toggleLeft();
        }
    });

    // Normalize TOC HREFs and add '#' for no-frames webhelp
    $('#contentBlock li a').each(function () {
        var old = $(this).attr('href');
        var newHref = old;
		// Add '#' only if @target attribute is not specified
        if ($(this).attr('target')!==undefined) {
            newHref = normalizeLink(old);
        } else {
            newHref = '#' + normalizeLink(old);
        }
        /* If with frames */
        if ( withFrames ) {
            newHref = normalizeLink(old);
        }
        if (old == 'javascript:void(0)') {
            $(this).attr('href', '#!_' + $(this).text());
        } else {
            $(this).attr('href', newHref);
            info('alter link:' + $(this).attr('href') + ' from ' + old);
        }
    });

    // Toggle clicked item from TOC
    $('#contentBlock li>span').click(function (ev) {
        if ( !ev.shiftKey && !ev.ctrlKey && !ev.altKey && ev.button==0 ) {
            ev.preventDefault();
        toggleItem($(this));
        }

    });

    // Determine if li element have submenus and sets corresponded class
    $('#contentBlock li>span').each(function () {
        if ($(this).parent().find('>ul').length > 0) {
            $(this).addClass('hasSubMenuClosed');
        } else {
            $(this).addClass('topic');
        }
    });

    debug('discover foldables ' + $('#tree > ul li > span').length + ' - ' + $('#tree > ul li > span.topic').length);

    // Calculate number of foldable nodes and show/hide expand buttons
    noFoldableNodes = $('#tree > ul li > span').length - $('#tree > ul li > span.topic').length;
    showHideExpandButtons();

    // Set title of expand/collapse all buttons
    $('#expandAllLink').attr('title', getLocalization("ExpandAll"));
    $('#collapseAllLink').attr('title', getLocalization("CollapseAll"));

    $("#indexForm").css("background-color", $("#leftPane").css("background-color"));

    /**
     * Keep Collapse / Expand buttons in the right sideof the left pane
     */
    $("#bck_toc").bind("scroll", function() {
        if ( $('#contentBlock').is(':visible') ) {
        var scrollX = $("#bck_toc").scrollLeft();
        $("#expnd").css("left", scrollX);
        } else {
            $("#bck_toc").scrollLeft(0);
        }
    });
});

$(window).resize(function(){
    if ($("#searchBlock").is(":visible")) {
        if ( !withFrames ) {
            var hAvailable = parseInt($("body").height()) - parseInt($("#header").height());
        } else {
            var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#searchForm").height())-parseInt($("#searchForm").css("padding-top"))-parseInt($("#searchForm").css("padding-bottom"));
        }
        $("#searchResults").css("height", hAvailable);
    }
    if ($("#indexBlock").is(":visible")) {
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#indexForm").height())-parseInt($("#indexForm").css("padding-top"))-parseInt($("#indexForm").css("padding-bottom"))-parseInt($("#bck_toc").css("padding-top"));
        $("#iList").css("height", hAvailable);
    }
    if ($("#contentBlock").is(":visible")) {
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#bck_toc").css("padding-top"));
        $("#bck_toc").css("height", hAvailable);
    }
});


try {
    $(window.parent).resize(function () {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(resizeContent, 10);
    });
} catch (e) {
    error(e);
}

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

/**
 * {Refactored}
 * @description Redirect browser to a new address
 * @param link - the url to be redirected to
 */
function redirect(link) {
    debug ('redirect(' + link + ');');
    // Determine the webhelp type: no-frames or frames
    if ( !withFrames ) {
        // no-frames
        window.location.href = link;
    } else {
        // with frames
        window.parent.contentwin.location.href = link;
    }
}

/**
 * {Refactored}
 * @description Initialize the menu tabs: Content, Search & Index
 */
function initTabs() {
    if (! tabsInitialized) {
        // Get the tabs internationalization text
        var contentLinkText = getLocalization("webhelp.content");
        if ( !withFrames ) {
        var searchLinkText = getLocalization("SearchResults");
        } else {
            var searchLinkText = getLocalization("webhelp.search");
        }
        var indexLinkText = getLocalization("webhelp.index");
        var IndexPlaceholderText = getLocalization("IndexFilterPlaceholder");
        var SearchPlaceholderText = getLocalization("webhelp.search");

        var tabs = new Array("content", "search", "index");
        for (var i = 0; i < tabs.length; i++) {
            var currentTabId = tabs[i];
            // Generates menu tabs
            if ($("#"+currentTabId).length > 0) {
                debug('Init tab with name: ' + currentTabId);
                $("#"+currentTabId).html(eval(currentTabId + "LinkText"));
                $("#"+currentTabId).attr("title", eval(currentTabId + "LinkText"));
            } else {
                info('init no tab found with name: ' + currentTabId);
            }
            tabsInitialized = true;
        }

        $("#id_search").attr("placeholder", IndexPlaceholderText);
        $("#textToSearch").attr("placeholder", SearchPlaceholderText);
    }
}


/*
 * Lazy loading of indexterms on Index tab.
 */
function loadIndexterms() {
    try {
        var scriptTag = document.createElement("script");
        scriptTag.type = "text/javascript";
        scriptTag.src = "oxygen-webhelp/indexterms.js";
        document.getElementsByTagName('head')[0].appendChild(scriptTag);
        
        var loaded = setInterval(function(){
            if ( trim($("#indexBlock #iList").html()) != '' ) {
                // append footer
                $("#indexBlock #iList").append($("#leftPane .footer"));
                clearInterval(loaded);
            }
        }, 10);
    } catch (e) {
        if ( $("#indexList").length < 1 ) {
            $("#index").html('<span id="loadingError">Index loading error: ' + e.message + '</span>');
        }
    }
}

/**
 * @description Select "Content" tab if no other tab is selected
 */
function initializeTabsMenu() {
    debug("Active tabs: ", $(".selectedClass").length);
    if ( $(".selectedTab").length == 0 ) {
        showMenu('content');
    }
}

/**
 * {Refactored}
 * @description Hide and show left side section elements
 * @param displayTab - Tab to be displayed
 */
function showMenu(displayTab) {
    debug('showMenu(' + displayTab + ');');
    if(notLocalChrome){
        try {
        	parent.termsToHighlight = Array();
        } catch (e) {
            error(e);
	    }
    }

    initTabs();
    var tabs = new Array("content", "search", "index");
    for (var i = 0; i < tabs.length; i++) {
        var currentTabId = tabs[i];
        // generates menu tabs
        if ($("#" + currentTabId).length > 0) {
            // show selected block
            var selectedBlock = displayTab + "Block";
            if (currentTabId == displayTab) {
                $("#" + selectedBlock).show();
                $('#' + currentTabId).addClass('selectedTab');
            } else {
                $("#" + currentTabId + 'Block').hide();
                $('#' + currentTabId).removeClass('selectedTab');
            }
        }
    }
    if (displayTab == 'content') {
        $("#bck_toc").removeAttr('style');
        if ( $('#searchResults').text() == '' && !withFrames ) {
            $('#search').hide();
        }
        searchedWords = "";
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#bck_toc").css("padding-top"));
        $("#bck_toc").css("height", hAvailable);
        $("#contentBlock").append($("#leftPane .footer"));
        scrollToVisibleItem();
    }
    if (displayTab == 'search') {
        $('#search').show();
        $('.textToSearch').focus();
        searchedWords = $('#textToSearch').text();
        $("#bck_toc").css("overflow", "hidden");
        $("#bck_toc,#bck_toc #indexBlock").css("height", "100%");
        if ( !withFrames ) {
            var hAvailable = parseInt($("body").height()) - parseInt($("#header").height());
        } else {
            var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#searchForm").height())-parseInt($("#searchForm").css("padding-top"))-parseInt($("#searchForm").css("padding-bottom"));
        }
        $("#searchResults").css("height", hAvailable);
        $("#searchResults").append($("#leftPane .footer"));
    }
    if (displayTab == 'index') {
        if ( $('#searchResults').text() == '' && !withFrames ) {
            $('#search').hide();
        }
        if ( !withFrames ) {
            loadIndexterms();
        } else {
            $("#iList").append($("#leftPane .footer"));
        }
        
        $('#id_search').focus();
        searchedWords = "";
        $("#bck_toc").css("overflow", "hidden");
        $("#bck_toc,#bck_toc #searchBlock").css("height", "100%");
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#indexForm").height())-parseInt($("#indexForm").css("padding-top"))-parseInt($("#indexForm").css("padding-bottom"))-parseInt($("#bck_toc").css("padding-top"));
        $("#iList").css("height", hAvailable);
    }
}
/**
 * @description Show navigation links tooltip (parent / previous / next)
 * @param event
 */
var showTooltip = function (event) {
    $('div.tooltip').remove();
    $('<div id="tooltipNew" class="tooltip"></div>').appendTo('body');
    $('#tooltipNew').html($(this).find('>a').attr('title'));
    changeTooltipPosition(event);
};

/**
 * @description Change tooltip position of the navigation links
 * @param event
 */
var changeTooltipPosition = function (event) {
    var tooltipX = event.pageX;
    var tooltipY = event.pageY + 20;
    $('div.tooltip').css({
        top: tooltipY,
        left: tooltipX
    });
};

/**
 * @description Hide navigation links tooltip
 */
var hideTooltip = function () {
    $('div.tooltip').remove();
};

/**
 * @description In the left pane(Content, Search, Index), if not all elements are visible will add the scroll bars
 */
function showScrolls() {
//    var w = $('#leftPane').width();
//    var bckTH = $('#bck_toc').height();
//    var leftPH = $('#leftPane').height();
//    debug('showScrolls() w=' + w + ' bckTH=' + bckTH + ' leftPH=' + leftPH);
//    if (w > 0) {
//        if (bckTH > leftPH) {
//            $('#leftPane').css('overflow-y', 'scroll');
//        } else {
//            $('#leftPane').css('overflow-y', 'auto');
//        }
//    } else if (w == 0) {
//        $('#leftPane').css('overflow-y', 'hidden');
//    } else {
//        $('#leftPane').css('overflow-y', 'auto');
//    }
}

/**
 * @description return false if browser is Google Chrome and WebHelp is used on a local machine, not a web server
 * @returns {boolean}
 */
function verifyBrowser() {
    var returnedValue = true;
    var browser = BrowserDetect.browser;
    var addressBar = window.location.href;
    if (browser == 'Chrome' && addressBar.indexOf('file://') === 0) {
        returnedValue = false;
    }
    debug('verifyBrowser()=' + returnedValue);
    return returnedValue;
}

/**
 * {Refactored}
 * @description Reposition the tooltips when browser is resized
 */
function resizeContent() {
    breadCrumbWidth = parseInt($('#breadcrumbLinks').css('width'));
    var need = tocWidth + navLinksWidth + breadCrumbWidth;
    var needMin = tocWidth + navLinksWidthMin + breadCrumbWidth;
    debug('NEED: '+need+' | NEED-MIN: '+needMin);

    var heightScreen = $(window).height();
    var hh = $('#header').height();
    var splitterH = heightScreen - hh -3;
    info('resizeContent() hh=' + hh + ' hs=' + heightScreen);
    $('#splitterContainer').height(splitterH);

    var navLinks = withFrames?$(top.frames[ "contentwin"].document).find(".navparent a,.navprev a,.navnext a"):$(".navparent a,.navprev a,.navnext a");
    var navHeader = withFrames?$(top.frames[ "contentwin"].document).find(".navheader"):$(".navheader");

    if(parseInt($('.tool tr:first').css('width')) < parseInt(need*1.05)){
        debug('Need to hide navigation links');
        navHeader.css("min-width", "110px");
        navLinks.hide();
        if(parseInt($('.tool tr:first').css('width')) < parseInt(needMin*1.05)) {
            debug('Need to hide part of breadcrumbs');
            $('#productToolbar .navheader_parent_path').each(function () {
                $(this).attr("data-title")==undefined ? $(this).attr("data-title", $(this).text()) : null;
                if ($(this).attr('data-title').length>37) {
                    $(this).text($(this).text().replace(/\./gi, '').substr(0, 37) + "...");
                }
            });
        } else {
            debug('Need to show all breadcrumbs');
            $('#productToolbar .navheader_parent_path').each(function () {
                $(this).text($(this).attr("data-title"));
            });
        }
    } else {
        debug('Need to show navigation links');
        navHeader.css("min-width", "326px");
        navLinks.show();
    }
}

debug('<hr> Load Window....');
debug('var whUrl:' + whUrl);
debug('var islocal:' + islocal);
debug('var pageName:' + pageName);
debug('browser:' + navigator.userAgent);
debug('os:' + navigator.appVersion);

/**
 * {Refactored}
 * @description Highlight Search Terms in right frame, except in Chrome when is opened locally
 * @param words (type:Array) - Words to be highlighted
 */
function highlightSearchTerm(words) {

    if (notLocalChrome) {
        if (words !== null && words !== undefined && words.length > 0) {
            // highlight each term in the content view
            var $frm = $('#frm');
            $frm.contents().find('body').removeHighlight();
            for (var i = 0; i < words.length; i++) {
                debug('highlight(' + words[i] + ');');
                $frm.contents().find('body').highlight(words[i]);
            }
        }
    } else {
        // For index with frames
        if (parent.termsToHighlight !== null && parent.termsToHighlight !== undefined && parent.termsToHighlight > 0) {
            // highlight each term in the content view
            for (var i = 0; i < parent.termsToHighlight.length; i++) {
                $('*', window.parent.contentwin.document).highlight(parent.termsToHighlight[i]);
            }
        }
    }
}

/**
 * @description Remove highlight from right frame, except in Chrome when is opened locally
 */
function clearHighlights() {
    debug("clearHighlights()");
    if (top==self) {
        if (notLocalChrome) {
            $('#frm').contents().find('body').removeHighlight();
        }
    } else {
        // For index with frames
        try {
            $(window.parent.contentwin.document).find('body').removeHighlight();
        } catch (e) {
            error(e);
        }
    }
}

/**
 * @description Show/Hide the expand/collapse buttons from the 'Content' tab
 */
function showHideExpandButtons() {
    if (noFoldableNodes > 0) {
        if (BrowserDetect.browser == 'Explorer' && BrowserDetect.version < 8) {
            //debug('IE7');
            $('#expandAllLink').show();
            $('#collapseAllLink').show();
        } else {
            if ($('#tree > ul li > span.hasSubMenuOpened').length != noFoldableNodes) {
                $('#expandAllLink').show();
            } else {
                $('#expandAllLink').hide();
            }
            if ($('#tree > ul > li > span.hasSubMenuOpened').length <= 0) {
                $('#collapseAllLink').hide();
            } else {
                $('#collapseAllLink').show();
            }
        }
    } else {
        $('#expandAllLink').hide();
        $('#collapseAllLink').hide();
    }
}

/**
 * {Refactored}
 * @description Expand all 'Content' tab entries
 */
function expandAll() {
    element=$('#contentBlock li ul').parent().find('>span');
    element.removeClass('hasSubMenuClosed');
    element.addClass('hasSubMenuOpened');
    $('#contentBlock li ul').show();
    showHideExpandButtons();
    return false;
}

/**
 * {Refactored}
 * @description Collapse all 'Content' tab entries
 */
function collapseAll() {
    element=$('#contentBlock li ul').parent().find('>span');
    element.removeClass('hasSubMenuOpened');
    element.addClass('hasSubMenuClosed');
    $('#contentBlock li ul').hide();
    showHideExpandButtons();
    return false;
}

/**
 * @description Scroll TOC to make currently selected item visible.
 */
function scrollToVisibleItem() {
    var $menuItemSelected = $(".menuItemSelected");
    try {
        var tocSelectedItemPos = $menuItemSelected.offset();
        var tocContentPos = $menuItemSelected.parents("#contentBlock").offset();
        var tocSelectedItemOffset = {
            top: tocSelectedItemPos.top - tocContentPos.top,
            left: tocSelectedItemPos.left - tocContentPos.left
        };
    
        var $contentBlock = $("#contentBlock");
        var contentToc = $contentBlock.offset();
        var toc = $contentBlock.parents("#bck_toc").offset();
        var contentTocOffset = {
            top: contentToc.top - toc.top,
            left: contentToc.left - toc.left
        };
        var $bckToc = $("#bck_toc");
        var contentTocHeight = $bckToc.height();
        var maxLimit = contentTocHeight - contentTocOffset.top;
        var minLimit = 0 - contentTocOffset.top;

        if (tocSelectedItemOffset.top < minLimit || tocSelectedItemOffset.top > maxLimit) {
            $bckToc.scrollTop(tocSelectedItemOffset.top);
        }
    } catch (e) {
        error(e);
    }
}

/**
 * {Refactored}
 * @description Toggle the selected item
 * @param loc (Object) - Which list item to expand
 * @param forceOpen - Force to expand the ul element
 */
function toggleItem(loc, forceOpen) {
    debug('toggleItem(' + loc.prop("tagName") + ', ' + forceOpen + ')');

    $(loc).parent().parents('#contentBlock li').find('>span').addClass('hasSubMenuOpened');
    $(loc).parent().parents('#contentBlock li').find('>span').removeClass('hasSubMenuClosed');
    if (loc.hasClass('hasSubMenuOpened') && !(forceOpen == true)) {
        if ($(loc).parent().find('>ul').length > 0) {
            $(loc).removeClass('hasSubMenuOpened');
            $(loc).addClass('hasSubMenuClosed');
            $(loc).parent('#contentBlock li').find('>ul').hide();
        }
    } else {
        if ($(loc).parent().find('>ul').length > 0) {
            $(loc).addClass('hasSubMenuOpened');
            $(loc).removeClass('hasSubMenuClosed');
            $(loc).parent('#contentBlock li').find('>ul').show();
        }
        $(loc).parent().parents('#contentBlock li').find('>ul').show();
    }
    showHideExpandButtons();
    // De vazut daca mai este necesara
    // showScrolls();
}

/**
 * {Refactored}
 * @description Decide which links to show in navheader and navfooter section (parent / previous / next)
 */
function showParents() {
    if (loaded || (! loaded && $("#frm").length == 0)) {
        // We have information about selected item in TOC
        if ($.cookie("wh_pn") != "" && $.cookie("wh_pn") !== undefined && $.cookie("wh_pn") !== null) {

            // Read index of currently selected item and calculate next / previous index
            var currentTOCSelection = parseInt($.cookie("wh_pn"));
            var prevTOCIndex = currentTOCSelection -1;
            var nextTOCIndex = currentTOCSelection + 1;

            // Get the href of the parent / next / previous related to current item
            var prevTOCSelection = getUrlWithoutAnchor($('#tree li:eq(' + prevTOCIndex + ')').find('a').attr('href'));
            var nextTOCSelection = getUrlWithoutAnchor($('#tree li:eq(' + nextTOCIndex + ')').find('a').attr('href'));


            // Current href
            var currentTOCSelectionBase = getUrlWithoutAnchor($('#tree li:eq(' + currentTOCSelection + ')').find('a').attr('href'));

            while (getUrlWithoutAnchor(nextTOCSelection) == currentTOCSelectionBase) {
                nextTOCIndex++;
                nextTOCSelection = $('#tree li:eq(' + nextTOCIndex + ')').find('a').attr('href');
            }

            var auxSelection = currentTOCSelection-2;
            var auxHref = $('#tree li:eq(' + auxSelection + ')').find('a').attr('href');
            while (auxHref != undefined && getUrlWithoutAnchor(prevTOCSelection) == getUrlWithoutAnchor(auxHref)) {
                prevTOCIndex=auxSelection;
                prevTOCSelection = $('#tree li:eq(' + prevTOCIndex + ')').find('a').attr('href');

                auxSelection--;
                auxHref = $('#tree li:eq(' + auxSelection + ')').find('a').attr('href');
            }

            auxSelection = currentTOCSelection;
            auxHref = currentTOCSelectionBase;
            while (getUrlWithoutAnchor(auxHref) == currentTOCSelectionBase) {
                auxSelection--;
                auxHref = $('#tree li:eq(' + auxSelection + ')').find('a').attr('href');
            }
            auxSelection++;

            var parentTOCSelection = getUrlWithoutAnchor($('#tree li:eq(' + auxSelection + ')').closest('ul').closest('li').find('a').attr('href'));

            // Eliminate the first character (#), for no-frames webhelp
            if (prevTOCSelection !== undefined) {
                prevTOCSelection = prevTOCSelection.substring(1);
            } else {
                prevTOCSelection = "undefined";
            }
            
            if (nextTOCSelection !== undefined) {
                nextTOCSelection = nextTOCSelection.substring(1);
            } else {
                nextTOCSelection = "undefined";
            }
            
            if (parentTOCSelection !== undefined) {
                parentTOCSelection = parentTOCSelection.substring(1);
            } else {
                parentTOCSelection = "undefined";
            }

            var navLinks;
            // Get the element that contains navigation links and hide irrelevant links
            if ( !withFrames ) {
                navLinks = $('#navigationLinks');
            } else {
                navLinks = $(parent.frames[ "contentwin"].document).find('.nav').find('.navheader');

                var footerLinks = $(parent.frames[ "contentwin"].document).find('.navfooter');
                $(footerLinks).find('.navparent').hide();
                $(footerLinks).find('.navparent').find('a[href*="' + parentTOCSelection + '"]').first().parent().show();
                
                $(footerLinks).find('.navprev').hide();
                $(footerLinks).find('.navprev').find('a[href*="' + prevTOCSelection + '"]').first().parent().show();
                
                $(footerLinks).find('.navnext').hide();
                $(footerLinks).find('.navnext').find('a[href*="' + nextTOCSelection + '"]').first().parent().show();
            }
            
            navLinks.find('.navparent').hide();
            navLinks.find('.navparent').find('a[href*="' + parentTOCSelection + '"]').first().parent().show();
            
            navLinks.find('.navprev').hide();
            navLinks.find('.navprev').find('a[href*="' + prevTOCSelection + '"]').first().parent().show();
            
            navLinks.find('.navnext').hide();
            navLinks.find('.navnext').find('a[href*="' + nextTOCSelection + '"]').first().parent().show();
        }
    } else {
        debug("P: document not loaded...");
    }
}

/**
 * @description Removes anchors from the given URL
 * @param url {string} URL to remove anchor from
 * @returns {string}
 */
function getUrlWithoutAnchor(url){
    var toReturn = url;

    if (url == undefined || url == "undefined") {
        return url;
    }
    
    if (url.lastIndexOf("#") > 0) {
        toReturn = url.substring(0, url.lastIndexOf("#"))
    }

    return toReturn;
}

/**
 * @description Normalize given link
 * @example normalizeLink("http://www.example.com/flowers/../../concepts/summerFlowers.html")="concepts/summerFlowers.html"
 * @param originalHref - HREF that will be normalized
 */
function normalizeLink(originalHref) {
    var relLink = originalHref;
    var logStr = '';
        
    if (startsWith(relLink, whUrl)) {
        relLink = relLink.substr(whUrl.length);
    }

    var toReturn = stripUri(relLink);
    info(logStr + 'normalizeLink(' + originalHref + ')=' + toReturn);
    return toReturn;
}

/**
 * @description Opens a page file(topic) from Search tab and highlights a word from it.
 * @param page - the page that will be opened
 * @param words - the searched words that will be highlighted
 */
function openAndHighlight(page, words) {
    searchedWords = words;
    debug('openAndHighlight(' + page + ',' + words.join(':') + ');');
    if( withFrames ){
        // with frames
        parent.termsToHighlight = words;
        parent.frames['contentwin'].location = page;
    } else {
        if (page != lastLoadedPage) {
            redirect(pageName + window.location.search + '#' + page);
        } else {
            highlightSearchTerm(searchedWords);
        }
    }
    return false;
}

/*
 * {Refactored}
 * De vazut ca merg  link-uri de forma ../../link
 *
 */
function stripUri(uri) {
    var toReturn = '';
    var ret = new Array();
    var bar = uri.split("/");
    //var reti = -1;
    var i = bar.length;
    for (var i = bar.length; i > 0; i--) {
        if (bar[i] == '..') {
            for (var j = i -1; j >= 0; j--) {
                if (bar[j] != '..' && bar[j] != '^') {
                    bar[j] = '^';
                    bar[i] = '^';
                    break;
                }
            }
        }
    }
    for (var i = 0; i < bar.length; i++) {
        if (bar[i] != '^') {
            toReturn = toReturn + bar[i];
            if (i < bar.length -1) {
                toReturn = toReturn + '/';
            }
        } else {
            if (i == 0) {
                if (bar[i] != '^') {
                    toReturn = toReturn + '/';
                }
            }
        }
    }
    log.debug('stripUri(' + uri + ')=' + toReturn);
    return toReturn;
}

/*
 * Set log level to 0 if we have param log=true in CGI
 */
if (location.search.indexOf('log=true') != -1) {
    log.setLevel(0);
}

// OVERWRITES old selecor
jQuery.expr[ ':'].contains = function (a, i, m) {
    return jQuery(a).text().toUpperCase().indexOf(m[3].toUpperCase()) >= 0;
};

/**
 * @description Highlight terms
 * @param what - text that will be highlighted
 * @param spanClass - class that will be applied to searched text
 */
$.fn.highlightContent = function (what, spanClass) {
    return this.each(function () {
        var container = this,
        content = container.innerHTML,
        pattern = new RegExp('(>[^<.]*)(' + what + ')([^<.]*)', 'g'),
        replaceWith = '$1<span ' + (spanClass ? 'class="' + spanClass + '"': '') + '">$2</span>$3',
        highlighted = content.replace(pattern, replaceWith);
        container.innerHTML = highlighted;
    });
}