/*

Oxygen Webhelp plugin
Copyright (c) 1998-2014 Syncro Soft SRL, Romania.  All rights reserved.
Licensed under the terms stated in the license file EULA_Webhelp.txt
available in the base directory of this Oxygen Webhelp plugin.

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

// If page is local and is viewed in Chrome
var notLocalChrome = verifyBrowser();

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

function openTopic(anchor) {
    if ((anchor).attr('target') === undefined) { 
            /* Remove the old selection and add selection to the clicked item */
            $('#contentBlock li span').removeClass('menuItemSelected');
            (anchor).parent('li span').addClass('menuItemSelected');

            /* Calculate index of selected item and write value to cookie */
            index = (anchor).parents('li').index('li');
            $.cookie('wh_pn', index);

            /* Redirect to requested page */
            redirect((anchor).attr('href'));
    } else {
        window.open((anchor).attr('href'), (anchor).attr('target'));
    }
}


$(document).ready(function () {
    $('#preload').hide();

    /**
     * {Refactored}
     * @description Selects the clicked item
     */
    $('#contentBlock li a').click(function() {
        if ($(this).attr('href').indexOf('#!_') == 0) {
            // expand topichead
            toggleItem($(this));
            // find first descendant that is a topicref with href attribute
            // and open that topicref
            $(this).parents('li').first().find('li a').each(function() {
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
    });

    // Show clicked TAB
    $('.tab').click(function () {
        showMenu($(this).attr('id'));
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

    // Toggle clicked item from TOC
    $('#contentBlock li>span').click(function () {
        toggleItem($(this));
    })

    // Determine if li element have submenus and sets corresponded class
    $('#contentBlock li>span').each(function () {
        if ($(this).parent().find('>ul').size() > 0) {
            $(this).addClass('hasSubMenuClosed');
        } else {
            $(this).addClass('topic');
        }
    })

    debug('discover foldables ' + $('#tree > ul li > span').size() + ' - ' + $('#tree > ul li > span.topic').size());

    // Calculate number of foldable nodes and show/hide expand buttons
    noFoldableNodes = $('#tree > ul li > span').size() - $('#tree > ul li > span.topic').size();
    showHideExpandButtons();

    // Set title of expand/collapse all buttons
    $('#expandAllLink').attr('title', getLocalization("ExpandAll"));
    $('#collapseAllLink').attr('title', getLocalization("CollapseAll"));

    $("#searchForm").css("background-color", $("#leftPane").css("background-color"));
    $("#indexForm").css("background-color", $("#leftPane").css("background-color"));

    /**
     * Keep Collapse / Expand buttons in the right sideof the left pane
     */
    $("#bck_toc").bind("scroll", function() {
        var scrollX = $("#bck_toc").scrollLeft();
        $("#expnd").css("left", scrollX);
    });
});

$(window).resize(function(){
    if ($("#searchBlock").is(":visible")) {
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#searchForm").height())-parseInt($("#searchForm").css("padding-top"))-parseInt($("#searchForm").css("padding-bottom"));
        $("#searchResults").css("height", hAvailable);
    }
    if ($("#indexBlock").is(":visible")) {
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#indexForm").height())-parseInt($("#indexForm").css("padding-top"))-parseInt($("#indexForm").css("padding-bottom"));
        $("#iList").css("height", hAvailable);
    }
    if ($("#contentBlock").is(":visible")) {
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#bck_toc").css("padding-top"));
        $("#bck_toc").css("height", hAvailable);
    }
});


$(window.parent).resize(function () {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(resizeContent, 10);
});

/**
 * {Refactored}
 * @description Redirect browser to a new address
 * @param link - the url to be redirected to
 */
function redirect(link) {
    debug ('redirect(' + link + ');');
    // Determine the webhelp type: no-frames or frames
    if (self == top) {
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
        var contentLinkText = getLocalization("Content");
        var searchLinkText = getLocalization("Search");
        var indexLinkText = getLocalization("Index");
        var IndexPlaceholderText = getLocalization("IndexFilterPlaceholder");
        var SearchPlaceholderText = getLocalization("Keywords");

        var tabs = new Array("content", "search", "index");
        for (var i = 0; i < tabs.length; i++) {
            var currentTabId = tabs[i];
            // Generates menu tabs
            if ($("#"+currentTabId).length > 0) {
                debug('Init tab with name: ' + currentTabId);
                $("#"+currentTabId).html(eval(currentTabId + "LinkText"));
            } else {
                info('init no tab found with name: ' + currentTabId);
            }
            tabsInitialized = true;
        }

        $("#id_search").attr("placeholder", IndexPlaceholderText);
        $("#textToSearch").attr("placeholder", SearchPlaceholderText);
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
        parent.termsToHighlight = Array();
    }

    initTabs();
    var tabs = new Array("content", "search", "index");
    for (var i = 0; i < tabs.length; i++) {
        var currentTabId = tabs[i];
        // generates menu tabs
        if ($("#" + currentTabId).length > 0) {
            // show selected block
            selectedBlock = displayTab + "Block";
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
        searchedWords = "";
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#bck_toc").css("padding-top"));
        $("#bck_toc").css("height", hAvailable).css("overflow", "auto");
        $("#contentBlock").append($("#leftPane .footer"));
    }
    if (displayTab == 'search') {
        $('.textToSearch').focus();
        searchedWords = $('#textToSearch').text();
        $("#bck_toc").css("overflow", "hidden")
        $("#bck_toc,#bck_toc #indexBlock").css("height", "100%");
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#searchForm").height())-parseInt($("#searchForm").css("padding-top"))-parseInt($("#searchForm").css("padding-bottom"));
        $("#searchResults").css("height", hAvailable);
        $("#searchResults").append($("#leftPane .footer"));
    }
    if (displayTab == 'index') {
        $('#id_search').focus();
        searchedWords = "";
        $("#bck_toc").css("overflow", "hidden")
        $("#bck_toc,#bck_toc #searchBlock").css("height", "100%");
        var hAvailable = parseInt($("body").height())-parseInt($("#header").height())-parseInt($("#indexForm").height())-parseInt($("#indexForm").css("padding-top"))-parseInt($("#indexForm").css("padding-bottom"));
        $("#iList").css("height", hAvailable);
        $("#iList").append($("#leftPane .footer"));
    }
    // Without Frames: Show left side(toc, search, index) if hidden
    if ($("#frm").length > 0) {
        toggleLeft();
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
    var need = tocWidth + navLinksWidth + breadCrumbWidth;
    var needMin = tocWidth + navLinksWidthMin + breadCrumbWidth;
    debug('NEED: '+need+' | NEED-MIN: '+needMin);
    var withFrames = window.parent.frames.length>1?true:false;

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
                if ($(this).attr('title').length>37) {
                    $(this).text($(this).text().replace(/\./gi, '').substr(0, 37) + "...");
                }
            });
        } else {
            debug('Need to show all breadcrumbs')
            $('#productToolbar .navheader_parent_path').each(function () {
                $(this).text($(this).attr('title'));
            });
        }
    } else {
        debug('Need to show navigation links');
        navHeader.css("min-width", "326px");
        navLinks.show();
    }
    //showScrolls();
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
        if (words != null) {
            // highlight each term in the content view
            $('#frm').contents().find('body').removeHighlight();
            for (i = 0; i < words.length; i++) {
                debug('highlight(' + words[i] + ');');
                $('#frm').contents().find('body').highlight(words[i]);
            }
        }
    } else {
        // For index with frames
        if (parent.termsToHighlight != null) {
            // highlight each term in the content view
            for (i = 0; i < parent.termsToHighlight.length; i++) {
                $('*', window.parent.contentwin.document).highlight(parent.termsToHighlight[i]);
            }
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
            if ($('#tree > ul li > span.hasSubMenuOpened').size() != noFoldableNodes) {
                $('#expandAllLink').show();
            } else {
                $('#expandAllLink').hide();
            }
            if ($('#tree > ul > li > span.hasSubMenuOpened').size() <= 0) {
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
        if ($(loc).parent().find('>ul').size() > 0) {
            $(loc).removeClass('hasSubMenuOpened');
            $(loc).addClass('hasSubMenuClosed');
            $(loc).parent('#contentBlock li').find('>ul').hide();
        }
    } else {
        if ($(loc).parent().find('>ul').size() > 0) {
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
            currentTOCSelection = parseInt($.cookie("wh_pn"));
            prevTOCIndex = currentTOCSelection -1;
            nextTOCIndex = currentTOCSelection + 1;

            // Get the href of the parent / next / previous related to current item
            prevTOCSelection = $('#tree li:eq(' + prevTOCIndex + ')').find('a').attr('href');
            nextTOCSelection = $('#tree li:eq(' + nextTOCIndex + ')').find('a').attr('href');
            parentTOCSelection = $('#tree li:eq(' + currentTOCSelection + ')').parents('ul').parents('li').find('a').attr('href');

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

            // Get the element that contains navigation links and hide irrelevant links
            if (self==top) {
                var navLinks = $('#navigationLinks');
            } else {
                var navLinks = $(top.frames[ "contentwin"].document).find('.nav');
                navLinks = $(navLinks).find('.navheader');

                var footerLinks = $(top.frames[ "contentwin"].document).find('.navfooter');
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
 * @description Normalize given link
 * @example normalizeLink("http://www.example.com/flowers/../../concepts/summerFlowers.html")="concepts/summerFlowers.html"
 * @param originalHref - HREF that will be normalized
 */
function normalizeLink(originalHref) {
    var relLink = originalHref;
    var logStr = '';
    if (! $.support.hrefNormalized) {
        var relp = window.location.pathname.substring(0, window.location.pathname.lastIndexOf('/'));
        //ie7
        logStr = ' IE7 ';
        var srv = window.location.protocol + '//' + window.location.hostname;
        var localHref = parseUri(originalHref);
        
        if (window.location.protocol.toLowerCase() != 'file:' && localHref.protocol.toLowerCase() != '') {
            debug('ie7 file://');
            relLink = originalHref.substring(whUrl.length);
        }
    } else {
        if (startsWith(relLink, whUrl)) {
            relLink = relLink.substr(whUrl.length);
        }
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
    if(top != self){
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
if (location.search.indexOf('?log=true') >= 0) {
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