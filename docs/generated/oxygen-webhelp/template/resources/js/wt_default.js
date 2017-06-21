// Add some Bootstrap classes when document is ready
var highlighted = false;

$(document).ready(function () {

    var searchQuery = '';
    try {
        searchQuery = getParameter('searchQuery');
        searchQuery = decodeURIComponent(searchQuery);
        searchQuery = searchQuery.replace(/\+/g, " ");
        if (searchQuery!='' && searchQuery!==undefined && searchQuery!='undefined') {
            $('#textToSearch').val(searchQuery);
            executeQuery();
        }
    } catch (e) {
        debug(e);
    }

    // If we have a contextID, we must to redirect to the corresponding topic
    var contextId = getParameter('contextId');
    var appname = getParameter('appname');

    if ( contextId != undefined && contextId != "") {
        var scriptTag = document.createElement("script");
        scriptTag.type = "text/javascript";
        scriptTag.src = "context-help-map.js";
        document.getElementsByTagName('head')[0].appendChild(scriptTag);

        var ready = setInterval(function () {
                if (helpContexts != undefined) {
                    for(var i = 0; i < helpContexts.length; i++) {
                        var ctxt = helpContexts[i];
                        if (contextId == ctxt["appid"] && (appname == undefined || appname == ctxt["appname"])) {
                            var path = ctxt["path"];
                            if (path != undefined) {
                                window.location = path;
                            }
                            break;
                        }
                    }
                    clearInterval(ready);
                }
        }, 100);
    }

    // Navigational links and print
    $('#topic_navigation_links .navprev>a').addClass("glyphicon glyphicon-arrow-left");
    $('#topic_navigation_links .navnext>a').addClass("glyphicon glyphicon-arrow-right");
    $('.wh_print_link a').addClass('glyphicon glyphicon-print');
	
	// Hide sideTOC when it is empty
    var sideToc = $('#wh_side_toc');
    if (sideToc !== undefined) {
        var sideTocChildren = sideToc.find('*');
        if (sideTocChildren.length == 0) {
            sideToc.css('display', 'none');

            // The topic content should span on all 12 columns
            sideToc.removeClass('col-lg-4 col-md-4 col-sm-4 col-xs-12');
            var topicContentParent = $('.wh_topic_content').parent();
            if (topicContentParent !== undefined) {
                topicContentParent.removeClass(' col-lg-8 col-md-8 col-sm-8 col-xs-12 ');
                topicContentParent.addClass(' col-lg-12 col-md-12 col-sm-12 col-xs-12 ');
            }
        }
    }

    $(".wh_main_page_toc .wh_main_page_toc_accordion_header").click(function(event) {
        if ($(this).hasClass('expanded')) {
            $(this).removeClass("expanded");
        } else {
            $(".wh_main_page_toc .wh_main_page_toc_accordion_header").removeClass("expanded");
            $(this).addClass("expanded");
        }

        event.stopImmediatePropagation();
        return false;
    });

    $(".wh_main_page_toc a").click(function(event) {
        event.stopImmediatePropagation();
    });

    var dirAttr = $('html').attr('dir');
    var rtlEnabled = false;
    if (dirAttr=='rtl') {
        rtlEnabled = true;
    }

    $('.wh_top_menu').find('li').hover(function(){
        var firstLevelElementWidth = $('.wh_top_menu>ul>li:hover').width();
        var totalWidth = 0;
        $.each($('.wh_top_menu>ul li:hover'), function() {
            totalWidth+=parseInt($(this).width());
        });
        var offsetLeft = parseInt($(this).offset().left);
        var childWidth = 0;
        try {
            childWidth = parseInt($(this).children('ul').width());
        } catch (e) {
            debug(e);
        }
        totalWidth += childWidth - firstLevelElementWidth;
        var index = $('.wh_top_menu ul').index($(this).parent('ul'));
        if (!rtlEnabled) {
            var neededWidth = offsetLeft + totalWidth;
            if (neededWidth > parseInt($(window).width()) && index != 0) {
                $(this).children('ul').css('right', '100%');
                $(this).children('ul').css('left', 'auto');
            } else if (index != 0) {
                $(this).children('ul').css('right', 'auto');
                $(this).children('ul').css('left', '100%');
            }
        } else {
            var leftPositionNeeded = offsetLeft - totalWidth + childWidth;
            if (leftPositionNeeded < 0 && index != 0) {
                $(this).children('ul').css('right', 'auto');
                $(this).children('ul').css('left', '100%');
            } else if (index != 0) {
                $(this).children('ul').css('right', '100%');
                $(this).children('ul').css('left', 'auto');
            }
        }
    });

    highlightSearchTerm();
});

/**
 * @description Log messages and objects value into browser console
 */
function debug(message, object) {
    object = object || "";
    console.log(message, object);
}

/**
 * @description Highlight searched words
 */
function highlightSearchTerm() {
    debug("highlightSearchTerm()");
    if (highlighted) {
        return;
    }
    try {
        var $body = $('.wh_topic_content');
        var $relatedLinks = $('.wh_related_links');
				var $childLinks = $('.wh_child_links');

        $body.removeHighlight();
        $relatedLinks.removeHighlight();
    
        var jsonString = decodeURIComponent(String(getParameter('hl')));
        debug("jsonString: ", jsonString);
        
        if (jsonString !== undefined && jsonString != "") {
            var words = jsonString.split(',');
            debug("words: ", words);
            
            for (var i = 0; i < words.length; i++) {
                debug('highlight(' + words[i] + ');');
                $body.highlight(words[i]);
                $relatedLinks.highlight(words[i]);
								$childLinks.highlight(words[i]);
            }
        }
    }
    catch (e) {
        debug (e);
    }
    highlighted = true;
}

/**
 * @description Returns all available parameters or empty object if no parameters in URL
 * @return {Object} Object containing {key: value} pairs where key is the parameter name and value is the value of parameter
 */
function getParameter(parameter) {
    var whLocation = "";

    try {
        whLocation = window.location;
        var p = parseUri(whLocation);
    } catch (e) {
        debug(e);
    }

    return p.queryKey[parameter];
}

/**
 * Open the link from top_menu when the current group is expanded.
 */
$(".wh_top_menu li").click(function (event) {
    $(".wh_top_menu li").removeClass('active');
    $(this).addClass('active');
    $(this).parents('li').addClass('active');
    event.stopImmediatePropagation();
});

$(".wh_top_menu a").click(function (event) {
    var isTouchEnabled = false;
    try {
        if (document.createEvent("TouchEvent")) {
            isTouchEnabled = true;
        }
    } catch (e) {
        debug(e);
    }
    if ($(window).width() < 767 || isTouchEnabled) {
        var areaExpanded = $(this).closest('li');
        if (areaExpanded.hasClass('active') || areaExpanded.find('li').length == 0) {
            window.location = $(this).attr("href");
            event.preventDefault();
            event.stopImmediatePropagation();
            return false;
        } else {
            event.preventDefault();
        }
    } else {
        return true;
    }
});