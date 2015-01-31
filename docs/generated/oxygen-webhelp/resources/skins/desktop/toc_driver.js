/**
 * @description If Chrome and page is local redirect to index_frames.html
 */
$(document).ready(function () {
    debug("document ready ...");

    // Add @title to page title element (used to rewrite page title to contain topic title)
    $("title").attr("title", $("title").html());

    /**
     * @description Split page in leftPane and rightPane
     */
    $("#splitterContainer").splitter({
        minAsize: 0,
        maxAsize: 600,
        splitVertical: true,
        A: $('#leftPane'),
        B: $('#rightPane'),
        closeableto: 0,
        animSpeed: 100
    });

    /**
     * @description Action to take on iframe unload
     */
    $('#frm').unload(function (ev) {
        ev.preventDefault();
        return false;
    });
    loaded = true;

    showMenu('content');


    $('#iList a').each(function () {
        var old = $(this).attr('href');
        $(this).attr('href', '#' + normalizeLink(old));
        $(this).removeAttr('target');
    });
    if (!notLocalChrome) {
        var warningMsg = 'Chrome limits JavaScript functionality when a page is loaded from the local disk. This prevents the normal help viewer from loading.\nYou will be redirected to the frameset version of the help.';
        if (confirm(warningMsg)) {
            // using Chrome to read local files
            redirect('index_frames.html');
        } else {
            alert ("Not all features are enabled when using Google Chrome for webhelp loaded from local file system!");
            var warningSign = '<span id="warningSign"><img src="oxygen-webhelp/resources/img/warning.png" alt="warning" border="0"></span>';
            $('#productTitle .framesLink').append(warningSign);
            $('#warningSign').mouseenter(function () {
                $('#warning').show();
            });
            $('#warningSign').mouseleave(function () {
                $('#warning').hide();
            });
            var warning = '<div id="warning">Not all features will be enabled using Google Chrome for webhelp loaded from local file system!</div>';
            $('#productTitle .framesLink').append(warning);
        };
    }

//    toggleLeft();
});

/**
 * @description Print iframe content
 * @param id Iframe id
 * @return {boolean} Always return false
 */
function printFrame(id) {
    var frm = document.getElementById(id).contentWindow;
    frm.focus();// focus on contentWindow is needed on some ie versions
    frm.print();
    return false;
}

/**
 * @description If CGI contains the q param will redirect the user to the topic specified in the param value
 */
if (location.search.indexOf("?q=") == 0) {
    debug('search:' + location.search + ' hwDir:' + wh.directory);
    var pos = 0;
    var newLink = whUrl + pageName;
    if (islocal) {
        pos = location.search.lastIndexOf(wh.directory.substring(1));
        newLink = newLink + "#" + location.search.substring(pos + wh.directory.length -1);
    } else {
        pos = location.search.lastIndexOf(wh.directory);
        newLink = newLink + "#" + location.search.substring(pos + wh.directory.length);
    }
    debug('redirect to ' + newLink);
    redirect(newLink);
}

/**
 * @description Show content / search / index tabs from left pane
 */
function showDivs() {
    debug('showDivs()');
    if (! showAll) {
        $("#indexList").show();
        $("#indexList div").show();
        showAll = true;
    }
    showScrolls();
}

/**
 * @description Load dynamicURL to iFrame
 * @param dynamicURL - URL to be loaded
 */
function loadIframe(dynamicURL) {
    debug('loadIframe(' + dynamicURL + ')');
    var anchor = "";
    if (dynamicURL.indexOf("#") > 0) {
        //anchor
        anchor = dynamicURL.substr(dynamicURL.indexOf("#"));
        anchor = anchor.substr(1);
    }

    var tempLink = new String(dynamicURL);
    if (tempLink.indexOf('?') !== -1) {
        tempLink = tempLink.substr(0, tempLink.indexOf('?'));
        var tempLinks = tempLink.split("/");
        tempLink = tempLinks[tempLinks.length-1];
    }
    if (tempLink.indexOf('.') != -1 && tempLink.indexOf('.htm') === -1 && tempLink.indexOf('.xhtm') === -1) {
        debug('open in new window: ' + tempLink);
        window.open(tempLink, '_blank');
        return;
    }

    $('#frm').remove();
    var iframeHeaderCell = document.getElementById('rightPane');
    var iframeHeader = document.createElement('IFRAME');
    iframeHeader.id = 'frm';
    iframeHeader.src = dynamicURL;
    iframeHeader.frameBorder = 0;
    iframeHeader.align = 'center';
    iframeHeader.valign = 'top'
    iframeHeader.marginwidth = 0;
    iframeHeader.marginheight = 0;
    iframeHeader.hspace = 0;
    iframeHeader.vspace = 0;
    
    iframeHeader.style.display = 'none';
    iframeHeaderCell.appendChild(iframeHeader);

    $('#frm').load(function () {
        setTimeout(function(){
            tocWidth = parseInt($('#tocMenu').css('width'));
            navLinksWidth = parseInt($('#navigationLinks').css('width'));
            breadCrumbWidth = parseInt($('#breadcrumbLinks').css('width'));
            var withFrames = window.parent.frames.length>1?true:false;
            var navLinks = withFrames?$(top.frames[ "contentwin"].document).find(".navparent a,.navprev a,.navnext a"):$(".navparent a,.navprev a,.navnext a");
            navLinks.hide();
            navLinksWidthMin = parseInt($('#navigationLinks').css('width'));
            resizeContent();

            // Rewrite page title to contain topic title (EXM-30681)
            $("title").html($("title").attr("title") + " - " + $("#frm").contents().find("title").html());

            // EXM-31118 Rewrite anchors relative to current loaded frame to contain frame link
            var links = $('#frm').contents().find('a');
            var currentLocation = $('#frm').attr('src');
            if(currentLocation.indexOf('#')>0) {
                currentLocation = currentLocation.substring(0, currentLocation.indexOf('#'));
            }
            $.each(links, function(){
                var link = $(this).attr('href');
                if(link.indexOf('#')==0) {
                    $(this).attr('href', currentLocation+link);
                }
            });
        }, 10);
        debug('#frm.load');
        if (notLocalChrome) {
            debug('#frm.load 1');
            $('#frm').contents().find('.navfooter').before('<div class="footer_separator" style="border-top: 1px solid #EEE;"><!-- --></div>').hide();
            $('#frm').contents().find('.frames').hide();
            
            $('#frm').contents().find('a, area').click(function (ev) {
                var hrf = $(this).attr('href');
                /*EXM-26476 The mailto protocol is not properly detected by the parseUri utility.*/
                if (hrf && hrf.length > 6 && hrf.substring(0, 7) == "mailto:") {
                    return;
                }
                
                /* EXM-27247 Ignore <a> elements with the "target" attribute.*/
                var target = $(this).attr('target');
                if (target) {
                    // Let the default processing take place.
                    return;
                }

                var p = parseUri(hrf);
                if (p.protocol != '') {
                    //Let the default processing take place.
                    return;
                } else {
                    // EXM-27800 Decide to ignore or keep iframeDir in the path
                    // of the target of the <a> link based on ID of parent div element.
                    var newUrl = pageName + location.search + '#' +
                    processHref(hrf, $(this).closest("div").attr("id"));
                    window.location.href = whUrl + newUrl;
                    ev.preventDefault();
                }
                return false;
            });

            debug('#frm.load 2');
            if (navigator.appVersion.indexOf("MSIE 7.") == -1) {
              $('#navigationLinks').html($('#frm').contents().find('div.navheader .navparent, div.navheader .navprev, div.navheader .navnext'));
              $('#frm').contents().find('div.navheader').hide();
            } else {
              //  $('#navigationLinks').html($('#frm').contents().find('div.navheader').parent().html());

                $('#frm').contents().find("table.nav").find("tr:first-child").hide();
            }
            /**
             * Nu mai ascundem toc-ul - ii scadem relevanta din indexer
             * EXM-25565
             */
            //$('#frm').contents().find('.toc').hide();
            if (navigator.appVersion.indexOf("MSIE 7.") == -1) {
              $('#breadcrumbLinks').html($('#frm').contents().find('table.nav a.navheader_parent_path'));
            } else {

            }
            // normalize links
            $('#breadcrumbLinks a, #navigationLinks a').each(function () {
                var oldLink = $(this).attr('href');
                // we generate from oxygen '../'s in from of link
                while (oldLink.indexOf('../') == 0) {
                    info('strip \'../\' from ' + oldLink);
                    oldLink = oldLink.substring(3);
                }
                $(this).attr('href', stripUri(oldLink));
            });
            if (navigator.appVersion.indexOf("MSIE 7.") == -1) {
              showParents();
              $('#frm').contents().find('table.nav').hide();
            } else {

            }
        }
        $('#frm').show();
        $('div.tooltip').remove();
        $('#breadcrumbLinks').find('a').after('<span>&nbsp;/&nbsp;</span>');
        $('#breadcrumbLinks').find('span').last().html('&nbsp;&nbsp;');
        $('.navparent a').click(function () {
            if ($.cookie("wh_pn") != "" && $.cookie("wh_pn") !== undefined && $.cookie("wh_pn") !== null) {
                currentTOCSelection = parseInt($.cookie("wh_pn"));
                parentTOCSelection = $('#tree li:eq(' + currentTOCSelection + ')').parents('ul').parents('li').index('li');
                $.cookie('wh_pn', parentTOCSelection);
            }
        });
        $('.navprev a').click(function () {
            prevTOCSelection = parseInt($.cookie('wh_pn')) -1;
            $.cookie('wh_pn', prevTOCSelection);
        });
        $('.navnext a').click(function () {
            nextTOCSelection = parseInt($.cookie('wh_pn')) + 1;
            $.cookie('wh_pn', nextTOCSelection);
        });

        highlightSearchTerm(searchedWords);

        // Click on navigation links without text
	    $('.navparent,.navprev,.navnext').unbind('click').bind('click', function(){
	        $(this).find('a').trigger('click');
	    });
    });
}

/**
 * @description Add special class to selected item from TOC
 * @param hrl - anchor of item that will be selected
 * @param startWithMatch when this is true will select item that begins with hrl
 * @returns {boolean} TRUE if special class is added with success, FALSE otherwise
 */
function markSelectItem(hrl, startWithMatch) {
    debug("hrl: " + hrl);
    
    if ($.cookie("wh_pn") !== undefined && parseInt($.cookie("wh_pn")) > -1 && $.cookie("wh_pn") != "") {
        currentTOCSelection = parseInt($.cookie("wh_pn"));
    } else {
        currentTOCSelection = "none";
    }
    debug('markSelectItem(' + hrl + ',' + startWithMatch + ')');
    $('#contentBlock li span').removeClass('menuItemSelected');
    if (startWithMatch == null || typeof startWithMatch === 'undefined') {
        startWithMatch = false;
        debug('forceMatch - false');
    }
    
    var toReturn = false;
    if (loaded) {
        debug('markSelectItem(..loaded..)');
        var loc = '#contentBlock li:eq(' + currentTOCSelection + ') a[href="#' + hrl + '"]';
        if (startWithMatch) {
            loc = '#contentBlock li:eq(' + currentTOCSelection + ') a[href^="#' + hrl + '#"]';
        }
        if ($(loc).length != 0) {
            if (window.location.hash != "") {
                debug("hash found - toggle !");
                toggleItem($(loc).first().parent(), true);
            } else {
                debug("no hash found");
            }
            if (hrl.indexOf("!_") == 0) {
                // do not mark selected - fake link found
            } else {
                $('#contentBlock li span').removeClass('menuItemSelected');
                var item = $(loc).first();
                item.parent('li span').addClass('menuItemSelected');
                $.cookie("wh_pn", $(loc).first().parents('li').index('li'));
            }
            toReturn = true;
        } else {
            var loc = '#contentBlock a[href="#' + hrl + '"]';
            if (startWithMatch) {
                loc = '#contentBlock a[href^="#' + hrl + '#"]';
            }
            if ($(loc).length != 0) {
                if (window.location.hash != "") {
                    debug("hash found - toggle !");
                    toggleItem($(loc).first().parent(), true);
                } else {
                    debug("no hash found");
                }
                if (hrl.indexOf("!_") == 0) {
                    // do not mark selected - fake link found
                } else {
                    $('#contentBlock li span').removeClass('menuItemSelected');
                    var item = $(loc).first();
                    item.parent('li span').addClass('menuItemSelected');
                    $.cookie("wh_pn", $(loc).first().parents('li').index('li'));
                }
                toReturn = true;
            }
        }
    }
    debug('markSelectItem(...) =' + toReturn);
    return toReturn;
}

/**
 * @description Toggle left pane
 */
function toggleLeft() {
    var widthLeft = $('#leftPane').css('width')
    widthLeft = widthLeft.substr(0, widthLeft.length -2);
    debug('toggleLeft() - left=' + widthLeft);
    if (Math.round(widthLeft) <= 0) {
        $("#splitterContainer .splitbuttonV").trigger("mousedown");
        //trigger the button
        if ($("#splitterContainer .splitbuttonV").hasClass('invert')) {
            $("#splitterContainer .splitbuttonV").removeClass('invert');
        }
        if (! $("#splitterContainer .splitbuttonV").hasClass('splitbuttonV')) {
            $("#splitterContainer .splitbuttonV").addClass('splitbuttonV');
        }
    }
}

/**
 * @description Load new page in content window
 * @param link - link of page to be loaded
 */
function load(link) {
    if (loaded == true) {
        debug('document ready  ..');
    } else {
        debug('document not ready  ..');
        return;
    }
    var hash = "";
    if (link.indexOf("#") > 0) {
        hash = link.substr(link.indexOf("#") + 1);
    }
    
    
    if (hash == '') {
        $('#contentBlock li a').each(function (index, domEle) {
            if ($(this).attr('href').indexOf('#!_') != 0) {
                link = pageName + $(this).attr('href');
                debug('Found first link from toc: ' + link);
                return false;
            }
        });
    }
    
    if (link.indexOf("#") > 0 || pageName == '') {
        var hr = link;
        debug("index of # in " + link + " is at " + link.indexOf("#"));
        //if (link.indexOf("#")>0){
        hr = link.substr(link.indexOf("#") + 1);
        debug(' link w hash : ' + link + ' > ' + hr);
        //hr=hr.substring(1);
        /*
        }else{
        hr="3";
        }
         */
        debug(' link @ hash : ' + hr);
        var hrl = hr;
        if (hr.indexOf("#") > 0) {
            hrl = hr.substr(0, hr.indexOf("#"));
        }
        
        if (! markSelectItem(hr)) {
            if (! markSelectItem(hrl)) {
                markSelectItem(hr, true);
            }
        }

        // Scroll to make selectedItem visible
        if($(".menuItemSelected").length>0) {
            if(parseInt($(".menuItemSelected").offset().top+$(".menuItemSelected").height()) > eval($("#leftPane").offset().top+$("#leftPane").height())) {
                var sTo = $(".menuItemSelected").offset().top - eval($("#leftPane").offset().top+$("#leftPane").height()) + $("#leftPane").scrollTop();
                $("#leftPane").scrollTop(eval(sTo + 2*$(".menuItemSelected").height()));
            } else if( $(".menuItemSelected").offset().top < $("#leftPane").offset().top ){
                var sTo = $(".menuItemSelected").offset().top<0?eval($("#leftPane").scrollTop() - Math.abs($(".menuItemSelected").offset().top) - $("#leftPane").offset().top):eval($("#leftPane").scrollTop() - ($("#leftPane").offset().top - $(".menuItemSelected").offset().top));
                $("#leftPane").scrollTop(sTo);
            }
        }

        if (hr.indexOf("!_") == 0) {
            //fake link found
        } else {
            if (hr && (hr != lastLoadedPage)) {
                lastLoadedPage = hr;
                debug('lastLoadedPage=' + hr);
                loadIframe(hr);
                var p = parseUri(hr);
                debug('load: parseUri(hr)=', p);
                iframeDir = p.host + p.directory;
                if (p.protocol == '' && p.path == '' && p.directory == '') {
                    iframeDir = '';
                }
                debug('iframeDir=' + p.host + '+' + p.directory);
            } else {
                //already loaded
            }
        }
        //has hash
    } else {
        debug(' link w no hash : ' + link);
    }
}

/**
 * @description Remove "../" from hrf
 * @example processHref(../../concepts/glossaryGenus.html)=concepts/glossaryGenus.html
 * @param hrf - the link hash
 * @param idName - ID of clicked link section. Used only to detect the links from
 * 'navigationLinks' and 'breadcrumbLinks'
 * @returns {string}
 */
function processHref(hrf, idName) {
    // EXM-27800 Decide to ignore or keep iframeDir in the path
    // of the target of the <a> link based on ID of parent div element.
    var toReturn = "";
    if (idName === "navigationLinks" || idName === "breadcrumbLinks") {
        toReturn = hrf;
    } else {
        var pp = parseUri(hrf);
        if(pp.host=="") {
            pp.host = parseUri((location.hash).substring(1)).file;
        }
        var toReturn = pp.host + pp.directory + pp.file;

        debug('parseUri(' + hrf + ')=' + pp.host + '+' + pp.directory + '+' + pp.file);
        debug('iframeDir=' + iframeDir);
        count = (hrf.match(new RegExp("\\.\\.\\/", "g")) ||[]).length;
        toReturn = toReturn.replace(new RegExp("\\.\\.\\/", "g"), '');
        var dirParts = iframeDir.split("/");
        for (i = 0; i < dirParts.length; i++) {
            if (dirParts[i] == "") {
                dirParts.splice(i, 1);
            }
        }
        
        var dir = "";
        for (i = 0; i <(dirParts.length - count);
        i++) {
            dir += dirParts[i] + "/";
        }

        toReturn = dir + toReturn;
        if (pp.anchor != "") {
            toReturn = toReturn + "#" + pp.anchor;
        }
    }

    debug('processHref(' + hrf + ')=' + toReturn);
    return toReturn;
}

var currentHref = window.location.href;
$(function () {
    $(window).on("hashchange", function(e) {
        var newHref = window.location.href;
        var textAreaContent = $("#frm").contents().find(".cleditorMain").find("iframe").contents().find("body").html();

        if(textAreaContent!='' && textAreaContent!==undefined && $("#frm").contents().find("#newComment").is(":visible") && currentHref!=newHref) {
            if (confirm(getLocalization("label.Unsaved"))) {
                currentHref = window.location.href;
                load(window.location.href);
            } else {
                window.location.href = currentHref;
            }

        } else {
            currentHref = window.location.href;
            load(window.location.href);
        }
    });
    // Since the event is only triggered when the hash changes, we need to trigger
    // the event now, to handle the hash the page may have loaded with.
    $(window).hashchange();
});

if (!("onhashchange" in window) && ($.browser.msie)) {
    //IE and browsers that don't support hashchange
    $('#contentBlock a').bind('click', function () {
        var hash = $(this).attr('href');
        debug('#contentBlock a click(' + hash + ')');
        load(hash);
    });
}