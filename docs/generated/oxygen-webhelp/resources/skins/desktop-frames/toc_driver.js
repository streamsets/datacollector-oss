/*

Oxygen Webhelp plugin
Copyright (c) 1998-2014 Syncro Soft SRL, Romania.  All rights reserved.
Licensed under the terms stated in the license file EULA_Webhelp.txt
available in the base directory of this Oxygen Webhelp plugin.

 */
 
/**
 * {Refactored}
 * @description Marks the current page in TOC
 * @param url - the page that is marked as selected in TOC
 */
function markSelectItem(url) {
    debug('markSelectItem(', url);
    url = normalizeLink(url);
    if ($.cookie("wh_pn") !== undefined && parseInt($.cookie("wh_pn")) > -1 && $.cookie("wh_pn") != "") {
        currentTOCSelection = parseInt($.cookie("wh_pn"));
    } else {
        currentTOCSelection = "none";
    }
    
    if (startsWith(url, '../')) {
        url = url.substr(url.lastIndexOf('../') + 3);
    }
    
    var relp = window.location.pathname.substring(0, window.location.pathname.lastIndexOf('/'));
    debug('relp:' + relp);
    var toFind = url.indexOf("#") != -1 ? url.substring(0, url.indexOf("#")): url;
    debug('markSelectItem(' + toFind + ') - loaded');
    
    if (currentTOCSelection != "none") {
        var newloc = '#contentBlock a[href^="' + toFind + '"]';
        var closest = 65000;
        var diff = 65000;
        $(newloc).each(function () {
            if (Math.abs($(this).parents('li').index('li') - currentTOCSelection) < diff) {
                diff = Math.abs($(this).parents('li').index('li') - currentTOCSelection);
                closest = $(this).parents('li').index('li');
            }
        });
        var loc = '#contentBlock li:eq(' + currentTOCSelection + ') a[href="' + toFind + '"]';
        if ($(loc).length == 0) {
            loc = '#contentBlock li:eq(' + closest + ') a[href="' + toFind + '"]';
        }
        $.cookie("wh_pn", closest);
    } else {
        var loc = '#contentBlock a[href^="' + toFind + '"]';
    }
    
    if ($(loc).length == 0) {
        loc = '#contentBlock a[href^="' + toFind + '"]';
    }
    log.info('search:' + toFind);
    lastLoadedPage = url;
    if (lastLoadedPage != "") {
        toggleItem($(loc).first().parent(), true);
    }
    var item = $(loc).first();
    $('#contentBlock li span').removeClass('menuItemSelected');
    
    showParents();
    item.parent('li span').addClass('menuItemSelected');

    // Scroll TOC to make selectedItem visible
    if($(".menuItemSelected").length>0) {
        if(parseInt($(".menuItemSelected").offset().top)<$(window).scrollTop()) {
            $(window).scrollTop(parseInt($(".menuItemSelected").offset().top));
        } else if (eval($(".menuItemSelected").offset().top+$(".menuItemSelected").height())>eval($(window).scrollTop()+$(window).height())) {
            $(window).scrollTop(eval($(".menuItemSelected").offset().top - $(window).height() + 2*$(".menuItemSelected").height()));
        }
    }
}