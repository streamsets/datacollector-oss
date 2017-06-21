/*

Oxygen WebHelp Plugin
Copyright (c) 1998-2016 Syncro Soft SRL, Romania.  All rights reserved.

*/
 
$(document).ready(function(){
    $("#searchForm").css("background-color", $("#leftPane").css("background-color"));

		$('#customLogo').click(
        function(event) {
            $(this).attr('target', '_parent');
            return true;
        }
    );
});

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
                var findIndexFor = $(this).closest('li');
                closest = $('#contentBlock li').index(findIndexFor);
            }
        });
        var loc = '#contentBlock li:eq(' + currentTOCSelection + ') a[href="' + toFind + '"]';
        if ($(loc).length == 0) {
            loc = '#contentBlock li:eq(' + closest + ') a[href="' + toFind + '"]';
        }
        
        if ( wh.protocol == 'https' ) {
            $.cookie('wh_pn', closest, { secure: true });
        } else {
            $.cookie('wh_pn', closest);
        }
        
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
    scrollToVisibleItem();
}