/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*

Oxygen WebHelp Plugin
Copyright (c) 1998-2016 Syncro Soft SRL, Romania.  All rights reserved.

*/

/**
 * @description Get location used for "Link to this page"
 * @param currentUrl URL of current loaded page (frame)
 * @returns {string|*}
 */
function getPath(currentUrl) {
    //With Frames
    if (/MSIE (\d+\.\d+);/.test(navigator.userAgent) && location.hostname == '' && currentUrl.search("/") == '0') {
        currentUrl = currentUrl.substr(1);
    }
    path = prefix + "?q=" + currentUrl;
    return path;
}

/**
 * @description Highlight searched words
 * @param words {array} words to be highlighted
 */
function highlightSearchTerm(words) {
    if (top==self) {
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
        try {
            if (parent.termsToHighlight != null) {
                // highlight each term in the content view
                for (i = 0; i < parent.termsToHighlight.length; i++) {
                    $(window.parent.contentwin.document).find('body').highlight(parent.termsToHighlight[i]);
                }
            }
        } catch (e) {
            debug(e);
        }
    }
}

/**
 * @description Rewrite title of the page to contain topic title (EXM-30681)
 */
function rewriteTitle() {
    var tocTitle = window.parent.tocwin.document.title;
    var topicTitle = window.parent.contentwin.document.title;

    window.parent.document.title = tocTitle + " - " + topicTitle;
}

$(document).ready(function () {
    $('#permalink').show();
    if ($('#permalink').length > 0) {
        if (window.top !== window.self) {
            try {
                if (window.parent.location.protocol != 'file:' && typeof window.parent.location.protocol != 'undefined') {
                    $('#permalink>a').attr('href', window.parent.location.pathname + '?q=' + window.location.pathname);
                    $('#permalink>a').attr('target', '_blank');
                } else {
                    $('#permalink').hide();
                }
            } catch (e) {
                
            }
        } else {
            window.location.href=getPath(location.pathname);
        }
    }
    
    // Expand toc in case there are frames.
    // Rewrite page title
    if (top !== self && window.parent.tocwin) {
       try {
            window.parent.tocwin.markSelectItem(window.location.href);
            setTimeout(rewriteTitle, 10);
        } catch (e) {
            
        }
    }

    // Click on navigation links without text 	     
    $('.navparent,.navprev,.navnext').unbind('click').bind('click', function(){
        $(this).find('a')[0].click();
	});

	/**
     * @description Scroll to anchor. Get anchor from iframe source and scroll to this.
     *              This is necessary because Chrome doesn't scroll to this
     */
    function scrollToAnchor() {
        var anchor = window.location.hash;
        var position = $(anchor).position();
        if(position!==undefined) {
            $(window).scrollTop(position.top);
        }
    }

	/**
     * Invoke scrollToAnchor after document is completely loaded
     */
    setTimeout(scrollToAnchor, 100);

});