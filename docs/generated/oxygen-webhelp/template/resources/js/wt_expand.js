/**
 * Created by alin_balasa on 12-May-16.
 */

var selectors = {
    /* Selectors for the nodes that will contain an expand/collapse button. */
    "expand_buttons" : [
        /* Table caption */
        "table > caption",
        /* Article title */
        ".topic > .title",
        /* Section title */
        ".sectiontitle",
        /* Index terms groups */
        ".wh_term_group > .wh_first_letter"
    ]
};

/**
 * Add expand-collapse support.
 */
$(document).ready(function () {
    /* Add the expand/collapse buttons. */
    selectors.expand_buttons.forEach(
        function(selector) {
            var matchedNodes =  $(document).find(selector);
            // Add the element with expand/collapse capabilities
            matchedNodes.prepend("<span class=\"wh_expand_btn expanded\"/>");
            markHiddenSiblingsAsNotExpandable(matchedNodes);
        }
    );

    /* Expand / collapse support for the marked content */
    $(document).find('.wh_expand_btn').click(function(event){

        // Change the button state
        $(this).toggleClass("expanded");
        // Will expand-collapse the siblings of the parent node, excepting the ones that were marked otherwise
        var siblings = $(this).parent().siblings(':not(.wh_not_expandable)');
        var tagName = $(this).prop("tagName");
        if (tagName == "CAPTION") {
            // The table does not have display:block, so it will not slide.
            // In this case we'll just hide it
            siblings.toggle();
        } else {
            siblings.slideToggle("1000");
        }

        event.stopImmediatePropagation();
        return false;
    });
});

/**
 * Marks the hidden siblings of the matched nodes as being not expandable.
 *
 * @param nodes The matched nodes.
 */
function markHiddenSiblingsAsNotExpandable(nodes) {
    var siblings = nodes.siblings(":hidden");
    siblings.addClass("wh_not_expandable");
}