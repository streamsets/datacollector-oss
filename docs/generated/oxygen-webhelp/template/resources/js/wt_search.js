/**
 * @description Search using Google Search if it is available, otherwise use our search engine to execute the query
 * @return {boolean} Always return false
 */
function executeQuery() {
    var input = document.getElementById('textToSearch');
    try {
        var element = google.search.cse.element.getElement('searchresults-only0');
    } catch (e) {
        debug(e);
    }
    if (element != undefined) {
        if (input.value == '') {
            element.clearAllResults();
        } else {
            element.execute(input.value);
        }
    } else {
        SearchToc($("#textToSearch").val());
    }

    return false;
}

function clearHighlights() {

}

$(document).ready(function () {
    $('.wh_indexterms_link').find('a').text('');
    
    $('.gcse-searchresults-only').attr('data-queryParameterName', 'searchQuery');
});