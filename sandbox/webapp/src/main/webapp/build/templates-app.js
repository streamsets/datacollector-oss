angular.module('templates-app', ['app/data/data.tpl.html', 'app/flow/flow.tpl.html', 'app/home/home.tpl.html']);

angular.module("app/data/data.tpl.html", []).run(["$templateCache", function($templateCache) {
  $templateCache.put("app/data/data.tpl.html",
    "<h1>Data Page for Analytics</h1>");
}]);

angular.module("app/flow/flow.tpl.html", []).run(["$templateCache", function($templateCache) {
  $templateCache.put("app/flow/flow.tpl.html",
    "<h1>Flow Page for Monitoring Metrics during runtime.</h1>\n" +
    "\n" +
    "<h2> FFFFFFn</h2>");
}]);

angular.module("app/home/home.tpl.html", []).run(["$templateCache", function($templateCache) {
  $templateCache.put("app/home/home.tpl.html",
    "<button type=\"button\" class=\"btn btn-primary btn btn-create-pipeline\" ng-hide=\"config\" ng-click=\"createPipelineAgent()\">\n" +
    "  <span class=\"glyphicon glyphicon-plus\"></span> Create Pipeline Flow\n" +
    "</button>\n" +
    "\n" +
    "<div ng-show=\"config\" >\n" +
    "  <bg-splitter orientation=\"vertical\">\n" +
    "    <bg-pane min-size=\"100\" class=\"canvas-pane\">\n" +
    "      Canvas Pane\n" +
    "    </bg-pane>\n" +
    "    <bg-pane min-size=\"50\" class=\"detail-pane\">\n" +
    "      Detail Pane\n" +
    "    </bg-pane>\n" +
    "  </bg-splitter>\n" +
    "</div>\n" +
    "");
}]);
