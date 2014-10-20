angular.module('pipelineAgentApp.tabDirectives', [])
  .directive('showtab',
    function () {
      return {
        link: function (scope, element) {
          element.click(function(e) {
            e.preventDefault();
            $(element).tab('show');
          });
        }
      };
    }
  );