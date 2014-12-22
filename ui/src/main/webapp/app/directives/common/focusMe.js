/**
 * Directive for setting focus on input field
 *
 * http://stackoverflow.com/questions/14833326/how-to-set-focus-on-input-field
 *
 */

angular.module('pipelineAgentApp.commonDirectives')
  .directive("focusMe", function($timeout, $parse) {
    return {
      link: function(scope, element, attrs) {
        var model = $parse(attrs.focusMe);
        scope.$watch(model, function(value) {
          if(value === true) {
            $timeout(function() {
              element[0].focus();
            });
          }
        });
      }
    };
  });