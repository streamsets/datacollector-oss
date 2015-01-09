/**
 * Editing Text In-Place using HTML5 ContentEditable
 */

angular.module('pipelineAgentApp.commonDirectives')
  .directive('contenteditable', function() {
    return {
      restrict: 'A',
      require: 'ngModel',
      link: function(scope, element, attrs, ngModel) {

        function read() {
          ngModel.$setViewValue(element.html());
        }

        ngModel.$render = function() {
          if(ngModel.$viewValue !== undefined || ngModel.$viewValue !== null) {
            element.html(ngModel.$viewValue + '');
          } else {
            element.html('');
          }
        };

        element.bind('input', function() {
          scope.$apply(read);
        });
      }
    };
  });