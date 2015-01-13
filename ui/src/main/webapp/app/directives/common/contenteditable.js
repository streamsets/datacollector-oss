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

        element.on({
          'input': function() {
             scope.$apply(read);
          },
          'keypress': function(e) {
            if(e.which === 13) {
              $('<div class="temp-contenteditable-el" contenteditable="true"></div>')
                .appendTo('body').focus().remove();
              return false;
            }
            return true;
          }
        });
      }
    };
  });