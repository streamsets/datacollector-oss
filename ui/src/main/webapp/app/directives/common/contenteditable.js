/**
 * Editing Text In-Place using HTML5 ContentEditable
 */

angular.module('dataCollectorApp.commonDirectives')
  .directive('contenteditable', function() {
    return {
      restrict: 'A',
      require: 'ngModel',
      scope: {
        valueType: '=valueType'
      },
      link: function(scope, element, attrs, ngModel) {
        function read() {
          var value = element.text();

          if(value === 'null') {
            ngModel.$setViewValue(null);
          } else if(scope.valueType) {
            switch(scope.valueType) {
              case 'INTEGER':
                if(value && !isNaN(value)) {
                  value = parseInt(value);
                  ngModel.$setViewValue(value);
                } else if(value) {
                  ngModel.$setViewValue(value);
                } else {
                  ngModel.$setViewValue(null);
                }
                break;

              case 'LONG':
              case 'FLOAT':
              case 'DOUBLE':
                if(value && !isNaN(value)) {
                  value = parseFloat(value);
                  ngModel.$setViewValue(value);
                } else if(value) {
                  ngModel.$setViewValue(value);
                } else {
                  ngModel.$setViewValue(null);
                }
                break;

              default:
                ngModel.$setViewValue(value);
            }

          } else {
            ngModel.$setViewValue(value);
          }
        }

        ngModel.$render = function() {
          if(ngModel.$viewValue !== undefined || ngModel.$viewValue !== null) {
            element.text(ngModel.$viewValue + '');
          } else {
            element.text('');
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