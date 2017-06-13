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
/**
 * Editing Text In-Place using HTML5 ContentEditable
 */

angular.module('commonUI.commonDirectives')
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
          } else {
            ngModel.$setViewValue(value);
          }
        }

        ngModel.$render = function() {
          if(ngModel.$viewValue !== undefined) {
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