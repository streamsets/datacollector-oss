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
 * Directive for droppable.
 */

angular.module('commonUI.commonDirectives')
  .directive("droppable", function () {
    return {
      scope: {
        drop: '&'
      },
      link: function(scope, element) {
        // again we need the native object
        var el = element[0];

        el.addEventListener(
          'dragover',
          function(e) {
            //console.log('droppable dragover');
            e.dataTransfer.dropEffect = 'move';
            // allows us to drop
            if (e.preventDefault) {
              e.preventDefault();
            }
            this.classList.add('over');
            return false;
          },
          false
        );

        el.addEventListener(
          'dragenter',
          function(e) {
            //console.log('droppable dragenter');
            this.classList.add('over');
            return false;
          },
          false
        );

        el.addEventListener(
          'dragleave',
          function(e) {
            //console.log('droppable dragleave');
            this.classList.remove('over');
            return false;
          },
          false
        );

        el.addEventListener(
          'drop',
          function(e) {
            // Stops some browsers from redirecting.
            if (e.stopPropagation) {
              e.stopPropagation();
            }
            e.preventDefault();

            this.classList.remove('over');

            var dragData = JSON.parse(e.dataTransfer.getData('Text'));

            scope.$apply(function(scope) {
              var fn = scope.drop();
              if ('undefined' !== typeof fn) {
                fn(e, dragData);
              }
            });

            return false;
          },
          false
        );
      }
    };
  });