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

angular.module('commonUI.filters')
  .filter('abbreviateNumber', function() {
    return function (value) {
      var newValue = value;
      if (value >= 1000) {
        value = Math.ceil(value);
        var suffixes = ["", "k", "m", "b","t"];
        var suffixNum = Math.floor( (""+value).length/3 );
        var shortValue = '';

        for (var precision = 2; precision >= 1; precision--) {
          shortValue = parseFloat( (suffixNum !== 0 ? (value / Math.pow(1000,suffixNum) ) : value).toPrecision(precision));
          var dotLessShortValue = (shortValue + '').replace(/[^a-zA-Z 0-9]+/g,'');
          if (dotLessShortValue.length <= 2) { break; }
        }

        if (shortValue % 1 !== 0) {
          shortValue = shortValue.toFixed(1);
        }

        newValue = shortValue+suffixes[suffixNum];
      } else if(newValue && (newValue % 1) !== 0) {
        newValue = newValue.toFixed(2);
      }
      return newValue;
    };
  });