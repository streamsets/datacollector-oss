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
 * https://github.com/paulyoder/angular-bootstrap-show-errors
 * MIT License
 */

angular.module('commonUI.commonDirectives')
  .provider('showErrorsConfig', function() {
    var _showSuccess, _trigger;
    _showSuccess = false;
    _trigger = 'blur';
    this.showSuccess = function(showSuccess) {
      return _showSuccess = showSuccess;
    };
    this.trigger = function(trigger) {
      return _trigger = trigger;
    };
    this.$get = function() {
      return {
        showSuccess: _showSuccess,
        trigger: _trigger
      };
    };
  })
  .directive('showErrors', function($timeout, showErrorsConfig, $interpolate) {
    var getShowSuccess, getTrigger, linkFn;
    getTrigger = function(options) {
      var trigger;
      trigger = showErrorsConfig.trigger;
      if (options && (options.trigger != null)) {
        trigger = options.trigger;
      }
      return trigger;
    };
    getShowSuccess = function(options) {
      var showSuccess;
      showSuccess = showErrorsConfig.showSuccess;
      if (options && (options.showSuccess != null)) {
        showSuccess = options.showSuccess;
      }
      return showSuccess;
    };
    linkFn = function(scope, el, attrs, formCtrl) {
      $timeout(function(){
        var blurred, inputEl, inputName, inputNgEl, options, showSuccess, toggleClasses, trigger;
        blurred = false;
        options = scope.$eval(attrs.showErrors);
        showSuccess = getShowSuccess(options);
        trigger = getTrigger(options);
        inputEl = el[0].querySelector('.form-control[name]');
        inputNgEl = angular.element(inputEl);
        inputName = $interpolate(inputNgEl.attr('name') || '')(scope);
        if (!inputName) {
          //throw "show-errors element has no child input elements with a 'name' attribute and a 'form-control' class";
          return;
        }
        inputNgEl.bind(trigger, function() {
          blurred = true;
          if(formCtrl[inputName]) {
            return toggleClasses(formCtrl[inputName].$invalid);
          }
        });
        scope.$watch(function() {
          return formCtrl[inputName] && formCtrl[inputName].$invalid;
        }, function(invalid) {
          if (!blurred) {
            return;
          }
          return toggleClasses(invalid);
        });
        scope.$on('show-errors-check-validity', function() {
          if(formCtrl[inputName]) {
            return toggleClasses(formCtrl[inputName].$invalid);
          }
        });
        scope.$on('show-errors-reset', function() {
          return $timeout(function() {
            el.removeClass('has-error');
            el.removeClass('has-success');
            return blurred = false;
          }, 0, false);
        });
        return toggleClasses = function(invalid) {
          el.toggleClass('has-error', invalid);
          if (showSuccess) {
            return el.toggleClass('has-success', !invalid);
          }
        };
      });
    };
    return {
      restrict: 'A',
      require: '^form',
      compile: function(elem, attrs) {
        if (!elem.hasClass('form-group')) {
          throw "show-errors element does not have the 'form-group' class";
        }
        return linkFn;
      }
    };
  });