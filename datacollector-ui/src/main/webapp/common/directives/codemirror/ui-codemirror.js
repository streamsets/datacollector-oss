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
 * Binds a CodeMirror widget to a <textarea> element.
 *
 * https://github.com/angular-ui/ui-codemirror
 * MIT License
 *
 */
angular.module('commonUI.codemirrorDirectives')
  .constant('uiCodemirrorConfig', {})
  .directive('uiCodemirror', uiCodemirrorDirective);

/**
 * @ngInject
 */
function uiCodemirrorDirective($timeout, uiCodemirrorConfig) {

  return {
    restrict: 'EA',
    require: '?ngModel',
    compile: function compile() {

      // Require CodeMirror
      if (angular.isUndefined(window.CodeMirror)) {
        throw new Error('ui-codemirror need CodeMirror to work... (o rly?)');
      }

      return postLink;
    }
  };

  function postLink(scope, iElement, iAttrs, ngModel) {

    var codemirrorOptions = angular.extend(
      { value: iElement.text() },
      uiCodemirrorConfig.codemirror || {},
      scope.$eval(iAttrs.uiCodemirror),
      scope.$eval(iAttrs.uiCodemirrorOpts)
    );

    var codemirror = newCodemirrorEditor(iElement, codemirrorOptions);

    var dataType = scope.$eval(iAttrs.type) || 'STRING';

    configOptionsWatcher(
      codemirror,
      iAttrs.uiCodemirror || iAttrs.uiCodemirrorOpts,
      scope
    );

    configNgModelLink(codemirror, ngModel, scope, dataType);

    configUiRefreshAttribute(codemirror, iAttrs.uiRefresh, scope);

    // Allow access to the CodeMirror instance through a broadcasted event
    // eg: $broadcast('CodeMirror', function(cm){...});
    scope.$on('CodeMirror', function(event, callback) {
      if (angular.isFunction(callback)) {
        callback(codemirror);
      } else {
        throw new Error('the CodeMirror event requires a callback function');
      }
    });

    // onLoad callback
    if (angular.isFunction(codemirrorOptions.onLoad)) {
      codemirrorOptions.onLoad(codemirror);
    }

    // Hack to fix bug. ngClick is not triggered when it is clicked inside ${} or during selecting text
    codemirror.on('focus', function(instance) {
      $(iElement).click();
    });
  }

  function newCodemirrorEditor(iElement, codemirrorOptions) {
    var codemirrot;

    if (iElement[0].tagName === 'TEXTAREA') {
      // Might bug but still ...
      codemirrot = window.CodeMirror.fromTextArea(iElement[0], codemirrorOptions);
    } else {
      iElement.html('');
      codemirrot = new window.CodeMirror(function(cm_el) {
        iElement.append(cm_el);
      }, codemirrorOptions);
    }

    return codemirrot;
  }

  function configOptionsWatcher(codemirrot, uiCodemirrorAttr, scope) {
    if (!uiCodemirrorAttr) { return; }

    var codemirrorDefaultsKeys = Object.keys(window.CodeMirror.defaults);
    scope.$watch(uiCodemirrorAttr, updateOptions, true);
    function updateOptions(newValues, oldValue) {
      if (!angular.isObject(newValues)) { return; }
      codemirrorDefaultsKeys.forEach(function(key) {
        if (newValues.hasOwnProperty(key)) {

          if (oldValue && newValues[key] === oldValue[key]) {
            return;
          }

          codemirrot.setOption(key, newValues[key]);
        }
      });
    }
  }

  function configNgModelLink(codemirror, ngModel, scope, dataType) {
    if (!ngModel) { return; }
    // CodeMirror expects a string, so make sure it gets one.
    // This does not change the model.
    ngModel.$formatters.push(function(value) {
      if (angular.isUndefined(value) || value === null) {
        return '';
      } else if (angular.isObject(value) || angular.isArray(value)) {
        //throw new Error('ui-codemirror cannot use an object or an array as a model');

        return JSON.stringify(value, null, "\t");
      }
      return value;
    });


    // Override the ngModelController $render method, which is what gets called when the model is updated.
    // This takes care of the synchronizing the codeMirror element with the underlying model, in the case that it is changed by something else.
    ngModel.$render = function() {
      //Code mirror expects a string so make sure it gets one
      //Although the formatter have already done this, it can be possible that another formatter returns undefined (for example the required directive)
      var safeViewValue = ngModel.$viewValue;

      if (safeViewValue === null || safeViewValue === undefined) {
        safeViewValue = '';
      }

      if (dataType === 'NUMBER' && !isNaN(safeViewValue)) {
        safeViewValue = safeViewValue + '';
      }

      codemirror.setValue(safeViewValue);
    };

    // a map from editor elements to last cursor positions
    var lastCursorsByElement = {};

    // Keep the ngModel in sync with changes from CodeMirror
    codemirror.on('change', function(instance) {
      var newValue = instance.getValue();

      if (newValue) {
        newValue = newValue.trim();
      }

      if (dataType === 'NUMBER') {
        if (newValue === '') {
          newValue = 0;
        } else if (!isNaN(newValue)) {
          newValue = parseFloat(newValue);
        }
      }

      if (dataType === 'LIST') {
        try {
          newValue = JSON.parse(newValue);
        } catch (e) {
          //In case of parse exception return with out updating model value
          return;
        }
      }

      if (newValue !== ngModel.$viewValue || dataType === 'LIST') {
        scope.$evalAsync(function() {
          ngModel.$setViewValue(newValue);
        });
      }

      var cursor = instance.getCursor();
      var inputElem = instance.getInputField();
      if (cursor && (cursor.line || cursor.ch)) {
        // nonzero cursor position to save
        lastCursorsByElement[inputElem] = cursor;
      } else if (lastCursorsByElement[inputElem]) {
        // current cursor was zero, but one was stored for its input element
        // first, restore the saved cursor
        instance.setCursor(lastCursorsByElement[inputElem]);
        // then remove from map
        lastCursorsByElement[inputElem] = null;
      }
    });
  }

  function configUiRefreshAttribute(codeMirror, uiRefreshAttr, scope) {
    if (!uiRefreshAttr) { return; }

    scope.$watch(uiRefreshAttr, function(newVal, oldVal) {
      // Skip the initial watch firing
      if (newVal !== oldVal) {
        $timeout(function() {
          codeMirror.refresh();
        });
      }
    });
  }

}
uiCodemirrorDirective.$inject = ["$timeout", "uiCodemirrorConfig"];
