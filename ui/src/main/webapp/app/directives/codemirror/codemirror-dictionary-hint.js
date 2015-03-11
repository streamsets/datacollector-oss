
angular.module('dataCollectorApp.codemirrorDirectives')
  .directive('ngCodemirrorDictionaryHint', function($parse, $timeout) {
    'use strict';
    return {
      restrict: 'A',
      priority: 2, // higher than ui-codemirror which is 1.
      compile: function compile() {
        var ctrlSpaceKey = false;

        if (angular.isUndefined(window.CodeMirror)) {
          throw new Error('ng-codemirror-dictionary-hint needs CodeMirror to work.');
        }

        return function postLink(scope, iElement, iAttrs) {
          var dictionary = [];

          // Register our custom Codemirror hint plugin.
          window.CodeMirror.registerHelper('hint', 'dictionaryHint', function(editor) {
            var cur = editor.getCursor(), curLine = editor.getLine(cur.line);
            var start = cur.ch, end = start;

            while (end < curLine.length && /[\w:/$]+/.test(curLine.charAt(end))) {
              ++end;
            }
            while (start && /[\w:/$]+/.test(curLine.charAt(start - 1))) {
              --start;
            }
            var curWord = start != end && curLine.slice(start, end);
            var regex = new RegExp('^' + curWord, 'i');

            return {
              list:(!curWord ? (ctrlSpaceKey ? dictionary.sort() : []) : dictionary.filter(function(item) {
                return item.match(regex);
              })).sort(),
              from: CodeMirror.Pos(cur.line, start),
              to: CodeMirror.Pos(cur.line, end)
            };
          });

          // Check if the ui-codemirror directive is present.
          if (!iAttrs.hasOwnProperty('uiCodemirror') && iElement[0].tagName.toLowerCase() !== 'ui-codemirror') {
            throw new Error('The ng-codemirror-dictionary-hint directive can only be used either ' +
            'on a ui-codemirror element or an element with the ui-codemirror attribute set.');
          }

          if (iAttrs.ngCodemirrorDictionaryHint) {
            scope.$watch('iAttrs.ngCodemirrorDictionaryHint', function() {
              dictionary = $parse(iAttrs.ngCodemirrorDictionaryHint)(scope);
              if (!angular.isArray(dictionary)) {
                throw new Error('ng-codemirror-dictionary-hint must be a list.');
              }
            });
          }

          // The ui-codemirror directive allows us to receive a reference to the Codemirror instance on demand.
          scope.$broadcast('CodeMirror', function(cm) {
            cm.on('change', function(instance, change) {
              if (change.origin !== 'complete') {
                instance.showHint({ hint: window.CodeMirror.hint.dictionaryHint, completeSingle: false });
              }
              $timeout(function() {});
            });

            cm.on('keyHandled', function(instance, name, event) {
              if(name === 'Ctrl-Space') {
                ctrlSpaceKey = true;
                instance.showHint({ hint: window.CodeMirror.hint.dictionaryHint, completeSingle: false });
                ctrlSpaceKey = false;
              }
            });

          });

        };
      }
    };
  });