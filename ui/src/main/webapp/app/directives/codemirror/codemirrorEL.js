
angular.module('dataCollectorApp.codemirrorDirectives')
  .directive('codemirrorEl', function($parse, $timeout) {
    'use strict';
    return {
      restrict: 'A',
      priority: 2, // higher than ui-codemirror which is 1.
      compile: function compile() {
        var ctrlSpaceKey = false,
          cmPos = CodeMirror.Pos,
          cmpPos = CodeMirror.cmpPos,
          cls = "CodeMirror-EL-",
          cachedArgHints = null,
          activeArgHints = null,
          activeAutoComplete = false;

        if (angular.isUndefined(window.CodeMirror)) {
          throw new Error('codemirror-el needs CodeMirror to work.');
        }

        return function postLink(scope, iElement, iAttrs) {
          var dictionary = {},
            fieldPaths = [];

          // Register our custom Codemirror hint plugin.
          window.CodeMirror.registerHelper('hint', 'dictionaryHint', function(editor, cm, c) {
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
            var completions =[];

            if(curWord || ctrlSpaceKey) {
              angular.forEach(dictionary.elConstantDefinitions, function(elConstantDefn) {
                if(!curWord || elConstantDefn.name.match(regex)) {
                  completions.push({
                    text: elConstantDefn.name,
                    displayText: elConstantDefn.name,
                    className: 'CodeMirror-EL-completion CodeMirror-EL-completion-constant',
                    data: elConstantDefn
                  });
                }
              });


              angular.forEach(dictionary.elFunctionDefinitions, function(elFunctionDefn) {
                if(!curWord || elFunctionDefn.name.match(regex)) {
                  completions.push({
                    text: elFunctionDefn.name + (elFunctionDefn.elFunctionArgumentDefinition.length ? '(' : '()'),
                    displayText: elFunctionDefn.name,
                    className: 'CodeMirror-EL-completion CodeMirror-EL-completion-fn',
                    data: elFunctionDefn
                  });
                }
              });

              angular.forEach(fieldPaths, function(fieldPath) {
                if(!curWord || fieldPath.match(regex)) {
                  completions.push({
                    text: fieldPath,
                    displayText: fieldPath,
                    className: 'CodeMirror-EL-completion CodeMirror-EL-completion-field-path',
                    data: fieldPath
                  });
                }
              });
            }

            completions = _.sortBy(completions, 'text');

            if(completions.length > 0 ) {
              activeAutoComplete = true;
              closeArgHints();
            } else {
              activeAutoComplete = false;
            }

            var obj = {
              list: completions,
              from: CodeMirror.Pos(cur.line, start),
              to: CodeMirror.Pos(cur.line, end)
            };

            var tooltip = null;
            CodeMirror.on(obj, "close", function() {
              remove(tooltip);
              activeAutoComplete = false;
            });

            CodeMirror.on(obj, "update", function() {
              remove(tooltip);
            });

            CodeMirror.on(obj, "select", function(cur, node) {
              remove(tooltip);
              var content = cur.data.description;
              if (content) {
                tooltip = makeTooltip(node.parentNode.getBoundingClientRect().right + window.pageXOffset,
                  node.getBoundingClientRect().top + window.pageYOffset, content);
                tooltip.className += " " + cls + "hint-doc";
              }
            });

            return obj;
          });


          // Check if the ui-codemirror directive is present.
          if (!iAttrs.hasOwnProperty('uiCodemirror') && iElement[0].tagName.toLowerCase() !== 'ui-codemirror') {
            throw new Error('The codemirror-el directive can only be used either ' +
            'on a ui-codemirror element or an element with the ui-codemirror attribute set.');
          }

          if (iAttrs.codemirrorEl) {
            scope.$watch('iAttrs.codemirrorEl', function() {
              dictionary = $parse(iAttrs.codemirrorEl)(scope);
            });
          }

          if (iAttrs.fieldPaths) {
            scope.$watch('iAttrs.fieldPaths', function() {
              fieldPaths = $parse(iAttrs.fieldPaths)(scope);
            });
          }

          scope.$on('fieldPathsUpdated', function(event, _fieldPaths) {
            fieldPaths = _fieldPaths;
          });

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

            cm.on('cursorActivity', function(cm) {
              updateArgHints(cm);
            });

            cm.on("blur", function(){
              closeArgHints();
            });

          });

          function elt(tagname, cls /*, ... elts*/) {
            var e = document.createElement(tagname);
            if (cls) {
              e.className = cls;
            }
            for (var i = 2; i < arguments.length; ++i) {
              var eltArg = arguments[i];
              if (typeof eltArg == "string") {
                eltArg = document.createTextNode(eltArg);
              }
              e.appendChild(eltArg);
            }
            return e;
          }

          function makeTooltip(x, y, content) {
            var node = elt("div", cls + "tooltip", content);
            node.style.left = x + "px";
            node.style.top = y + "px";
            document.body.appendChild(node);
            return node;
          }

          function remove(node) {
            var p = node && node.parentNode;
            if (p) {
              p.removeChild(node);
            }
          }

          function updateArgHints(cm) {
            closeArgHints();

            if (cm.somethingSelected() || activeAutoComplete) {
              return;
            }

            var state = cm.getTokenAt(cm.getCursor()).state;
            var inner = CodeMirror.innerMode(cm.getMode(), state);

            //if (inner.mode.name != "javascript") return;
            var lex = inner.state.lexical;

            if (lex.info != "call") {
              return;
            }

            var ch,
              argPos = lex.pos || 0,
              tabSize = cm.getOption("tabSize"),
              pos,
              functionName;

            for (var line = cm.getCursor().line, e = Math.max(0, line - 9), found = false; line >= e; --line) {
              var str = cm.getLine(line), extra = 0;

              for (pos = 0; ;) {
                var tab = str.indexOf("\t", pos);
                if (tab == -1) {
                  break;
                }
                extra += tabSize - (tab + extra) % tabSize - 1;
                pos = tab + 1;
              }

              ch = lex.column - extra;

              if (str.charAt(ch) == "(") {
                found = true;

                //Get Method Name
                var startIndex = ch - 1;
                while(startIndex >= 0 && /[a-zA-Z:]+/.test(str.charAt(startIndex))) {
                  startIndex--;
                }

                startIndex++;
                functionName = str.substr(startIndex , ch - startIndex);

                break;
              }
            }

            if (!found) {
              return;
            }

            var start = cmPos(line, ch);
            var cache = cachedArgHints;
            if (cache && cache.doc == cm.getDoc() && cmpPos(start, cache.start) === 0) {
              return showArgHints(cm, argPos);
            }

            var functionDefintion = _.find(dictionary.elFunctionDefinitions, function(elFunctionDefn) {
              return elFunctionDefn.name === functionName;
            });

            cachedArgHints = {
              start: pos,
              name: functionName,
              guess: 'functionGuess',
              doc: cm.getDoc(),
              functionDefinition: functionDefintion
            };

            showArgHints(cm, argPos);
          }

          function closeArgHints() {
            if (activeArgHints) {
              remove(activeArgHints);
              activeArgHints = null;
            }
          }

          function showArgHints(cm, pos) {
            closeArgHints();

            var cache = cachedArgHints,
              fd = cache.functionDefinition;

            var tip = elt("span", cache.guess ? cls + "fhint-guess" : null,
              elt("span", cls + "fname", cache.name), "(");


            for (var i = 0; i < fd.elFunctionArgumentDefinition.length; ++i) {
              if (i) {
                tip.appendChild(document.createTextNode(", "));
              }

              var arg = fd.elFunctionArgumentDefinition[i];

              tip.appendChild(elt("span", cls + "farg" + (i == pos ? " " + cls + "farg-current" : ""), arg.name || "?"));
              if (arg.type != "?") {
                tip.appendChild(document.createTextNode(":\u00a0"));
                tip.appendChild(elt("span", cls + "type", arg.type));
              }
            }

            tip.appendChild(document.createTextNode(fd.returnType ? ") ->\u00a0" : ")"));

            if (fd.returnType) {
              tip.appendChild(elt("span", cls + "type", fd.returnType));
            }

            var place = cm.cursorCoords(null, "page");

            activeArgHints = makeTooltip(place.right + 1, place.bottom, tip);
          }

        };
      }
    };
  });