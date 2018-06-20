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
angular.module('commonUI.codemirrorDirectives')
  .directive('codemirrorEl', function($parse, $timeout) {
    'use strict';
    var fnDefMapping = {};

    // Note(chab) we will have one shared for all instances of the codeMirrorDirectice
    // if we want to have a dedicated for each instance, there are some work needed in order
    // to have dedicated event handling for each instance of the directive
    var tooltip = null;

    return {
      restrict: 'A',
      priority: 2, // higher than ui-codemirror which is 1.
      compile: function compile() {
        var cmPos = CodeMirror.Pos,
            cmpPos = CodeMirror.cmpPos,
            cls = "CodeMirror-EL-",
            cachedArgHints = null,
            activeArgHints = null,
            activeAutoComplete = false;

        if (angular.isUndefined(window.CodeMirror)) {
          throw new Error('codemirror-el needs CodeMirror to work.');
        }

        return function postLink(scope, iElement, iAttrs) {
          var fieldPaths = [],
              dFieldPaths = [],
              fieldPathsType = [];

          // Register our custom Codemirror hint plugin.
          window.CodeMirror.registerHelper('hint', 'dictionaryHint', function(editor, options, c) {
            var dictionary = editor.options.dictionary;
            var dictRegex = (dictionary.regex && dictionary.regex === 'wordColonSlash') ? /[\w:/$]+/ : /[\w:'\[\]/$]+/;
            var mode = editor.doc.modeOption;
            var cur = editor.getCursor(), curLine = editor.getLine(cur.line);
            var start = cur.ch, end = start;

            while (end < curLine.length && dictRegex.test(curLine.charAt(end))) {
              ++end;
            }
            while (start && dictRegex.test(curLine.charAt(start - 1))) {
              --start;
            }
            var curWord = start != end && curLine.slice(start, end).
                replace(/\[/g, '\\[').
                replace(/\]g/, '\\]').
                replace("'", "\\'");
            var regex = new RegExp('^' + curWord, 'i');
            var completions =[];

            var startWithSingleQuote = curLine.charAt(start- 1) == "'";

            if(curWord || options.ctrlSpaceKey) {

              if(dictionary.pipelineConstants && dictionary.pipelineConstants.length) {
                angular.forEach(dictionary.pipelineConstants, function(pipelineConstant) {
                  if(pipelineConstant.key && (!curWord || pipelineConstant.key.match(regex))) {
                    completions.push({
                      text: pipelineConstant.key,
                      displayText: pipelineConstant.key,
                      className: 'CodeMirror-EL-completion CodeMirror-EL-completion-user-constant',
                      data: {
                        description: 'Pipeline Parameter with value - ' + pipelineConstant.value
                      }
                    });
                  }
                });
              }

              if(dictionary.runtimeConfigs && dictionary.runtimeConfigs.length) {
                angular.forEach(dictionary.runtimeConfigs, function(runtimeConfig) {
                  if(runtimeConfig && (!curWord || runtimeConfig.match(regex))) {
                    completions.push({
                      text: runtimeConfig,
                      displayText: runtimeConfig,
                      className: 'CodeMirror-EL-completion CodeMirror-EL-completion-runtime-config',
                      data: {
                        description: 'Runtime Config'
                      }
                    });
                  }
                });
              }

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

              fnDefMapping = {};

              angular.forEach(dictionary.elFunctionDefinitions, function(elFunctionDefn) {
                if(!curWord || elFunctionDefn.name.match(regex)) {
                  fnDefMapping[elFunctionDefn.name] = elFunctionDefn.description;
                  completions.push({
                    text: elFunctionDefn.name + (elFunctionDefn.elFunctionArgumentDefinition.length ? '()' : '()'),
                    displayText: elFunctionDefn.name,
                    className: 'CodeMirror-EL-completion CodeMirror-EL-completion-fn',
                    data: elFunctionDefn
                  });
                }
              });

              var fieldPathList = getFieldPaths(dictionary, startWithSingleQuote);

              angular.forEach(fieldPathList, function(fieldPath, index) {
                if(!curWord || fieldPath.match(regex)) {
                  var desc = 'Field Path: ' + fieldPath;

                  if(fieldPathsType && fieldPathsType.length > index) {
                    desc += ' , Type: ' + fieldPathsType[index];
                  }

                  completions.push({
                    text: fieldPath,
                    displayText: fieldPath,
                    className: 'CodeMirror-EL-completion CodeMirror-EL-completion-field-path',
                    data: {
                      description: desc
                    }
                  });
                }
              });

              var keywords = getKeywords(mode);
              angular.forEach(keywords, function(value, keyword) {
                if(!curWord || keyword.match(regex)) {
                  completions.push({
                    text: keyword.toUpperCase(),
                    displayText: keyword.toUpperCase(),
                    className: 'CodeMirror-EL-completion CodeMirror-EL-completion-keyword',
                    data: {
                      description: 'Keyword - ' + keyword.toUpperCase()
                    }
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

          if (iAttrs.fieldPaths) {
            scope.$watch('iAttrs.fieldPaths', function() {
              fieldPaths = $parse(iAttrs.fieldPaths)(scope);
              dFieldPaths = $parse(iAttrs.dFieldPaths)(scope);
              fieldPathsType = $parse(iAttrs.fieldPathsType)(scope);
            });
          }

          scope.$on('fieldPathsUpdated', function(event, _fieldPaths, _fieldPathsType, _dFieldPaths) {
            fieldPaths = _fieldPaths;
            dFieldPaths = _dFieldPaths;
            fieldPathsType = _fieldPathsType;
          });

          // The ui-codemirror directive allows us to receive a reference to the Codemirror instance on demand.
          scope.$broadcast('CodeMirror', function(cm) {
            cm.on('change', function(instance, change) {
              if (change.origin !== 'complete' && change.origin !== 'setValue') {
                instance.showHint({ hint: window.CodeMirror.hint.dictionaryHint, completeSingle: false });

                angular.element('.CodeMirror-hints > .CodeMirror-EL-completion').on('mouseover', function(e) {
                  angular.element('.CodeMirror-hint-active').removeClass('CodeMirror-hint-active');
                  angular.element('.CodeMirror-EL-tooltip').remove();
                  var hint = angular.element(e.target).addClass('CodeMirror-hint-active');
                  tooltip = makeTooltip(e.target.parentNode.getBoundingClientRect().right + window.pageXOffset,
                      e.target.getBoundingClientRect().top + window.pageYOffset,
                      fnDefMapping[hint.text()]);
                  tooltip.className += " " + cls + "hint-doc";
                });
              }
              $timeout(function() {});
            });

            cm.on('keydown', function(instance, e) {
              if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                angular.element('.CodeMirror-hint-active').removeClass('CodeMirror-hint-active');
                angular.element('.CodeMirror-EL-tooltip').remove();
              }
            });

            cm.on('keyHandled', function(instance, name, e) {
              if (name === 'Ctrl-Space') {
                instance.showHint({
                  hint: window.CodeMirror.hint.dictionaryHint,
                  completeSingle: false,
                  ctrlSpaceKey: true
                });
              }
            });

            cm.on('cursorActivity', function(cm) {
              updateArgHints(cm);
            });

            cm.on("blur", function() {
              closeArgHints();
            });

          });

          function getFieldPaths(dictionary, startWithSingleQuote) {
            var fieldPathList = fieldPaths;

            if(startWithSingleQuote) {
              fieldPathList = dFieldPaths;
            }

            if(dictionary && dictionary.textMode &&
              (dictionary.textMode === 'text/javascript' || dictionary.textMode === 'text/x-python')) {
              var fp = [];
              angular.forEach(fieldPathList, function(fieldPath) {
                var fieldPathArr = fieldPath.split('/');
                var val = 'value';
                angular.forEach(fieldPathArr, function(p, index) {
                  if(p) {
                    var listIndex = p.indexOf('[');
                    if(listIndex === 0) {
                      val += p;
                    } else if(listIndex != -1) {
                      val += '[\'' + p.substring(0, listIndex) + '\']' + p.substring(listIndex, p.length);
                    } else {
                      val += '[\'' + p + '\']';
                    }
                  }
                });
                fp.push(val);
              });
              return fp;
            } else {
              return fieldPathList;
            }
          }

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
            var dictionary = cm.options.dictionary;
            closeArgHints();

            if (cm.somethingSelected() || activeAutoComplete) {
              return;
            }

            var state = cm.getTokenAt(cm.getCursor()).state;
            var inner = CodeMirror.innerMode(cm.getMode(), state);

            var ch,
              argPos = 0,
              tabSize = cm.getOption("tabSize"),
              pos,
              cursorPos = cm.getCursor().ch,
              functionName;

            for (var line = cm.getCursor().line, e = Math.max(0, line - 9), found = false; line >= e; --line) {
              var str = cm.getLine(line), closedBracketCount = 0;


              for(pos = cursorPos - 1; pos >= 0; pos--) {
                if(str.charAt(pos) === '('){
                  if(closedBracketCount === 0) {
                    ch = pos;
                    break;
                  } else {
                    closedBracketCount--;
                  }
                }

                if(str.charAt(pos) === ')'){
                  closedBracketCount++;
                }
              }

              if (ch >= 0) {
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

            if (!functionDefintion) {
              //console.log('No function definition found for name:' + functionDefintion);
              return;
            }

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

          function getKeywords(mode) {
            if(mode && mode.name === 'text/x-sql') {
              return CodeMirror.resolveMode(mode).keywords;
            }
            return [];
          }
        };
      }
    };
  });
