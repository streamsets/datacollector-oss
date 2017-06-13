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
 * Controller for Library Pane.
 *
 * System labels are text only, and user-defined labels are nodes
 * which may have N levels of nested labels.
 */
angular
  .module('dataCollectorApp.home')
  .controller('LibraryController', function ($scope, $timeout) {
    var DEBOUNCE_LIMIT = 300; // Meaning for +300 raw pipeline labels,
    var DEBOUNCE_TIME = 500; // debounce label filter for 500ms

    angular.extend($scope, {
      fetchingSystemLabels: true,
      systemPipelineLabels: [],
      pipelineLabels: [],
      labelFilter: {value: ''},
      loadingLabels: true,

      /**
       * Determine whether the view has user-defined labels
       * @returns {boolean}
       */
      hasPipelineLabels: function() {
        return !_.isEmpty($scope.rawPipelineLabels);
      },

      /**
       * Return obj keys sorted alphabetically
       * @param obj
       * @returns {*}
       */
      sortedKeys: function(obj) {
        return !obj ? null : Object.keys(obj).sort();
      },

      /**
       * Select a label
       * @param label
       */
      onSelectLabel : function(label) {
        $scope.$storage.pipelineListState.selectedLabel = label;
        $scope.selectPipelineLabel(label);
      },

      /**
       * Select a node label which may have children
       * @param node
       */
      onSelectNode: function(node) {
        if ($scope.hasChildren(node)) {
          var start = $scope.pipelineLabels.indexOf(node) + 1;
          node.isExpanded = !node.isExpanded;

          // Look ahead for children
          for (var i=start; i<$scope.pipelineLabels.length; i++) {
            var nextNode = $scope.pipelineLabels[i];

            // Expand all in next level only
            if (node.isExpanded && nextNode.level === node.level + 1) {
              nextNode.isVisible = true;
            }

            // Collapse all nested labels
            if (!node.isExpanded && nextNode.level > node.level) {
              nextNode.isVisible = false;
              nextNode.isExpanded = false;
            }

            // Only siblings or parents remaining
            if (node.level >= nextNode.level) {
              break;
            }
          }
        }

        // Finally, select the label
        if ($scope.rawPipelineLabels.indexOf(node.vPath) !== -1) {
          $scope.onSelectLabel(node.vPath);
        }
      },

      /**
       * Determine whether a node has children
       * @param node
       * @returns {boolean}
       */
      hasChildren: function(node) {
        return !_.isEmpty(node.children);
      },

      /**
       * Reset the label filter state to empty
       */
      clearLabelFilter: function() {
        $scope.labelFilter.value = '';
        $scope.filterLabels();
      },

      /**
       * Debounce for large data sets
       * @returns {number}
       */
      getDebounce: function() {
        return $scope.rawPipelineLabels.length > DEBOUNCE_LIMIT ? DEBOUNCE_TIME : 0;
      },

      /**
       * Filter labels and auto-expand its parents for better contextualization
       */
      filterLabels: function() {
        $scope.loadingLabels = true;

        // Delay filter in the event loop to enable loading animation for large data sets
        $timeout(function() {
          $scope.pipelineLabels.forEach(function(node) {

            // Filter value is not present, revert to default state
            if (!$scope.labelFilter.value) {
              var isExpanded = $scope.$storage.pipelineListState.selectedLabel.indexOf(node.vPath) === 0;
              node.isVisible = node.level === 1 || isExpanded;
              node.isExpanded = isExpanded;

            // Filter value is present, apply filter
            } else {
              node.isVisible = node.label.indexOf($scope.labelFilter.value) !== -1;
              node.isExpanded = false;

              // Look behind for parents
              if (node.isVisible) {
                var start = $scope.pipelineLabels.indexOf(node);
                var delta = 1;
                for (var i=start; i>=0; i--) {
                  var prevNode = $scope.pipelineLabels[i];

                  // Auto-expand parents in context with the child node only
                  if (prevNode.level === node.level - delta) {
                    prevNode.isVisible = true;
                    prevNode.isExpanded = false;
                    delta++;
                  }
                }
              }
            }
          });

          // Trigger observer in mutation-callback
          $scope.treeElementTimestamp = new Date().getTime();
        });
      }
    });

    /**
     * Highlight matching text within search results
     *
     * jQuery implementation for better performance, allowing for one-way binding
     * in angular and taking weight away from ng-bind-html.
     */
    var highlight = function() {
      $('.predefined-labels .label-display').each(function(i, el) {
        var el = $(el);

        // Filter value is not present, revert to default state
        if (!$scope.labelFilter.value) {
          el.html(el.text());

          // Filter value is present, apply the highlight
        } else {
          var safeString = $scope.labelFilter.value.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
          var regex = new RegExp(safeString, 'g');
          el.html(el.text().replace(regex, '<span class="highlight">' + $scope.labelFilter.value + '</span>'));
        }
      });
    };

    /**
     * Build a tree node of labels
     * @returns {{}}
     */
    var buildLabelTreeNode = function() {
      var tree = {};

      // Raw labels as entered by user on pipeline form. e.g. "grand/parent/child"
      $scope.rawPipelineLabels.forEach(function(raw) {
        var parts = (raw || '').replace(/\/+/g, '/').replace(/^\/|\/$/g, '').split('/');
        var context = tree;

        // Each part of the raw label split by slash, e.g. "grand"
        parts.forEach(function(part, j) {

          // Virtual paths are unique paths to any given point in the tree, e.g. "grand/parent"
          var vPath = parts.slice(0, j + 1).join('/');
          var level = vPath.split('/').length;
          var selectedLabel = $scope.$storage.pipelineListState.selectedLabel;
          var isExpanded = selectedLabel && selectedLabel.indexOf(vPath) === 0;

          // Build the node. By default, only visible nodes are top-level or
          // parents of the currently selected node for better contextualization
          context[part] = context[part] || {
            raw: raw,
            vPath: vPath,
            label: part,
            level: level,
            isVisible: level === 1 || isExpanded,
            isExpanded: isExpanded
          };

          // Setup nested structure
          var node = context[part];
          node.children = node.children || {};
          context = node.children;
        });
      });

      return tree;
    };

    /**
     * Parse tree node into a flat list of labels
     * @returns {Array}
     */
    var parsePipelineLabels = function() {
      var flat = [];

      var iterate = function(context) {
        $scope.sortedKeys(context.children).forEach(function(label) {
          var node = context.children[label];
          flat.push(node);
          iterate(node);
        });
      };

      iterate({children: buildLabelTreeNode()});
      return flat;
    };

    /**
     * Listen to treeElementChange which is triggered by the
     * mutation-callback defined at the <ul> element
     */
    $scope.$on('treeElementChange', function() {
      $scope.loadingLabels = false;
      $scope.$apply();
      highlight();
    });

    /**
     * Labels are loaded only once in home.js so we get notified with them here
     */
    $scope.onLabelsLoaded(function(systemPipelineLabels, rawPipelineLabels) {
      $scope.fetchingSystemLabels = false;
      $scope.systemPipelineLabels = systemPipelineLabels;
      $scope.rawPipelineLabels = rawPipelineLabels;
      $scope.pipelineLabels = parsePipelineLabels();
    });
  });
