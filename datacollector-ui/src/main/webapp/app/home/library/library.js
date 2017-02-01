/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
 */
angular
  .module('dataCollectorApp.home')
  .controller('LibraryController', function ($scope, api, $q) {
    angular.extend($scope, {
      systemPipelineLabels: [],
      pipelineLabels: [],

      /**
       * Determine whether the view has user-defined labels
       * @returns {boolean}
       */
      hasPipelineLabels: function() {
        return !_.isEmpty($scope.pipelineLabels);
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
       * Select a label string from the flat list of system labels
       * @param label
       */
      onSelectLabel : function(label) {
        $scope.$storage.pipelineListState.selectedLabel = label;
        $scope.selectPipelineLabel(label);
      },

      /**
       * Select a label node from the tree of user-defined labels
       * @param node
       */
      onSelectNode: function(node) {
        if (this.hasChildren(node)) {
          node.isExpanded = !node.isExpanded;
        }
        if ($scope.rawPipelineLabels.indexOf(node.raw) !== -1) {
          this.onSelectLabel(node.raw);
        }
      },

      /**
       * Determine whether a node has children
       *
       * @param node
       * @returns {boolean}
       */
      hasChildren: function(node) {
        return !_.isEmpty(node.children);
      }
    });

    /**
     * Build a TreeNode of Labels
     *
     * @param labels
     * @returns {{}}
     */
    var buildLabelTreeNode = function(labels, selectedLabel) {
      var tree = {};

      labels.forEach(function(raw) {
        var parts = (raw || '').replace(/\/+/g, '/').replace(/^\/|\/$/g, '').split('/');
        var context = tree;

        parts.forEach(function(part, j) {
          var index = raw.indexOf('/' + parts[j + 1]);
          var vPath = (index === -1) ? raw : raw.substr(0, index);
          var node = context[part] = context[part] || {raw: vPath, isExpanded: selectedLabel.indexOf(vPath) === 0};
          node.children = node.children || {};
          context = node.children;
        });
      });

      return tree;
    };

    /**
     * Labels are loaded only once in home.js so we get notified with them here
     */
    $scope.onLabelsLoaded(function(systemPipelineLabels, rawPipelineLabels) {
      $scope.systemPipelineLabels = systemPipelineLabels;
      $scope.rawPipelineLabels = rawPipelineLabels.sort();

      $scope.pipelineLabels = {
        children: buildLabelTreeNode($scope.rawPipelineLabels, $scope.$storage.pipelineListState.selectedLabel)
      };
    });
  });
