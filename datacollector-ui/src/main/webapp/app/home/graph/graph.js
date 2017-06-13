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
 * Controller for Graph Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('GraphController', function ($scope) {

    angular.extend($scope, {
      iconOnly: true,
      selectedSource: {},
      connectStage: {},
      insertStage: {},

      /**
       * Callback function when Selecting Source from alert div.
       *
       */
      onSelectSourceChange: function() {
        var selectedStage = $scope.selectedSource.selected;
        $scope.pipelineConfig.issues = [];
        $scope.selectedSource = {};
        $scope.addStageInstance({
          stage: selectedStage
        });
      },

      /**
       * Callback function when selecting Processor/Target from alert div.
       */
      onConnectStageChange: function() {
        var connectStage = $scope.connectStage.selected;
        $scope.addStageInstance({
          stage: connectStage,
          firstOpenLane: $scope.firstOpenLane
        });
        $scope.connectStage = {};
        $scope.firstOpenLane.stageInstance = undefined;
      },

      /**
       * Callback function when selecting Processor/Target from alert div.
       */
      onInsertStageChange: function() {
        var connectStage = $scope.insertStage.selected;
        $scope.addStageInstance({
          stage: connectStage,
          insertBetweenEdge: $scope.selectedObject
        });
        $scope.insertStage = {};
      },

      /**
       * Callback function when stage is dropped.
       * @param e
       * @param stage
       */
      stageDrop: function(e, stage) {
        if(e && stage) {
          $scope.addStageInstance({
            stage: stage,
            relativeXPos: e.offsetX - 130,
            relativeYPos: e.offsetY
          });
        }
      }
    });

  });