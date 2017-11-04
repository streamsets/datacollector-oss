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
 * Controller for Stage Library Pane.
 */

angular
  .module('dataCollectorApp.home')
  .controller('StageLibraryController', function ($scope, pipelineService, pipelineConstant) {
    angular.extend($scope, {
      filteredStageLibraries: [],
      searchInput: '',

      allStage: {
        group: 'All'
      },

      /**
       * Return Stage Icon URL
       *
       * @param stage
       * @returns {*}
       */
      getStageIconURL: function(stage) {
        return pipelineService.getStageIconURL(stage);
      },


      /**
       * Callback function when stageFilterGroup is updated.
       *
       */
      onStageFilterGroupChange: function() {
        var stageNameList = [];
        var regex = new RegExp($scope.searchInput, 'i');
        $scope.filteredStageLibraries = [];
        angular.forEach($scope.stageLibraries, function(stageLibrary) {
          if (libraryFilter(stageLibrary) && !_.contains(stageNameList, stageLibrary.name) &&
            regex.test(stageLibrary.label) && !stageLibrary.errorStage && !stageLibrary.statsAggregatorStage &&
            stageLibrary.library !== 'streamsets-datacollector-stats-lib' &&
            stageLibrary.executionModes.indexOf($scope.executionMode) !== -1) {
            stageNameList.push(stageLibrary.name);
            $scope.filteredStageLibraries.push(stageLibrary);
          }
        });
      }
    });

    var typeGroups = [{
      group: 'Type',
      name: pipelineConstant.SOURCE_STAGE_TYPE,
      label: 'Origins'
    },{
      group: 'Type',
      name: pipelineConstant.PROCESSOR_STAGE_TYPE,
      label: 'Processors'
    },{
      group: 'Type',
      name: pipelineConstant.TARGET_STAGE_TYPE,
      label: 'Destinations'
    },{
      group: 'Type',
      name: pipelineConstant.EXECUTOR_STAGE_TYPE,
      label: 'Executors'
    }];

    /**
     * Filter callback function
     *
     * @param stage
     * @returns {boolean}
     */
    var libraryFilter = function(stage) {
      var filterGroup = $scope.$storage.stageFilterGroup;

      if (filterGroup === undefined) {
        $scope.$storage.stageFilterGroup = '';
      }

      if (filterGroup) {
        if (filterGroup.group === 'Type') {
          return stage.type === filterGroup.name;
        } else if (filterGroup.group === 'Library') {
          return stage.library === filterGroup.name;
        }
      }

      return true;
    };

    var updateStageGroups = function() {
      var stageGroups = [];
      var labels = {};

      var libraryList = _.chain($scope.stageLibraries)
        .filter(function (stageLibrary) {
          return stageLibrary.library !== 'streamsets-datacollector-stats-lib';
        })
        .sortBy('libraryLabel')
        .pluck("library")
        .unique()
        .value();

      angular.forEach($scope.stageLibraries, function(stageLibrary) {
        labels[stageLibrary.library] = stageLibrary.libraryLabel;
      });

      stageGroups = stageGroups.concat(typeGroups);

      angular.forEach(libraryList, function(library) {
        stageGroups.push({
          group: 'Library',
          name: library,
          label: labels[library]
        });
      });

      $scope.stageGroups = stageGroups;

      $scope.onStageFilterGroupChange();
    };

    updateStageGroups();

    $scope.$watch('stageLibraries', function() {
      updateStageGroups();
    });

    $scope.$on('updateGraph', function(event, options) {
      if (options && options.nodes && options.nodes.length === 0) {
        $scope.$storage.stageFilterGroup = _.find($scope.stageGroups, function(group) {
          return group.name === pipelineConstant.SOURCE_STAGE_TYPE;
        });
        $scope.onStageFilterGroupChange();
      }
    });

  });
