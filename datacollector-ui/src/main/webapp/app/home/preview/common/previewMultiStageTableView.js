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
 * Controller for Preview/Snapshot Common Table View.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PreviewMultiStageTableViewController', function ($scope, pipelineService) {
    var columnLimit = 5;

    angular.extend($scope, {
      inputFieldPaths: [],
      outputFieldPaths: [],
      inputLimit: columnLimit,
      outputLimit: columnLimit,

      /**
       * Return map of flatten record.
       *
       * @param record
       * @returns {{}}
       */
      getFlattenRecord: function(record) {
        if(record) {
          var flattenRecord = {};
          if(pipelineService.isCSVRecord(record.value)) {
            pipelineService.getFlattenRecordForCSVRecord(record.value, flattenRecord);
          } else {
            pipelineService.getFlattenRecord(record.value, flattenRecord);
          }
          return flattenRecord;
        }
      },

      /**
       * Callback function when Show more link clicked.
       *
       * @param $event
       */
      onShowMoreInputClick: function($event) {
        $event.preventDefault();
        $scope.inputLimit += columnLimit;

        if($scope.inputLimit > $scope.inputFieldPaths.length) {
          $scope.inputLimit = $scope.inputFieldPaths.length;
        }
      },

      /**
       * Callback function when Show all link clicked.
       *
       * @param $event
       */
      onShowAllInputClick: function($event) {
        $event.preventDefault();
        $scope.inputLimit = $scope.inputFieldPaths.length;
      },

      /**
       * Callback function when Show more link clicked.
       *
       * @param $event
       */
      onShowMoreOutputClick: function($event) {
        $event.preventDefault();
        $scope.outputLimit += columnLimit;
      },

      /**
       * Callback function when Show all link clicked.
       *
       * @param $event
       */
      onShowAllOutputClick: function($event) {
        $event.preventDefault();
        $scope.outputLimit = $scope.outputFieldPaths.length;

        if($scope.outputLimit > $scope.outputFieldPaths.length) {
          $scope.outputLimit = $scope.outputFieldPaths.length;
        }
      }
    });

    $scope.$watch('multiStagePreviewData', function(event, options) {
      updateFieldPaths();
    });

    var updateFieldPaths = function() {
      var output = $scope.multiStagePreviewData.output,
        input = $scope.multiStagePreviewData.input,
        fieldPathsList,
        fieldPaths,
        isCSVRecord;

      $scope.inputFieldPaths = [];
      if(input && input.length) {

        fieldPathsList = [];
        angular.forEach(input, function(record, index) {
          if(index === 0) {
            isCSVRecord = pipelineService.isCSVRecord(record.value);
          }

          fieldPaths = [];
          if(isCSVRecord) {
            pipelineService.getFieldPathsForCSVRecord(record.value, fieldPaths);
          } else {
            pipelineService.getFieldPaths(record.value, fieldPaths, true);
          }

          fieldPathsList.push(fieldPaths);
        });

        $scope.inputFieldPaths = _.union.apply(_, fieldPathsList);
      }

      if(columnLimit > $scope.inputFieldPaths.length) {
        $scope.inputLimit = $scope.inputFieldPaths.length;
      } else {
        $scope.inputLimit = columnLimit;
      }


      $scope.outputFieldPaths = [];
      if(output && output.length) {
        fieldPathsList = [];
        angular.forEach(output, function(record, index) {

          if(index === 0) {
            isCSVRecord = pipelineService.isCSVRecord(record.value);
          }

          fieldPaths = [];

          if(isCSVRecord) {
            pipelineService.getFieldPathsForCSVRecord(record.value, fieldPaths);
          } else {
            pipelineService.getFieldPaths(record.value, fieldPaths, true);
          }

          fieldPathsList.push(fieldPaths);
        });

        $scope.outputFieldPaths = _.union.apply(_, fieldPathsList);
      }

      if(columnLimit > $scope.outputFieldPaths.length) {
        $scope.outputLimit = $scope.outputFieldPaths.length;
      } else {
        $scope.outputLimit = columnLimit;
      }

    };


    updateFieldPaths();

  });