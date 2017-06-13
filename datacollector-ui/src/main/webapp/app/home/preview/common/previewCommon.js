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
 * Controller for Preview/Snapshot Common Code.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PreviewCommonController', function ($scope, $rootScope, _, previewService) {
    angular.extend($scope, {
      expandAllInputData: false,
      expandAllOutputData: false,

      /**
       * Returns output records produced by input record.
       *
       * @param outputRecords
       * @param inputRecord
       * @returns {*}
       */
      getOutputRecords: function(outputRecords, inputRecord) {
        return _.filter(outputRecords, function(outputRecord) {
          if(outputRecord.header.previousTrackingId === inputRecord.header.trackingId) {
            if(inputRecord.expand && outputRecord.expand === undefined) {
              outputRecord.expand = true;
            }
            return true;
          }
        });
      },

      /**
       * Returns error records produced by input record.
       *
       * @param errorRecords
       * @param inputRecord
       * @returns {*}
       */
      getErrorRecords: function(errorRecords, inputRecord) {
        return _.filter(errorRecords, function(errorRecord) {

          if(errorRecord.header.trackingId === inputRecord.header.trackingId) {
            if(inputRecord.expand && errorRecord.expand === undefined) {
              errorRecord.expand = true;
            }
            return true;
          }
        });
      },

      /**
       * Callback function for expand all input data
       */
      onExpandAllInputData: function() {
        $scope.expandAllInputData = true;
        angular.forEach($scope.stagePreviewData.input, function(record) {
          record.expand = true;
        });
      },

      /**
       * Callback function for collapse all input data
       */
      onCollapseAllInputData: function() {
        $scope.expandAllInputData = false;
        angular.forEach($scope.stagePreviewData.input, function(record) {
          record.expand = false;
        });
      },

      /**
       * Callback function for expand all output data
       */
      onExpandAllOutputData: function() {
        $scope.expandAllOutputData = true;
        angular.forEach($scope.stagePreviewData.output, function(record) {
          record.expand = true;
        });
      },

      /**
       * Callback function for collapse all output data
       */
      onCollapseAllOutputData: function() {
        $scope.expandAllOutputData = false;
        angular.forEach($scope.stagePreviewData.output, function(record) {
          record.expand = false;
        });
      },

      /**
       * Return Additional Information about the record.
       * @param stageInstance
       * @param record
       * @param recordType
       */
      getRecordAdditionalInfo: function(stageInstance, record, recordType) {
        return previewService.getRecordAdditionalInfo(stageInstance, record, recordType);
      },

      escapeHtml: function(unsafe) {
        return unsafe
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;")
          .replace(/'/g, "&#039;");
      }
    });

  });
