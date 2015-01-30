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
            if(inputRecord.expand) {
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
        return _.filter(errorRecords, function(outputRecord) {

          if(outputRecord.header.trackingId === inputRecord.header.trackingId) {
            if(inputRecord.expand) {
              outputRecord.expand = true;
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
      }
    });

  });