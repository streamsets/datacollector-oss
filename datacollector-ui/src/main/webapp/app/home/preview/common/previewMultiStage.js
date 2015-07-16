/**
 * Controller for Preview/Snapshot Multistage Code.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PreviewMultiStageController', function ($scope, previewService, $timeout, pipelineConstant) {
    var stages = $scope.pipelineConfig.stages;

    angular.extend($scope, {
      fromStage: {},

      toStage: {},

      /**
       * Filter Callback for filtering Sources and Processors
       * @param stage
       * @returns {boolean}
       */
      filterSourceAndProcessors: function(stage) {
        return stage.uiInfo.stageType ===  pipelineConstant.SOURCE_STAGE_TYPE ||
          stage.uiInfo.stageType ===  pipelineConstant.PROCESSOR_STAGE_TYPE;
      },

      onFromStageChange: function() {
        $timeout(function() {
          $scope.toStageList = previewService.getStageChildren($scope.fromStage.selected, $scope.pipelineConfig);
          if($scope.toStageList && $scope.toStageList.length) {
            $scope.toStage.selected = $scope.toStageList[$scope.toStageList.length - 1];
          }
          updatePreviewData($scope.fromStage.selected, $scope.toStage.selected);
        });
      },

      onToStageChange: function() {
        $scope.multiStagePreviewData = {
          input: [],
          output: [],
          errorRecords: [],
          stageErrors: []
        };
        $timeout(function() {
          updatePreviewData($scope.fromStage.selected, $scope.toStage.selected);
        });
      },

      /**
       * Returns output records produced by input record.
       *
       * @param inputRecords
       * @param outputRecord
       * @returns {*}
       */
      getInputRecords: function(inputRecords, outputRecord) {
        return _.filter(inputRecords, function(inputRecord) {
          if(inputRecord.header.sourceId === outputRecord.header.sourceId) {
            return true;
          }
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

    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param fromStage
     * @param toStage
     */
    var updatePreviewData = function(fromStage, toStage) {
      var batchData;

      if($scope.previewMode) {
        batchData = $scope.previewData.batchesOutput[0];
      } else if($scope.snapshotMode) {
        batchData = $scope.previewData.snapshotBatches[0];
      }

      $scope.multiStagePreviewData = previewService.getPreviewDataForMultiStage(batchData, fromStage, toStage);
      $scope.updateStartAndEndStageInstance(fromStage, toStage);
    };

    if(stages && stages.length) {
      $scope.fromStage.selected = stages[0];
      $scope.toStageList = previewService.getStageChildren($scope.fromStage.selected, $scope.pipelineConfig);

      if($scope.toStageList && $scope.toStageList.length) {
        $scope.toStage.selected = $scope.toStageList[$scope.toStageList.length - 1];
      }

      updatePreviewData($scope.fromStage.selected, $scope.toStage.selected);
    }

    $scope.$watch('previewData', function() {
      if(($scope.previewMode || $scope.snapshotMode) && $scope.previewMultipleStages) {
        updatePreviewData($scope.fromStage.selected, $scope.toStage.selected);
      }
    });

  });