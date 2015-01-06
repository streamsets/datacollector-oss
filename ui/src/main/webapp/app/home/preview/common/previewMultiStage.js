/**
 * Controller for Preview/Snapshot Multistage Code.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('PreviewMultiStageController', function ($scope, previewService, $timeout) {
    var stages = $scope.pipelineConfig.stages;

    angular.extend($scope, {
      onFromStageChange: function() {
        $timeout(function() {
          $scope.toStageList = previewService.getStageChildren($scope.fromStage, $scope.pipelineConfig);
          if($scope.toStageList && $scope.toStageList.length) {
            $scope.toStage = $scope.toStageList[0];
          }
          updatePreviewData($scope.fromStage, $scope.toStage);
        });
      },

      onToStageChange: function() {
        $timeout(function() {
          updatePreviewData($scope.fromStage, $scope.toStage);
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
      }
    });

    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param fromStage
     * @param toStage
     */
    var updatePreviewData = function(fromStage, toStage) {
      var batchData = $scope.previewData.batchesOutput[0];
      $scope.multiStagePreviewData = previewService.getPreviewDataForMultiStage(batchData, fromStage, toStage);
      $scope.updateStartAndEndStageInstance(fromStage, toStage);
    };



    if(stages && stages.length) {
      $scope.fromStage = stages[0];
      $scope.toStageList = previewService.getStageChildren($scope.fromStage, $scope.pipelineConfig);

      if($scope.toStageList && $scope.toStageList.length) {
        $scope.toStage = $scope.toStageList[0];
      }

      updatePreviewData($scope.fromStage, $scope.toStage);
    }

  });