/**
 * Controller for Preview Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('SnapshotController', function ($scope, $rootScope, _, api, $timeout, previewService, pipelineConstant) {
    var snapshotBatchSize = 10,
      captureSnapshotStatusTimer;

    angular.extend($scope, {
      previewMultipleStages: false,
      listView: true,
      showLoading: false,
      previewSourceOffset: 0,
      previewBatchSize: 10,
      previewData: {},
      stagePreviewData: {
        input: [],
        output: []
      },
      snapshotsInfo: [],

      /**
       * Preview Data for previous stage instance.
       *
       * @param stageInstance
       */
      previousStagePreview: function(stageInstance) {
        $scope.changeStageSelection({
          selectedObject: stageInstance,
          type: pipelineConstant.STAGE_INSTANCE
        });
      },

      /**
       * Preview Data for next stage instance.
       * @param stageInstance
       * @param inputRecords
       */
      nextStagePreview: function(stageInstance, inputRecords) {
        if($scope.stepExecuted && stageInstance.uiInfo.stageType === pipelineConstant.PROCESSOR_STAGE_TYPE) {
          $scope.stepPreview(stageInstance, inputRecords);
        } else {
          $scope.changeStageSelection({
            selectedObject: stageInstance,
            type: pipelineConstant.STAGE_INSTANCE
          });
        }
      },

      /**
       * Refresh Snapshot
       */
      viewSnapshot: function(snapshotName) {
        $scope.setSnapshotName(snapshotName);
        viewSnapshot(snapshotName);
      }
    });


    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param stageInstance
     */
    var updateSnapshotDataForStage = function(stageInstance) {
      if($scope.snapshotMode) {
        var stageInstances = $scope.pipelineConfig.stages,
          batchData = $scope.previewData.snapshotBatches[0];

        $scope.stagePreviewData = previewService.getPreviewDataForStage(batchData, stageInstance);

        if(stageInstance.inputLanes && stageInstance.inputLanes.length) {
          $scope.previousStageInstances = _.filter(stageInstances, function(instance) {
            return (_.intersection(instance.outputLanes, stageInstance.inputLanes)).length > 0;
          });
        } else {
          $scope.previousStageInstances = [];
        }

        if(stageInstance.outputLanes && stageInstance.outputLanes.length) {
          $scope.nextStageInstances = _.filter(stageInstances, function(instance) {
            return (_.intersection(instance.inputLanes, stageInstance.outputLanes)).length > 0;
          });
        } else {
          $scope.nextStageInstances = [];
        }
      }
    };

    var viewSnapshot = function(snapshotName) {
      api.pipelineAgent.getSnapshot($scope.activeConfigInfo.name, 0, snapshotName).
        success(function(res) {
          $scope.previewData = res;

          var firstStageInstance = $scope.pipelineConfig.stages[0];
          $scope.changeStageSelection({
            selectedObject: firstStageInstance,
            type: pipelineConstant.STAGE_INSTANCE
          });

          $rootScope.$broadcast('updateErrorCount',
            previewService.getPreviewStageErrorCounts($scope.previewData.snapshotBatches[0]));
          $scope.showLoading = false;
        }).
        error(function(data) {
          $rootScope.common.errors = [data];
          $scope.showLoading = false;
        });

    };

    $scope.$on('snapshotPipeline', function(event, snapshotName) {
      viewSnapshot(snapshotName);
    });

    if($scope.snapshotMode) {
      viewSnapshot($scope.snapshotName);

      api.pipelineAgent.getSnapshotsInfo().then(function(res) {
        if(res && res.data && res.data.length) {
          $scope.snapshotsInfo = _.sortBy(res.data, 'id');
        }
      }, function(res) {
        $scope.common.errors = [res.data];
      });
    }

    $scope.$on('onSelectionChange', function(event, options) {
      if($scope.snapshotMode) {
        if (options.type === pipelineConstant.STAGE_INSTANCE) {
          updateSnapshotDataForStage(options.selectedObject);
        } else {
          $scope.stagePreviewData = {
            input: {},
            output: {}
          };
        }
      }
    });

    $scope.$watch('previewMultipleStages', function(newValue) {
      if($scope.previewData.snapshotBatches && $scope.previewData.snapshotBatches[0]) {
        if(newValue === true) {
          $scope.moveGraphToCenter();
        } else {
          $scope.clearStartAndEndStageInstance();
          $scope.changeStageSelection({
            selectedObject: $scope.pipelineConfig.stages[0],
            type: pipelineConstant.STAGE_INSTANCE
          });
        }
      }
    });

  });