/**
 * Controller for Preview Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('SnapshotController', function ($scope, $rootScope, _, api, $timeout, previewService, pipelineConstant) {
    var snapshotBatchSize = 10,
      captureSnapshotStatusTimer;

    angular.extend($scope, {
      showLoading: false,
      previewSourceOffset: 0,
      previewBatchSize: 10,
      previewData: {},
      stagePreviewData: {
        input: [],
        output: []
      },

      /**
       * Preview Data for previous stage instance.
       *
       * @param stageInstance
       */
      previousStagePreview: function(stageInstance) {
        $scope.changeStageSelection(stageInstance);
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
          $scope.changeStageSelection(stageInstance);
        }
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
          batchData = $scope.previewData.snapshot;

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

    /**
     * Check for Snapshot Status for every 1 seconds, once done open the snapshot view.
     *
     */
    var checkForCaptureSnapshotStatus = function() {
      captureSnapshotStatusTimer = $timeout(
        function() {
          //console.log( "Pipeline Metrics Timeout executed", Date.now() );
        },
        1000
      );

      captureSnapshotStatusTimer.then(
        function() {
          api.pipelineAgent.getSnapshotStatus()
            .success(function(data) {
              if(data && data.snapshotInProgress === false) {
                console.log('Capturing Snapshot is completed.');


                api.pipelineAgent.getSnapshot($scope.activeConfigInfo.name).
                  success(function(res) {
                    $scope.previewData = res;

                    var firstStageInstance = $scope.pipelineConfig.stages[0];
                    $scope.changeStageSelection(firstStageInstance);

                    $scope.showLoading = false;
                  }).
                  error(function(data) {
                    $rootScope.common.errors = [data];
                    $scope.showLoading = false;
                  });



              } else {
                checkForCaptureSnapshotStatus();
              }
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };

    $scope.$on('snapshotPipeline', function(event, nextBatch) {
      $scope.showLoading = true;
      api.pipelineAgent.captureSnapshot(snapshotBatchSize).
        then(function() {
          checkForCaptureSnapshotStatus();
        });
    });

    $scope.$on('onStageSelection', function(event, stageInstance) {
      if($scope.snapshotMode) {
        if (stageInstance) {
          updateSnapshotDataForStage(stageInstance);
        } else {
          $scope.stagePreviewData = {
            input: {},
            output: {}
          };
        }
      }
    });

  });