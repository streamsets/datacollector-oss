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
 * Controller for Snapshot Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('SnapshotController', function ($scope, $rootScope, _, api, $timeout, previewService, pipelineConstant) {
    var snapshotBatchSize = 50000,
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
      recordMaxLimit: 10,
      recordPagination: {
        inputRecords: 10,
        outputRecords: 10,
        errorRecords: 10,
        eventRecords: 10,
        newRecords: 10
      },

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
      viewSnapshot: function(snapshotInfo) {
        $scope.setActiveSnapshotInfo(snapshotInfo);
        viewSnapshot(snapshotInfo.id);
      }
    });

    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param stageInstance
     */
    var updateSnapshotDataForStage = function(stageInstance) {
      if($scope.snapshotMode) {
        var stageInstances = $scope.stageInstances,
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
      api.pipelineAgent.getSnapshot($scope.activeConfigInfo.pipelineId, 0, snapshotName)
        .then(function(res) {
          $scope.previewData = res.data;

          var firstStageInstance = $scope.stageInstances[0];
          $scope.changeStageSelection({
            selectedObject: firstStageInstance,
            type: pipelineConstant.STAGE_INSTANCE
          });

          $rootScope.$broadcast('updateErrorCount',
            previewService.getPreviewStageErrorCounts($scope.previewData.snapshotBatches[0]));
          $scope.showLoading = false;
        })
        .catch(function(res) {
          $rootScope.common.errors = [res.data];
          $scope.showLoading = false;
        });
    };

    $scope.$on('snapshotPipeline', function(event, snapshotName) {
      viewSnapshot(snapshotName);
    });

    if($scope.snapshotMode) {
      viewSnapshot($scope.activeSnapshotInfo.id);

      api.pipelineAgent.getSnapshotsInfo().then(function(res) {
        if(res && res.data && res.data.length) {
          $scope.snapshotsInfo = _.chain(res.data)
            .filter(function(snapshotInfo) {
              return snapshotInfo.name === $scope.activeConfigInfo.pipelineId && !snapshotInfo.inProgress;
            })
            .sortBy('timeStamp')
            .value();
        }
      }, function(res) {
        $scope.common.errors = [res.data];
      });
    }

    $scope.$on('onSelectionChange', function(event, options) {
      if($scope.snapshotMode) {
        if (options.type === pipelineConstant.STAGE_INSTANCE) {
          $scope.recordPagination = {
            inputRecords: $scope.recordMaxLimit,
            outputRecords: $scope.recordMaxLimit,
            errorRecords: $scope.recordMaxLimit,
            eventRecords: $scope.recordMaxLimit,
            newRecords: $scope.recordMaxLimit
          };
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
            selectedObject: $scope.stageInstances[0],
            type: pipelineConstant.STAGE_INSTANCE
          });
        }
      }
    });

  });
