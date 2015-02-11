/**
 * Controller for Preview Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('PreviewController', function ($scope, $rootScope, _, api, previewService, pipelineConstant) {
    var previewDataBackup;

    angular.extend($scope, {
      previewMultipleStages: false,
      showLoading: false,
      previewSourceOffset: 0,
      previewBatchSize: 10,
      previewData: {},
      stagePreviewData: {
        input: [],
        output: []
      },
      previewDataUpdated: false,
      stepExecuted: false,
      dirtyLanes: [],

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
        //if($scope.previewDataUpdated && stageInstance.uiInfo.stageType === pipelineConstant.PROCESSOR_STAGE_TYPE) {
          //$scope.stepPreview(stageInstance, inputRecords);
        //} else {
          $scope.changeStageSelection({
            selectedObject: stageInstance,
            type: pipelineConstant.STAGE_INSTANCE
          });
        //}
      },


      /**
       * Set dirty flag to true when record is updated in Preview Mode.
       *
       * @param recordUpdated
       * @param recordValue
       * @param stageInstance
       */
      recordValueUpdated: function(recordUpdated, recordValue, stageInstance) {
        $scope.previewDataUpdated = true;
        recordUpdated.dirty = true;
        recordValue.dirty = true;

        if(!_.contains($scope.dirtyLanes, recordUpdated.laneName)) {
          $scope.dirtyLanes.push(recordUpdated.laneName);
        }
      },

      /**
       * Run Preview with user updated records.
       *
       * @param stageInstance
       */
      stepPreview: function(stageInstance) {
        var dirtyLanes = $scope.dirtyLanes,
          previewBatchOutput = $scope.previewData.batchesOutput[0],
          stageOutputs = [];

        angular.forEach(previewBatchOutput, function(stageOutput) {
          var lanesList = _.keys(stageOutput.output),
            intersection = _.intersection(dirtyLanes, lanesList);

          if(intersection && intersection.length) {
            var stageOutputCopy = angular.copy(stageOutput);
            /*stageOutputCopy.output = {};

            angular.forEach(intersection, function(laneName) {
              stageOutputCopy.output[laneName] = stageOutput.output[laneName];
            });*/
            stageOutputs.push(stageOutputCopy);
          }
        });

        $scope.showLoading = true;

        api.pipelineAgent.previewPipelineRunStage($scope.activeConfigInfo.name, $scope.previewSourceOffset,
          $scope.pipelineConfig.uiInfo.previewConfig.batchSize, 0, $scope.pipelineConfig.uiInfo.previewConfig.skipTargets, stageOutputs).
          success(function (previewData) {
            var updatedPreviewBatchOutput = previewData.batchesOutput[0];

            angular.forEach(updatedPreviewBatchOutput, function(stageOutput, index) {
              var lanesList = _.keys(stageOutput.output),
                intersection = _.intersection(dirtyLanes, lanesList),
                changedStageOutput = previewBatchOutput[index];

              if(intersection && intersection.length) {
                angular.forEach(intersection, function(laneName) {
                  stageOutput.output[laneName] = changedStageOutput.output[laneName];
                });
              }
            });

            $scope.previewData = previewData;

            if(!$scope.previewMultipleStages) {
              $scope.changeStageSelection({
                selectedObject: stageInstance,
                type: pipelineConstant.STAGE_INSTANCE
              });
            }

            $scope.stepExecuted = true;
            $scope.showLoading = false;
            $rootScope.common.errors = [];
          }).
          error(function(data) {
            $rootScope.common.errors = [data];
            $scope.showLoading = false;
          });
      },

      /**
       * Revert changes done in Preview Data.
       *
       */
      revertChanges: function() {
        $scope.previewData = angular.copy(previewDataBackup);
        $scope.previewDataUpdated = false;
        $scope.stepExecuted = false;
        $scope.dirtyLanes = [];

        if(!$scope.previewMultipleStages) {
          var firstStageInstance = $scope.pipelineConfig.stages[0];
          $scope.changeStageSelection({
            selectedObject: firstStageInstance,
            type: pipelineConstant.STAGE_INSTANCE
          });
        }

      },

      /**
       * Remove record from Source Output list.
       *
       * @param stageInstance
       * @param recordList
       * @param record
       * @param $index
       */
      removeRecord: function(stageInstance, recordList, record, $index) {
        var batchData = $scope.previewData.batchesOutput[0];
        recordList.splice($index, 1);
        $scope.previewDataUpdated = true;
        previewService.removeRecordFromSource(batchData, stageInstance, record);

        if(!_.contains($scope.dirtyLanes, record.laneName)) {
          $scope.dirtyLanes.push(record.laneName);
        }
      }

    });

    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param stageInstance
     */
    var updatePreviewDataForStage = function(stageInstance) {
      var stageInstances = $scope.pipelineConfig.stages,
        batchData = $scope.previewData.batchesOutput[0];

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
    };


    /**
     * Preview Pipeline.
     *
     */
    var previewPipeline = function() {
      $scope.stepExecuted = false;
      $scope.showLoading = true;

      api.pipelineAgent.previewPipeline($scope.activeConfigInfo.name, $scope.previewSourceOffset,
        $scope.pipelineConfig.uiInfo.previewConfig.batchSize, 0, $scope.pipelineConfig.uiInfo.previewConfig.skipTargets).
        success(function (previewData) {
          var firstStageInstance;

          //Clear Previous errors
          $rootScope.common.errors = [];

          previewDataBackup = angular.copy(previewData);

          $scope.previewData = previewData;
          $scope.previewDataUpdated = false;
          $scope.dirtyLanes = [];

          if(!$scope.previewMultipleStages) {
            firstStageInstance = $scope.pipelineConfig.stages[0];
            $scope.changeStageSelection({
              selectedObject: firstStageInstance,
              type: pipelineConstant.STAGE_INSTANCE
            });
          }

          $scope.showLoading = false;
        }).
        error(function(data) {
          $rootScope.common.errors = [data];
          $scope.closePreview();
          $scope.showLoading = false;
        });
    };

    if($scope.previewMode) {
      previewPipeline();
    }

    $scope.$on('previewPipeline', function(event) {
      previewPipeline();
    });

    $scope.$on('onSelectionChange', function(event, options) {
      if($scope.previewMode) {
        if (options.type === pipelineConstant.STAGE_INSTANCE) {
          updatePreviewDataForStage(options.selectedObject);
        } else {
          $scope.stagePreviewData = {
            input: {},
            output: {}
          };
        }
      }
    });


    $scope.$watch('previewMultipleStages', function(newValue) {
      if($scope.previewData.batchesOutput) {
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

    $scope.$on('recordUpdated', function(event, recordUpdated, recordValue) {
      $scope.recordValueUpdated(recordUpdated, recordValue);
    });

  });