/**
 * Controller for Preview Pane.
 */

angular
  .module('pipelineAgentApp.home')

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

      changeToPreviewMultipleStages : function() {
        $scope.previewMultipleStages = true;
        $scope.moveGraphToCenter();
      },

      changeToPreviewSingleStage: function() {
        $scope.previewMultipleStages = false;
        $scope.changeStageSelection($scope.pipelineConfig.stages[0]);
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
      },


      /**
       * Set dirty flag to true when record is updated in Preview Mode.
       *
       * @param recordUpdated
       * @param fieldName
       * @param stageInstance
       */
      recordValueUpdated: function(recordUpdated, recordValue, stageInstance) {
        $scope.previewDataUpdated = true;
        recordUpdated.dirty = true;
        recordValue.dirty = true;
      },

      /**
       * Run Preview with user updated records.
       *
       * @param stageInstance
       * @param inputRecords
       */
      stepPreview: function(stageInstance, inputRecords) {
        var instanceName = stageInstance.instanceName,
          records = _.map(inputRecords, _.clone);

        if(stageInstance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE) {
          //In Case of Source just update flag and return.

          $scope.stepExecuted = true;
          return;
        }

        _.each(records, function(record) {
          delete record.dirty;
          delete record.expand;
          delete record.laneName;

          _.each(record.value.value, function(key, value) {
            delete value.dirty;
          });

        });

        $scope.showLoading = true;

        api.pipelineAgent.previewPipelineRunStage($scope.activeConfigInfo.name, instanceName, records).
          success(function (previewData) {
            var targetInstanceData = previewData.batchesOutput[0][0];

            _.each($scope.previewData.batchesOutput[0], function(instanceRecords) {
              if(instanceRecords.instanceName === targetInstanceData.instanceName) {
                instanceRecords.output = targetInstanceData.output;
                instanceRecords.errorRecords = targetInstanceData.errorRecords;
              }
            });

            $scope.changeStageSelection(stageInstance);
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

        var firstStageInstance = $scope.pipelineConfig.stages[0];
        $scope.changeStageSelection(firstStageInstance);
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
        $scope.stepExecuted = true;
        $scope.previewDataUpdated = true;
        previewService.removeRecordFromSource(batchData, stageInstance, record);
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

    $scope.$on('previewPipeline', function(event, nextBatch) {
      $scope.stepExecuted = false;
      $scope.showLoading = true;

      if (nextBatch) {
        $scope.previewSourceOffset += $scope.previewBatchSize;
      } else {
        $scope.previewSourceOffset = 0;
      }

      api.pipelineAgent.previewPipeline($scope.activeConfigInfo.name, $scope.previewSourceOffset, $scope.previewBatchSize).
        success(function (previewData) {
          var firstStageInstance;

          //Clear Previous errors
          $rootScope.common.errors = [];

          previewDataBackup = angular.copy(previewData);

          $scope.previewData = previewData;
          $scope.previewDataUpdated = false;

          if(!$scope.previewMultipleStages) {
            firstStageInstance = $scope.pipelineConfig.stages[0];
          }

          $scope.changeStageSelection(firstStageInstance);

          $scope.showLoading = false;
        }).
        error(function(data) {
          $rootScope.common.errors = [data];
          $scope.closePreview();
          $scope.showLoading = false;
        });
    });

    $scope.$on('onStageSelection', function(event, stageInstance) {
      if($scope.previewMode) {
        if (stageInstance) {
          updatePreviewDataForStage(stageInstance);
        } else {
          $scope.stagePreviewData = {
            input: {},
            output: {}
          };
        }
      }
    });

    $scope.$on('recordUpdated', function(event, recordUpdated, recordValue) {
      $scope.recordValueUpdated(recordUpdated, recordValue);
    });

  });