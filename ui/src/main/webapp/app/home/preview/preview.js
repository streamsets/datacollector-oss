/**
 * Controller for Preview Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('PreviewController', function ($scope, $rootScope, _, api, previewService) {
    var SOURCE_STAGE_TYPE = 'SOURCE',
      PROCESSOR_STAGE_TYPE = 'PROCESSOR',
      TARGET_STAGE_TYPE = 'TARGET',
      previewDataBackup;

    angular.extend($scope, {
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
        if($scope.stepExecuted && stageInstance.uiInfo.stageType === PROCESSOR_STAGE_TYPE) {
          $scope.stepPreview(stageInstance, inputRecords);
        } else {
          $scope.changeStageSelection(stageInstance);
        }
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

        _.each(records, function(record) {
          delete record.dirty;
          delete record.expand;

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

        var firstStageInstance = $scope.pipelineConfig.stages[0];
        $scope.changeStageSelection(firstStageInstance);
      },

      onExpandAllInputData: function() {
        $scope.expandAllInputData = true;
      },

      onCollapseAllInputData: function() {
        $scope.expandAllInputData = false;
      },

      onExpandAllOutputData: function() {
        $scope.expandAllOutputData = true;
      },

      onCollapseAllOutputData: function() {
        $scope.expandAllOutputData = false;
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
          //Clear Previous errors
          $rootScope.common.errors = [];

          previewDataBackup = angular.copy(previewData);

          $scope.previewData = previewData;
          $scope.previewDataUpdated = false;

          var firstStageInstance = $scope.pipelineConfig.stages[0];
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

  });