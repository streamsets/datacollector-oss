/**
 * Controller for Preview Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('PreviewController', function ($scope, $rootScope, _, api) {
    var SOURCE_STAGE_TYPE = 'SOURCE',
      PROCESSOR_STAGE_TYPE = 'PROCESSOR',
      TARGET_STAGE_TYPE = 'TARGET';

    angular.extend($scope, {
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
          if(outputRecord.header.previousStageTrackingId === inputRecord.header.trackingId) {
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
      recordValueUpdated: function(recordUpdated, fieldName, stageInstance) {
        $scope.previewDataUpdated = true;
        recordUpdated.dirty = true;
        recordUpdated.value.value[fieldName].dirty = true;
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
          }).
          error(function(data) {
            $rootScope.common.errors = [data];
          });
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
     * Returns Preview input lane & output lane data for the given Stage Instance.
     *
     * @param previewData
     * @param stageInstance
     * @returns {{input: Array, output: Array}}
     */
    var getPreviewDataForStage = function (previewData, stageInstance) {
      var inputLane = (stageInstance.inputLanes && stageInstance.inputLanes.length) ?
          stageInstance.inputLanes[0] : undefined,
        outputLane = (stageInstance.outputLanes && stageInstance.outputLanes.length) ?
          stageInstance.outputLanes[0] : undefined,
        stagePreviewData = {
          input: [],
          output: [],
          errorRecords: []
        },
        batchData = previewData.batchesOutput[0];

      angular.forEach(batchData, function (stageOutput) {
        if (inputLane && stageOutput.output[inputLane] && stageOutput.output) {
          stagePreviewData.input = stageOutput.output[inputLane];
        } else if (outputLane && stageOutput.output[outputLane] && stageOutput.output) {
          stagePreviewData.output = stageOutput.output[outputLane];
          stagePreviewData.errorRecords = stageOutput.errorRecords;
        }
      });

      return stagePreviewData;
    };

    /**
     * Fetch fields information from Preview Data.
     *
     * @param lanePreviewData
     * @returns {Array}
     */
    var getFields = function(lanePreviewData) {
      var recordValues = _.isArray(lanePreviewData) && lanePreviewData.length ? lanePreviewData[0].value.value : [],
        fields = [];

      angular.forEach(recordValues, function(typeObject, fieldName) {
        fields.push({
          name : fieldName,
          type: typeObject.type,
          sampleValue: typeObject.value
        });
      });

      return fields;
    };

    /**
     * Update Stage Preview Data when stage selection changed.
     *
     * @param stageInstance
     */
    var updatePreviewDataForStage = function(stageInstance) {
      if($scope.previewMode) {
        var stageInstances = $scope.pipelineConfig.stages;

        $scope.stagePreviewData = getPreviewDataForStage($scope.previewData, stageInstance);

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
      } else {
        //In case of processors and targets run the preview to get input fields
        // if current state of config is previewable.
        if(stageInstance.uiInfo.stageType !== SOURCE_STAGE_TYPE) {
          if(!stageInstance.uiInfo.inputFields || stageInstance.uiInfo.inputFields.length === 0) {
            if($scope.pipelineConfig.previewable) {
              api.pipelineAgent.previewPipeline($scope.activeConfigInfo.name, $scope.previewSourceOffset, $scope.previewBatchSize).
                success(function (previewData) {
                  var stagePreviewData = getPreviewDataForStage(previewData, stageInstance);
                  stageInstance.uiInfo.inputFields = getFields(stagePreviewData.input);
                }).
                error(function(data) {
                  $rootScope.common.errors = [data];
                });
            }
          }
        }
      }
    };

    $scope.$on('previewPipeline', function(event, nextBatch) {
      $scope.stepExecuted = false;

      if (nextBatch) {
        $scope.previewSourceOffset += $scope.previewBatchSize;
      } else {
        $scope.previewSourceOffset = 0;
      }

      api.pipelineAgent.previewPipeline($scope.activeConfigInfo.name, $scope.previewSourceOffset, $scope.previewBatchSize).
        success(function (previewData) {

          $scope.previewData = previewData;
          $scope.previewDataUpdated = false;

          var firstStageInstance = $scope.pipelineConfig.stages[0];
          $scope.changeStageSelection(firstStageInstance);
        }).
        error(function(data) {
          $rootScope.common.errors = [data];
        });
    });

    $scope.$on('onStageSelection', function(event, stageInstance) {
      //if($scope.previewMode) {
        if (stageInstance) {
          updatePreviewDataForStage(stageInstance);
        } else {
          $scope.stagePreviewData = {
            input: {},
            output: {}
          };
        }
      //}
    });

  });