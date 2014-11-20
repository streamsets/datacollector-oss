/**
 * Controller for Library Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('PreviewController', function ($scope, _, api) {
    angular.extend($scope, {
      /**
       * Returns output records produced by input record.
       *
       * @param outputRecords
       * @param inputRecord
       * @returns {*}
       */
      getOutputRecords: function(outputRecords, inputRecord) {
        var matchedRecords = _.filter(outputRecords, function(outputRecord) {
          return outputRecord.header.previousStageTrackingId === inputRecord.header.trackingId;
        });

        return matchedRecords;
      },


      /**
       * Returns error records produced by input record.
       *
       * @param errorRecords
       * @param inputRecord
       * @returns {*}
       */
      getErrorRecords: function(errorRecords, inputRecord) {
        debugger;
        var matchedRecords = _.filter(errorRecords, function(outputRecord) {
          return outputRecord.record.header.trackingId === inputRecord.header.trackingId;
        });

        return matchedRecords;
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
        recordUpdated.values[fieldName].dirty = true;
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

          _.each(record.values, function(key, value) {
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


          }).
          error(function(data, status, headers, config) {
            $scope.httpErrors = [data];
          });
      }
    });
  });