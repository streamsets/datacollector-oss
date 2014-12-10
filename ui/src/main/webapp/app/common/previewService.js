/**
 * Service for providing access to the Preview/Snapshot utility functions.
 */
angular.module('pipelineAgentApp.common')
  .service('previewService', function(api, $q, $rootScope) {

    var self = this;

    /**
     * Returns Preview input lane & output lane data for the given Stage Instance.
     *
     * @param previewData
     * @param stageInstance
     * @returns {{input: Array, output: Array}}
     */
    this.getPreviewDataForStage = function (batchData, stageInstance) {
      var inputLane = (stageInstance.inputLanes && stageInstance.inputLanes.length) ?
          stageInstance.inputLanes[0] : undefined,
        outputLane = (stageInstance.outputLanes && stageInstance.outputLanes.length) ?
          stageInstance.outputLanes[0] : undefined,
        stagePreviewData = {
          input: [],
          output: [],
          errorRecords: []
        };

      angular.forEach(batchData, function (stageOutput) {
        if (inputLane && stageOutput.output[inputLane] && stageOutput.output) {
          stagePreviewData.input = stageOutput.output[inputLane];
        } else if (outputLane && stageOutput.output[outputLane] && stageOutput.output) {
          stagePreviewData.output = stageOutput.output[outputLane];
          stagePreviewData.errorRecords = stageOutput.errorRecords;
          stagePreviewData.stageErrors = stageOutput.stageErrors;
        }
      });

      return stagePreviewData;
    };

    this.getInputRecordsFromPreview = function(pipelineName, stageInstance, batchSize) {
      var deferred = $q.defer();
      api.pipelineAgent.previewPipeline(pipelineName, 0, batchSize).
        then(
          function (res) {
            var previewData = res.data,
              stagePreviewData = self.getPreviewDataForStage(previewData.batchesOutput[0], stageInstance);
            deferred.resolve(stagePreviewData.input);
          },
          function(res) {
            deferred.reject(res);
          }
        );

      return deferred.promise;
    };

  });