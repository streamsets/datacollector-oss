/**
 * Service for providing access to the Preview/Snapshot utility functions.
 */
angular.module('pipelineAgentApp.common')
  .service('previewService', function(api, $q) {

    var self = this;

    /**
     * Returns Preview input lane & output lane data for the given Stage Instance.
     *
     * @param batchData
     * @param stageInstance
     * @returns {{input: Array, output: Array}}
     */
    this.getPreviewDataForStage = function (batchData, stageInstance) {
      var stagePreviewData = {
          input: [],
          output: [],
          errorRecords: [],
          stageErrors: []
        };

      angular.forEach(batchData, function (stageOutput) {
        if(stageOutput.instanceName === stageInstance.instanceName) {

          angular.forEach(stageOutput.output, function(outputs, laneName) {
            angular.forEach(outputs, function(output) {
              output.laneName = laneName;
              stagePreviewData.output.push(output);
            });
          });

          stagePreviewData.errorRecords = stageOutput.errorRecords;
          stagePreviewData.stageErrors = stageOutput.stageErrors;
        }

        if(stageOutput.output && stageInstance.inputLanes && stageInstance.inputLanes.length) {
          angular.forEach(stageInstance.inputLanes, function(inputLane) {
            if(stageOutput.output[inputLane]) {
              angular.forEach(stageOutput.output[inputLane], function(input) {
                input.laneName = inputLane;
                stagePreviewData.input.push(input);
              });
            }
          });
        }

      });

      return stagePreviewData;
    };

    /**
     * Returns Preview input lane & output lane data for the given from Stage Instance & to Stage instance.
     *
     * @param batchData
     * @param fromStageInstance
     * @param toStageInstance
     * @returns {{input: Array, output: Array, errorRecords: Array, stageErrors: Array}}
     */
    this.getPreviewDataForMultiStage = function (batchData, fromStageInstance, toStageInstance) {
      var stagePreviewData = {
        input: [],
        output: [],
        errorRecords: [],
        stageErrors: []
      };

      angular.forEach(batchData, function (stageOutput) {
        if(stageOutput.instanceName === fromStageInstance.instanceName) {

          angular.forEach(stageOutput.output, function(outputs, laneName) {
            angular.forEach(outputs, function(output) {
              output.laneName = laneName;
              stagePreviewData.output.push(output);
            });
          });

          stagePreviewData.errorRecords = stageOutput.errorRecords;
          stagePreviewData.stageErrors = stageOutput.stageErrors;
        }

        if(stageOutput.output && toStageInstance.inputLanes && toStageInstance.inputLanes.length) {
          angular.forEach(toStageInstance.inputLanes, function(inputLane) {
            if(stageOutput.output[inputLane]) {
              angular.forEach(stageOutput.output[inputLane], function(input) {
                input.laneName = inputLane;
                stagePreviewData.input.push(input);
              });
            }
          });
        }

      });

      return stagePreviewData;
    };

    /**
     * Returns Input Records from Preview Data.
     *
     * @param pipelineName
     * @param stageInstance
     * @param batchSize
     * @returns {*}
     */
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


    /**
     * Returns Children of the stage instance in the graph.
     *
     * @param stageInstance
     * @param pipelineConfig
     * @returns {Array.<T>}
     */
    this.getStageChildren = function(stageInstance, pipelineConfig) {
      var stages = pipelineConfig.stages,
        index = _.indexOf(stages, stageInstance);

      return stages.slice(index + 1, stages.length + 1);
    };


    /**
     * Remove the record from Source.
     *
     * @param batchData
     * @param stageInstance
     * @param record
     */
    this.removeRecordFromSource = function(batchData, stageInstance, record) {
      var sourceOutput = batchData[0],
        foundLaneName,
        outputIndex;

      angular.forEach(sourceOutput.output, function(outputs, laneName) {
        angular.forEach(outputs, function(output, index) {
          if(output.header.sourceId === record.header.sourceId) {
            foundLaneName = laneName;
            outputIndex = index;
          }
        });
      });

      if(foundLaneName && outputIndex !== undefined) {
        sourceOutput.output[foundLaneName].splice(outputIndex, 1);
      }
    };


    /**
     * Return Additional Information about the record.
     * @param stageInstance
     * @param record
     * @param recordType
     */
    this.getRecordAdditionalInfo = function(stageInstance, record, recordType) {
      if(recordType === 'output' && stageInstance && stageInstance.outputLanes.length > 1) {
        var index = _.indexOf(stageInstance.outputLanes, record.laneName),
          lanePredicatesConfiguration = _.find(stageInstance.configuration, function(configuration) {
            return configuration.name === 'lanePredicates';
          }),
          info = '<span class="lane-label">lane: </span><span class="lane-value">' + (index + 1) + '</span>';


        if(lanePredicatesConfiguration) {
          var lanePredicate = lanePredicatesConfiguration.value[index];
          if(lanePredicate) {
            info += ', <span class="predicate-label">predicate: </span><span class="predicate-value">"' + lanePredicate.predicate + '"</span>';
          }
        }

        return ' (' + info  + ')';
      }

      return '';
    };

  });