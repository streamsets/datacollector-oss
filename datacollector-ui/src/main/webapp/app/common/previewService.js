/**
 * Service for providing access to the Preview/Snapshot utility functions.
 */
angular.module('dataCollectorApp.common')
  .service('previewService', function(api, $q, $translate) {

    var self = this,
      translations;


    $translate([
      'global.form.stream',
      'global.form.label',
      'global.form.condition'
    ]).then(function (_translations) {
      translations = _translations;
    });


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
      api.pipelineAgent.previewPipeline(pipelineName, 0, batchSize, 0, true, [], stageInstance.instanceName).
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
     * Returns Edge Input Records from Preview Data.
     *
     * @param pipelineName
     * @param edge
     * @param batchSize
     * @returns {*}
     */
    this.getEdgeInputRecordsFromPreview = function(pipelineName, edge, batchSize) {
      var deferred = $q.defer();
      api.pipelineAgent.previewPipeline(pipelineName, 0, batchSize, 0, true, [], edge.target.instanceName).
        then(
        function (res) {
          var previewData = res.data,
            stagePreviewData = self.getPreviewDataForEdge(previewData.batchesOutput[0], edge);
          deferred.resolve(stagePreviewData.input);
        },
        function(res) {
          deferred.reject(res);
        }
      );

      return deferred.promise;
    };


    /**
     * Returns Preview input lane & output lane data for the given Stage Instance.
     *
     * @param batchData
     * @param edge
     * @returns {{input: Array}}
     */
    this.getPreviewDataForEdge = function (batchData, edge) {
      var edgePreviewData = {
        input: []
      };

      angular.forEach(batchData, function (stageOutput) {
        if(stageOutput.instanceName === edge.source.instanceName) {
          edgePreviewData = {
            input: stageOutput.output[edge.outputLane]
          };
        }
      });

      return edgePreviewData;
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
          info = '<span class="lane-label">' + translations['global.form.stream'] + ': </span><span class="lane-value">' + (index + 1) + '</span>';


        if(lanePredicatesConfiguration) {
          var lanePredicate = lanePredicatesConfiguration.value[index];
          if(lanePredicate && lanePredicate.predicate) {
            info += ', <span class="predicate-label">' + translations['global.form.condition'] +
            ': </span><span class="predicate-value">' + escapeHtml(lanePredicate.predicate) + '</span>';
          }
        } else {
          var outputStreamLabels = stageInstance.uiInfo.outputStreamLabels,
            outputStreamLabel = outputStreamLabels ? outputStreamLabels[index] : undefined;

          if(outputStreamLabel) {
            info += ', <span class="predicate-label">' + translations['global.form.label'] + ': </span><span class="predicate-value">"' + outputStreamLabel + '"</span>';
          }
        }

        return ' (' + info  + ')';
      }

      return '';
    };

    function escapeHtml(unsafe) {
      return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

  });