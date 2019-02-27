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

 // Service for providing access to the Preview/Snapshot utility functions.
angular.module('dataCollectorApp.common')
  .service('previewService', function(api, $q, $translate, $timeout) {
    var self = this;
    var translations;

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
        stageErrors: [],
        newRecords: []
      };

      angular.forEach(batchData, function (stageOutput) {
        if(stageOutput.instanceName === stageInstance.instanceName) {
          angular.forEach(stageOutput.output, function(outputs, laneName) {
            angular.forEach(outputs, function(output, index) {
              output.laneName = laneName;
              if (index < 2) {
                output.expand = true;
              }
              stagePreviewData.output.push(output);
              if (output.header && !output.header.previousTrackingId) {
                stagePreviewData.newRecords.push(output);
              }
            });
          });
          stagePreviewData.errorRecords = stageOutput.errorRecords;
          stagePreviewData.stageErrors = stageOutput.stageErrors;
          stagePreviewData.eventRecords = [];
          if (stageOutput.eventRecords && stageOutput.eventRecords.length) {
            var eventLane = stageOutput.instanceName + '_EventLane';
            angular.forEach(stageOutput.eventRecords, function(eventRecord) {
              eventRecord.laneName = eventLane;
              stagePreviewData.eventRecords.push(eventRecord);
            });
          }
        }

        if(stageOutput.output && stageInstance.inputLanes && stageInstance.inputLanes.length) {
          angular.forEach(stageInstance.inputLanes, function(inputLane) {
            if(stageOutput.output[inputLane]) {
              angular.forEach(stageOutput.output[inputLane], function(input) {
                input.laneName = inputLane;
                stagePreviewData.input.push(input);
              });
            } else if (inputLane === stageOutput.instanceName + '_EventLane')  {
              angular.forEach(stageOutput.eventRecords, function(input) {
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

          stagePreviewData.eventRecords = [];
          if (stageOutput.eventRecords && stageOutput.eventRecords.length) {
            var eventLane = stageOutput.instanceName + '_EventLane';
            angular.forEach(stageOutput.eventRecords, function(eventRecord) {
              eventRecord.laneName = eventLane;
              stagePreviewData.eventRecords.push(eventRecord);
            });
          }
        }

        if(stageOutput.output && toStageInstance.inputLanes && toStageInstance.inputLanes.length) {
          angular.forEach(toStageInstance.inputLanes, function(inputLane) {
            if(stageOutput.output[inputLane]) {
              angular.forEach(stageOutput.output[inputLane], function(input) {
                input.laneName = inputLane;
                // filter records that does not belong to the selected stream
                if (input.header.trackingId.indexOf(fromStageInstance.instanceName) > 0) {
                    stagePreviewData.input.push(input);
                }
              });
            } else if (inputLane === stageOutput.instanceName + '_EventLane')  {
              angular.forEach(stageOutput.eventRecords, function(input) {
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
     * @param pipelineId
     * @param stageInstance
     * @param batchSize
     * @returns {*}
     */
    this.getInputRecordsFromPreview = function(pipelineId, stageInstance, batchSize) {
      var deferred = $q.defer();
      api.pipelineAgent.createPreview(pipelineId, 0, batchSize, 0, true, true, [], stageInstance.instanceName)
        .then(
          function (res) {
            var checkStatusDefer = $q.defer();
            checkForPreviewStatus(pipelineId, res.data.previewerId, checkStatusDefer);
            return checkStatusDefer.promise;
          },
          function(res) {
            deferred.reject(res);
          }
        )
        .then(
          function(previewData) {
            var stagePreviewData = self.getPreviewDataForStage(previewData.batchesOutput[0], stageInstance);
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
      api.pipelineAgent.createPreview(pipelineName, 0, batchSize, 0, true, true, [], edge.target.instanceName)
        .then(
          function (res) {
            var checkStatusDefer = $q.defer();
            checkForPreviewStatus(res.data.previewerId, checkStatusDefer);
            return checkStatusDefer.promise;
          },
          function(res) {
            deferred.reject(res);
          })
        .then(
          function(previewData) {
            var stagePreviewData = self.getPreviewDataForEdge(previewData.batchesOutput[0], edge);
            deferred.resolve(stagePreviewData.input);
          },
          function(res) {
            deferred.reject(res);
          });

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


    /**
     * Return Stage Lane info with index and condition.
     *
     * @param stageInstance
     * @returns {{}}
     */
    this.getStageLaneInfo = function(stageInstance) {
      var laneMap = {},
        lanePredicatesConfiguration = _.find(stageInstance.configuration, function(configuration) {
          return configuration.name === 'lanePredicates';
        });

      _.each(stageInstance.outputLanes, function(lane, index) {
        laneMap[lane] = {
          index: index + 1,
          condition: (lanePredicatesConfiguration ? lanePredicatesConfiguration.value[index].predicate : undefined)
        };
      });

      return laneMap;
    };

    /**
     * Return error counts for preview data.
     *
     * @param batchData
     * @returns {{}}
     */
    this.getPreviewStageErrorCounts = function(batchData) {
      var stageInstanceErrorCounts = {};

      angular.forEach(batchData, function (stageOutput) {
        var count = 0;

        if(stageOutput.errorRecords && stageOutput.errorRecords.length) {
          count += stageOutput.errorRecords.length;
        }

        if(stageOutput.stageErrors && stageOutput.stageErrors.length) {
          count += stageOutput.stageErrors.length;
        }

        stageInstanceErrorCounts[stageOutput.instanceName] = count;
      });

      return stageInstanceErrorCounts;
    };

    /**
     * Escape HTML utility function
     *
     * @param unsafe
     * @returns {*}
     */
    function escapeHtml(unsafe) {
      return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    /**
     * Check for Preview Status for every 1 seconds, once done open the snapshot view.
     *
     */
    var checkForPreviewStatus = function(pipelineId, previewerId, defer) {
      var previewStatusTimer = $timeout(
        function() {
        },
        500
      );

      previewStatusTimer.then(
        function() {
          api.pipelineAgent.getPreviewStatus(pipelineId, previewerId)
            .then(function(response) {
              var data = response.data;
              if(data && _.contains(['INVALID', 'START_ERROR', 'RUN_ERROR', 'CONNECT_ERROR', 'FINISHED', 'STOP_ERROR'], data.status)) {
                fetchPreviewData(pipelineId, previewerId, defer);
              } else {
                checkForPreviewStatus(pipelineId, previewerId, defer);
              }
            })
            .catch(function(response) {
              if (defer) {
                defer.reject();
              }
              $scope.common.errors = [response.data];
            });
        },
        function() {
          if (defer) {
            defer.reject();
          }
          console.log( "Timer rejected!" );
        }
      );
    };

    var fetchPreviewData = function(pipelineId, previewerId, defer) {
      api.pipelineAgent.getPreviewData(pipelineId, previewerId)
        .then(function(response) {
          var previewData = response.data;
          if(previewData.status !== 'FINISHED') {
            //Ignore
            defer.reject();
          } else {
            defer.resolve(previewData);
          }
        })
        .catch(function(response) {
          defer.reject();
          $scope.common.errors = [response.data];
        });
    };

  });
