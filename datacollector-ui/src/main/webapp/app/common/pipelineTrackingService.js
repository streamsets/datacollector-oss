/*
 * Copyright 2020 StreamSets Inc.
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
 * Helper functions for information needed for pipeline tracking
 */

angular.module('dataCollectorApp.common')
  .factory('pipelineTracking', function(tracking, pipelineConstant, api, $rootScope) {
    var pipelineTracking = {};

    /**
     * Get tracking data about the pipeline for MixPanel
     */
    pipelineTracking.getTrackingInfo = function(pipelineConfig) {
      var originStages = pipelineConfig.stages.filter(function(stage) {
        return stage.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE;
      });
      var destinationStages = pipelineConfig.stages.filter(function(stage) {
        return stage.uiInfo.stageType === pipelineConstant.TARGET_STAGE_TYPE;
      });
      var processorStages = pipelineConfig.stages.filter(function(stage) {
        return stage.uiInfo.stageType === pipelineConstant.PROCESSOR_STAGE_TYPE;
      });
      var destinationStageIds = destinationStages.map(function(stage) {
        return stage.instanceName;
      });
      var destinationStageNames = destinationStages.map(function(stage) {
        return stage.stageName;
      });
      var processorStageNames = processorStages.map(function(stage) {
        return stage.stageName;
      });
      var originStage = originStages[0] || {};
      var trackingData = {
        'Pipeline ID': pipelineConfig.pipelineId,
        'Number of Stages': pipelineConfig.stages.length,
        'Origin Stage ID': originStage.instanceName,
        'Origin Type Name': originStage.stageName,
        'Destination Stage ID List': destinationStageIds,
        'Destination Type Name List': destinationStageNames,
        'Processor Type Name List': processorStageNames
      };
      return trackingData;
    };

    /**
     * Tracks pipeline errors
     */
    pipelineTracking.pipelineError = function(activeConfigStatus, oldActiveConfigStatus, pipelineConfig) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      trackingData['Current Status'] = activeConfigStatus.status;
      trackingData['Previous Status'] = oldActiveConfigStatus.status;
      if (activeConfigStatus.attributes && activeConfigStatus.attributes.issues) {
        trackingData['Run Errors'] = pipelineTracking.getFlatIssueList(activeConfigStatus.attributes.issues);
        trackingData['Error Stage IDs'] = Object.keys(activeConfigStatus.attributes.issues.stageIssues);
      } else if (activeConfigStatus.attributes['ERROR_MESSAGE']) {
        trackingData['Run Errors'] = [activeConfigStatus.attributes['ERROR_MESSAGE']];
        trackingData['Error Stage IDs'] = [];
      } else {
        trackingData['Run Errors'] = ['Pipeline Status: ' + activeConfigStatus.status + ': ' +
          activeConfigStatus.message];
        trackingData['Error Stage IDs'] = [];
      }
      tracking.mixpanel.track('Pipeline Error', trackingData);
    };


    /**
     * Combines pipeline issues and stage issues into a single list,
     * with stage issues split to get only the code (e.g. JDBC_03)
     */
    pipelineTracking.getFlatIssueList = function(issues) {
      var issueList = [];
      var pipelineIssueMessages = [];
      if (issues.pipelineIssues) {
        pipelineIssueMessages = issues.pipelineIssues.map(function(issue) {
          return issue.message;
        });
      }
      if (issues.stageIssues) {
        for (var stage in issues.stageIssues) {
          issueList = issueList.concat(issues.stageIssues[stage].map(function(issue) {
            if (issue.message) {
              return issue.message.split(' ', 1)[0];
            } else {
              return JSON.stringify(issue);
            }
          }));
        }
      }
      issueList = issueList.concat(pipelineIssueMessages);
      return issueList;
    };

    /**
     * Gets object of current input, output, error, and batch records
     */
    pipelineTracking.getCurrentRecordCounts = function(pipelineMetrics) {
      var trackingData = {};

      var currentBatchCount;
      var currentInputRecordCount;
      var currentOutputRecordCount;
      var currentErrorRecordCount;
      var currentBatch = pipelineMetrics.counters['pipeline.batchCount.counter'];
      if (currentBatch) {
        currentBatchCount = currentBatch.count;
      }
      var currentInput = pipelineMetrics.counters['pipeline.batchInputRecords.counter'];
      if (currentInput) {
        currentInputRecordCount = currentInput.count;
      }
      var currentOutput = pipelineMetrics.counters['pipeline.batchOutputRecords.counter'];
      if (currentOutput) {
        currentOutputRecordCount = currentOutput.count;
      }
      var currentError = pipelineMetrics.counters['pipeline.batchErrorRecords.counter'];
      if (currentError) {
        currentErrorRecordCount = currentError.count;
      }

      trackingData['Batch Count'] = currentBatchCount;
      trackingData['Input Record Count'] = currentInputRecordCount;
      trackingData['Output Record Count'] = currentOutputRecordCount;
      trackingData['Error Record Count'] = currentErrorRecordCount;

      return trackingData;
    };

    /**
     * Gets time ago in minutes
     * @param unixTime - Time in milliseconds
     */
    pipelineTracking.timeAgoInMinutes = function(compareTime) {
      var diff = Date.now() - (compareTime - $rootScope.common.serverTimeDifference);
      return diff / 1000 / 60;
    };

    /**
     * Track pipeline stop event
     */
    pipelineTracking.trackPipelineStop = function(pipelineConfig, pipelineMetrics) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      var currentCounts = pipelineTracking.getCurrentRecordCounts(pipelineMetrics);
      Object.assign(trackingData, currentCounts);
      api.pipelineAgent.getHistory(pipelineConfig.pipelineId).then(function(res) {
        var stopTimes = res.data.filter(function(event) {
          return event.status === 'STOPPED';
        });
        var startTimes = res.data.filter(function(event) {
          return event.status === 'STARTING';
        });
        if (stopTimes.length > 0 && startTimes.length > 0) {
          var runTime = stopTimes[0].timeStamp - startTimes[0].timeStamp;
          trackingData['Run Time'] = pipelineTracking.timeAgoInMinutes(runTime);
        }
      }).finally(function() {
        tracking.mixpanel.track('Pipeline Stopped', trackingData);
      });
    };

    /**
     * Track pipeline stop request
     */
    pipelineTracking.trackPipelineStopRequest = function(pipelineStatusMap, pipelineId, forceStop, pipelineConfig, pipelineMetrics) {
      var previousPipelineTime;
      if(pipelineStatusMap[pipelineId]) {
        previousPipelineTime = pipelineStatusMap[pipelineId].timeStamp;
      }
      var trackingData;
      if (pipelineConfig) {
        trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
        var currentCounts = pipelineTracking.getCurrentRecordCounts(pipelineMetrics);
        Object.assign(trackingData, currentCounts);
      } else {
        trackingData = {'Pipeline ID': pipelineId};
      }
      trackingData['Stop Type'] = 'User';
      trackingData['Force Stop'] = forceStop;
      if (previousPipelineTime) {
        trackingData['Run Time'] = pipelineTracking.timeAgoInMinutes(previousPipelineTime);
      }
      tracking.mixpanel.track('Pipeline Stop Requested', trackingData);
    };

    /**
     * Track run reported and first pipeline microbatch
     */
    pipelineTracking.trackRunReported = function(pipelineMetrics, pipelineConfig) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      var currentCounts = pipelineTracking.getCurrentRecordCounts(pipelineMetrics);
      var currentBatchCount = currentCounts['Batch Count'];
      var currentInputRecordCount = currentCounts['Input Record Count'];
      Object.assign(trackingData, currentCounts);
      var previousBatchCount = $rootScope.common.previousBatchCount[pipelineConfig.pipelineId];
      if (previousBatchCount === 0 && currentBatchCount > 0) {
        tracking.mixpanel.track('First Pipeline Microbatch', trackingData);
      }
      $rootScope.common.previousBatchCount[pipelineConfig.pipelineId] = currentBatchCount;
      var previousInputRecordCount = $rootScope.common.previousInputRecordCount[pipelineConfig.pipelineId];
      if (previousInputRecordCount === 0 && currentInputRecordCount > 0) {
        tracking.mixpanel.track('Run Reported', trackingData);
      }
      $rootScope.common.previousInputRecordCount[pipelineConfig.pipelineId] = currentInputRecordCount;
    };

    pipelineTracking.trackValidationSelected = function(pipelineConfig) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      tracking.mixpanel.track('Validation Selected', trackingData);
      tracking.FS.event('Validation Selected', trackingData);
    };

    pipelineTracking.trackValidationComplete = function(pipelineConfig, success, previewData) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      trackingData['Validation Successful'] = success;
      trackingData['Validation Error'] = [];
      if (previewData) {
        if (previewData.issues) {
          var issueList = pipelineTracking.getFlatIssueList(previewData.issues);
          trackingData["Validation Error"] = issueList;
        } else if (previewData.message) {
          trackingData["Validation Error"] = [JSON.stringify(previewData.message)];
        }
      }
      tracking.mixpanel.track('Validation Complete', trackingData);
    };

    pipelineTracking.trackRunSelected = function(pipelineConfig, withParameters) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      trackingData['With Parameters'] = withParameters;
      tracking.mixpanel.track('Run Selected', trackingData);
      tracking.FS.event('Run Selected', trackingData);
    };

    pipelineTracking.trackStageAdded = function(stageInstance, pipelineId) {
      var stageTrackingDetail = {
        'Pipeline ID': pipelineId,
        'Stage ID': stageInstance.instanceName,
        'Stage Type Name': stageInstance.stageName,
        'Library Name': stageInstance.library
      };
      if (stageInstance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE) {
        tracking.mixpanel.track('Origin Added', stageTrackingDetail);
        tracking.mixpanel.people.set({
          'Core Journey Stage - Origin Added': true
        });
      } else if (stageInstance.uiInfo.stageType == pipelineConstant.PROCESSOR_STAGE_TYPE) {
        tracking.mixpanel.track('Processor Added', stageTrackingDetail);
        tracking.mixpanel.people.set({
          'Core Journey Stage - Processor Added': true
        });
      } else if (stageInstance.uiInfo.stageType == pipelineConstant.TARGET_STAGE_TYPE) {
        tracking.mixpanel.track('Destination Added', stageTrackingDetail);
        tracking.mixpanel.people.set({
          'Core Journey Stage - Destination Added': true
        });
      } else if (stageInstance.uiInfo.stageType == pipelineConstant.EXECUTOR_STAGE_TYPE) {
        tracking.mixpanel.track('Executor Added', stageTrackingDetail);
        tracking.mixpanel.people.set({
          'Core Journey Stage - Executor Added': true
        });
      } else {
        tracking.mixpanel.track('Stage with unknown type added', stageTrackingDetail);
      }
    };

    pipelineTracking.trackPreviewSelected = function(pipelineConfig) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      tracking.mixpanel.track('Preview Selected', trackingData);
      tracking.FS.event('Preview Selected', trackingData);
    };

    pipelineTracking.trackPreviewClosed = function(pipelineConfig) {
      tracking.mixpanel.track('Preview Closed', {'Pipeline ID': pipelineConfig ? pipelineConfig.pipelineId : 'N/A'});
    };

    pipelineTracking.trackPreviewConfigComplete = function(pipelineConfig, previewConfig) {
      var trackingData = pipelineTracking.getTrackingInfo(pipelineConfig);
      trackingData['Preview Source'] = previewConfig.previewSource;
      trackingData['Preview Batch Size'] = previewConfig.batchSize;
      trackingData['Preview Timeout'] = previewConfig.timeout;
      trackingData['Has Write to Destinations'] = previewConfig.writeToDestinations;
      trackingData['Has Pipeline Lifecycle Events'] = previewConfig.executeLifecycleEvents;
      trackingData['Has Show Record Field Header'] = previewConfig.showHeader;
      trackingData['Has Show Field Type'] = previewConfig.showFieldType;
      trackingData['Has Remember The Configuration'] = previewConfig.rememberMe;
      tracking.mixpanel.people.set({'Core Journey Stage - Preview Run': true});
      tracking.mixpanel.track('Preview Config Complete', trackingData);
    };

    return pipelineTracking;
});
