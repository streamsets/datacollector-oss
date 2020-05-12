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
  .factory('pipelineTracking', function(tracking, pipelineConstant) {
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

    return pipelineTracking;
});
