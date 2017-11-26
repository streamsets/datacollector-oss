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
/**
 * Service for providing access to the backend API via HTTP.
 */

angular.module('dataCollectorApp.common')
  .factory('api', function($rootScope, $http, $q) {
    var apiVersion = 'v1',
      apiBase = 'rest/' + apiVersion,
      api = {
        apiVersion: apiVersion,
        events: {}
      };

    api.log = {
      /**
       * Fetch current log
       *
       * @param endingOffset
       * @param extraMessage
       * @param filterPipeline
       * @param filterSeverity
       */
      getCurrentLog: function(endingOffset, extraMessage, filterPipeline, filterSeverity) {
        var url = apiBase + '/system/logs?endingOffset=' +  (endingOffset ? endingOffset : '-1');

        if (extraMessage) {
          url += '&extraMessage=' + extraMessage;
        }

        if (filterPipeline) {
          url += '&pipeline=' + filterPipeline;
         }

        if (filterSeverity) {
          url += '&severity=' + filterSeverity;
        }

        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetch list of Log file names
       *
       * @returns {*}
       */
      getFilesList: function() {
        var url = apiBase + '/system/logs/files';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get Log Config
       * @param def
       * @returns {*}
       */
      getLogConfig: function(def) {
        var url = apiBase + '/system/log/config?default=' + def;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Update Log Config
       * @param logConfig
       * @returns {*}
       */
      updateLogConfig: function(logConfig) {
        var url = apiBase + '/system/log/config';
        return $http({
          method: 'POST',
          url: url,
          data: logConfig,
          headers:  {
            'Content-Type': 'text/plain'
          }
        });
      }
    };

    api.admin = {

      /**
       * Fetches JVM Metrics
       * @returns {*}
       */
      getJMX : function() {
        var url = apiBase + '/system/jmx';
        return $http({
          method: 'GET',
          url: url
        });
      },


      /**
       * Fetches JVM Thread Dump
       */
      getThreadDump: function() {
        var url = apiBase + '/system/threads';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches User Information
       */
      getUserInfo: function() {
        var url = apiBase + '/system/info/currentUser';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Build Information
       */
      getBuildInfo: function() {
        var url = apiBase + '/system/info';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Remote Server Info
       */
      getRemoteServerInfo: function() {
        var url = apiBase + '/system/info/remote';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches SDC ID
       */
      getSdcId: function() {
        var url = apiBase + '/system/info/id';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Shutdown the Data Collector.
       * @returns {*}
       */
      shutdownCollector: function() {
        var url = apiBase + '/system/shutdown';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Restart the Data Collector.
       * @returns {*}
       */
      restartDataCollector: function() {
        var url = apiBase + '/system/restart';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Update Application Token
       * @returns {*}
       */
      updateApplicationToken: function(authToken) {
        var url = apiBase + '/system/appToken';
        return $http({
          method: 'POST',
          url: url,
          data: authToken
        });
      },

      /**
       * Enable SCH
       * @param dpmInfo
       */
      enableDPM: function(dpmInfo) {
        var url = apiBase + '/system/enableDPM';
        return $http({
          method: 'POST',
          url: url,
          data: dpmInfo
        });
      },

      /**
       * Disable SCH
       */
      disableDPM: function() {
        var url = apiBase + '/system/disableDPM';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Create SCH Groups & Users
       * @param dpmInfo
       * @returns {*}
       */
      createDPMGroupsAndUsers: function(dpmInfo) {
        var url = apiBase + '/system/createDPMUsers';
        return $http({
          method: 'POST',
          url: url,
          data: dpmInfo
        });
      },

      /**
       * logout
       */
      logout: function(authenticationType, isDPMEnabled) {
        var url;
        if (isDPMEnabled) {
          url = 'logout';
          return $http({
            method: 'GET',
            url: url
          });
        } else {
          url = apiBase + '/authentication/logout';
          return $http({
            method: 'POST',
            url: url
          });
        }
      },

      /**
       * Returns SDC Directories
       * @returns {*}
       */
      getSDCDirectories: function() {
        var url = apiBase + '/system/directories';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Returns Server Time
       */
      getServerTime: function() {
        var url = apiBase + '/system/info/serverTime';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Returns Groups
       * @returns {*}
       */
      getGroups: function() {
        var url = apiBase + '/system/groups';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Returns Users
       * @returns {*}
       */
      getUsers: function() {
        var url = apiBase + '/system/users';
        return $http({
          method: 'GET',
          url: url
        });
      }

    };

    api.pipelineAgent = {
      /**
       * Fetches Configuration from dist/src/main/etc/pipeline.properties
       *
       * @returns {*}
       */
      getConfiguration: function() {
        var url = apiBase + '/system/configuration';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches UI Configuration
       *
       * @returns {*}
       */
      getUIConfiguration: function() {
        var url = apiBase + '/system/configuration/ui';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches all configuration definitions of Pipeline and Stage Configuration.
       *
       * @returns {*}
       */
      getDefinitions: function() {
        var url = apiBase + '/definitions';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches all libraries information from archives/nightly.
       *
       * @returns {*}
       */
      getLibraries: function(repoUrl, installedOnly) {
        var url = apiBase + '/stageLibraries/list?installedOnly=' + !!installedOnly;

        if (repoUrl) {
          url += '&repoUrl=' + repoUrl;
        }

        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Install library from archives/nightly
       *
       * @returns {*}
       */
      installLibraries: function(repoUrl, libraryList) {
        var url = apiBase + '/stageLibraries/install';
        if (repoUrl) {
          url += '?repoUrl=' + repoUrl;
        }

        return $http({
          method: 'POST',
          url: url,
          data: libraryList
        });
      },

      /**
       * Uninstall library from archives/nightly
       *
       * @returns {*}
       */
      uninstallLibraries: function(libraryList) {
        var url = apiBase + '/stageLibraries/uninstall';
        return $http({
          method: 'POST',
          url: url,
          data: libraryList
        });
      },

      /**
       * Fetches all installed additional drivers
       *
       * @returns {*}
       */
      getStageLibrariesExtras: function() {
        var url = apiBase + '/stageLibraries/extras/list';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Update Stage Libraries extras
       *
       * @param libraryId
       * @param file
       * @returns {*}
       */
      installExtras: function (libraryId, file) {
        var url = apiBase + '/stageLibraries/extras/' + libraryId + '/upload';
        var formData = new FormData();
        formData.append('file', file);
        return $http.post(url, formData, {
          transformRequest: angular.identity,
          headers: {'Content-Type': undefined}
        });
      },


      /**
       * Delete Stage Libraries extras
       *
       * @param extrasList
       * @returns {*}
       */
      deleteExtras: function (extrasList) {
        var url = apiBase + '/stageLibraries/extras/delete';
        return $http({
          method: 'POST',
          url: url,
          data: extrasList
        });
      },

      /**
       * Return total pipelines count.
       *
       * @returns {*}
       */
      getPipelinesCount: function() {
        var url = apiBase + '/pipelines/count';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Returns System Pipeline Labels.
       *
       * @returns {*}
       */
      getSystemPipelineLabels: function() {
        var url = apiBase + '/pipelines/systemLabels';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Returns all Pipeline labels.
       *
       * @returns {*}
       */
      getPipelineLabels: function() {
        var url = apiBase + '/pipelines/labels';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches all Pipeline Configuration Info.
       *
       * @returns {*}
       */
      getPipelines: function(filterText, label, offset, len, orderBy, order, includeStatus) {
        if (!orderBy) {
          orderBy = 'NAME';
        }

        if (!order) {
          order = 'ASC';
        }

        var url = apiBase + '/pipelines?orderBy=' + orderBy + '&order=' + order;

        if (filterText) {
          url += '&filterText=' + filterText;
        }

        if (label) {
          url += '&label=' + label;
        }

        if (offset !== undefined) {
          url += '&offset=' + offset;
        }

        if (len) {
          url += '&len=' + len;
        }

        if (includeStatus) {
          url += '&includeStatus=' + includeStatus;
        }

        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Pipeline Configuration.
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfig: function(name) {
        var url = apiBase + '/pipeline/' + name;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Pipeline Configuration Information
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfigInfo: function(name) {
        var url = apiBase + '/pipeline/' + name + '?get=info';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Sends updated Pipeline configuration to server for update.
       *
       * @param name - Pipeline Name
       * @param config - Modified Pipeline Configuration
       * @returns Updated Pipeline Configuration
       */
      savePipelineConfig: function(name, config) {
        var url = apiBase + '/pipeline/' + name;
        return $http({
          method: 'POST',
          url: url,
          data: config
        });
      },

      /**
       * Create new Pipeline Configuration.
       *
       * @param name
       * @param description
       */
      createNewPipelineConfig: function(name, description) {
        var url = apiBase + '/pipeline/' + encodeURIComponent(name) + '?autoGeneratePipelineId=true&description=' + description;

        return $http({
          method: 'PUT',
          url: url
        });
      },

      /**
       * Delete Pipeline Configuration.
       *
       * @param name
       * @returns {*}
       */
      deletePipelineConfig: function(name) {
        var url = apiBase + '/pipeline/' + name;
        return $http({
          method: 'DELETE',
          url: url
        });
      },

      /**
       * Delete Pipelines.
       *
       * @param pipelineNames
       * @returns {*}
       */
      deletePipelines: function(pipelineNames) {
        var url = apiBase + '/pipelines/delete';
        return $http({
          method: 'POST',
          url: url,
          data: pipelineNames
        });
      },


      /**
       * Duplicate Pipeline Configuration
       *
       * @param label
       * @param description
       * @param pipelineObject
       * @param pipelineRulesObject
       * @returns {*|promise}
       */
      duplicatePipelineConfig: function(label, description, pipelineObject, pipelineRulesObject) {
        var deferred = $q.defer();
        var duplicatePipelineObject;
        var duplicatePipelineRulesObject;
        var name;

        // Create new config object
        // then copy the configuration from pipelineInfo to new Object.
        api.pipelineAgent.createNewPipelineConfig(label, description)
          .then(function(res) {
            duplicatePipelineObject = res.data;
            duplicatePipelineObject.configuration = pipelineObject.configuration;
            duplicatePipelineObject.uiInfo = pipelineObject.uiInfo;
            duplicatePipelineObject.errorStage = pipelineObject.errorStage;
            duplicatePipelineObject.statsAggregatorStage = pipelineObject.statsAggregatorStage;
            duplicatePipelineObject.stages = pipelineObject.stages;
            duplicatePipelineObject.startEventStages = pipelineObject.startEventStages;
            duplicatePipelineObject.stopEventStages = pipelineObject.stopEventStages;
            if (pipelineObject.metadata && pipelineObject.metadata.labels) {
              duplicatePipelineObject.metadata = {
                labels: pipelineObject.metadata.labels
              };
            }
            name = duplicatePipelineObject.info.pipelineId;
            return api.pipelineAgent.savePipelineConfig(name, duplicatePipelineObject);
          })
          .then(function(res) {
            duplicatePipelineObject = res.data;

            //Fetch the Pipeline Rules
            return api.pipelineAgent.getPipelineRules(name);
          })
          .then(function(res) {
            duplicatePipelineRulesObject = res.data;
            duplicatePipelineRulesObject.metricsRuleDefinitions = pipelineRulesObject.metricsRuleDefinitions;
            duplicatePipelineRulesObject.dataRuleDefinitions = pipelineRulesObject.dataRuleDefinitions;
            duplicatePipelineRulesObject.driftRuleDefinitions = pipelineRulesObject.driftRuleDefinitions;
            duplicatePipelineRulesObject.emailIds = pipelineRulesObject.emailIds;
            duplicatePipelineRulesObject.configuration = pipelineRulesObject.configuration;

            //Save the pipeline Rules
            return api.pipelineAgent.savePipelineRules(name, duplicatePipelineRulesObject);
          })
          .then(function(res) {
            deferred.resolve(duplicatePipelineObject);
          },function(res) {
            deferred.reject(res);
          });

        return deferred.promise;
      },

      /**
       * Export Pipeline Configuration.
       *
       * @param name
       * @param includeLibraryDefinitions
       */
      exportPipelineConfig: function(name, includeLibraryDefinitions) {
        var url = apiBase + '/pipeline/' + name + '/export?attachment=true';
        if (includeLibraryDefinitions) {
          url += '&includeLibraryDefinitions=true';
        }
        window.open(url, '_blank', '');
      },

      /**
       * Export Pipelines.
       *
       * @param pipelineIds
       * @param includeLibraryDefinitions
       */
      exportSelectedPipelines: function(pipelineIds, includeLibraryDefinitions) {

        var url = apiBase + '/pipelines/export';
        if (includeLibraryDefinitions) {
          url += '?includeLibraryDefinitions=true';
        }

        var xhr = new XMLHttpRequest();
        xhr.open('POST', url, true);
        xhr.responseType = 'arraybuffer';
        xhr.onload = function () {
          if (this.status === 200) {
            var filename = "";
            var disposition = xhr.getResponseHeader('Content-Disposition');
            if (disposition && disposition.indexOf('attachment') !== -1) {
              var filenameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/;
              var matches = filenameRegex.exec(disposition);
              if (matches !== null && matches[1]) {
                filename = matches[1].replace(/['"]/g, '');
              }
            }
            var type = xhr.getResponseHeader('Content-Type');

            var blob = new Blob([this.response], { type: type });

            var URL = window.URL || window.webkitURL;
            var downloadUrl = URL.createObjectURL(blob);

            if (filename) {
              // use HTML5 a[download] attribute to specify filename
              var a = document.createElement("a");
              // safari doesn't support this yet
              if (typeof a.download === 'undefined') {
                window.location = downloadUrl;
              } else {
                a.href = downloadUrl;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
              }
            } else {
              window.location = downloadUrl;
            }

            setTimeout(function () { URL.revokeObjectURL(downloadUrl); }, 100); // cleanup
          }
        };
        xhr.setRequestHeader('Content-type', 'application/json');
        xhr.setRequestHeader('X-Requested-By', 'Data Collector');
        xhr.send(JSON.stringify(pipelineIds));
      },

      /**
       * Import Pipeline Configuration.
       *
       * @param pipelineName
       * @param pipelineEnvelope
       * @param overwrite
       */
      importPipelineConfig: function(pipelineName, pipelineEnvelope, overwrite, autoGeneratePipelineId) {
        var url = apiBase + '/pipeline/' + pipelineName + '/import?autoGeneratePipelineId=' + !!autoGeneratePipelineId;
        if (overwrite) {
          url += '&overwrite=' + overwrite;
        }

        return $http({
          method: 'POST',
          url: url,
          data: pipelineEnvelope
        });
      },

      importPipelines: function(formData) {
        var url = apiBase + '/pipelines/import';
        return $http({
          method: 'POST',
          url: url,
          data: formData,
          headers: {'Content-Type': undefined}
        });
      },

      /**
       * Download Edge Executable.
       *
       * @param edgeOs
       * @param edgeArch
       * @param pipelineIds
       */
      downloadEdgeExecutable: function(edgeOs, edgeArch, pipelineIds) {
        var url = apiBase + '/pipelines/executable?edgeOs=' + edgeOs + '&edgeArch=' + edgeArch +
          '&pipelineIds=' + pipelineIds.join(',');
        window.open(url, '_blank', '');
      },

      /**
       * Start Preview for given Pipeline name
       *
       * @param name
       * @param sourceOffset
       * @param batchSize
       * @param rev
       * @param skipTargets
       * @param skipLifecycleEvents
       * @param stageOutputList
       * @param endStage
       * @param timeout
       * @returns {*}
       */
      createPreview: function(
        name,
        sourceOffset,
        batchSize,
        rev,
        skipTargets,
        skipLifecycleEvents,
        stageOutputList,
        endStage,
        timeout
      ) {
        var url;

        if (!batchSize) {
          batchSize = 10;
        }

        if (!timeout || timeout <=0) {
          timeout = 10000;
        }

        url = apiBase + '/pipeline/' + name + '/preview?batchSize=' + batchSize + '&rev=' + rev +
            '&skipTargets=' + skipTargets + '&timeout=' + timeout + '&skipLifecycleEvents=' + skipLifecycleEvents;

        if (endStage) {
          url += '&endStage=' + endStage;
        }

        return $http({
          method: 'POST',
          url: url,
          data: stageOutputList || []
        });
      },


      /**
       * Fetches Preview Status
       *
       * @param previewerId
       * @param pipelineName
       */
      getPreviewStatus: function(previewerId, pipelineName) {
        var url = apiBase + '/pipeline/pipelineName/preview/' + previewerId + '/status' ;
        return $http({
          method: 'GET',
          url: url
        });
      },


      /**
       * Fetches Preview Data
       *
       * @param previewerId
       * @param pipelineName
       */
      getPreviewData: function(previewerId, pipelineName) {
        var url = apiBase + '/pipeline/pipelineName/preview/' + previewerId;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Stop Preview
       *
       * @param previewerId
       * @param pipelineName
       */
      cancelPreview: function(previewerId, pipelineName) {
        var url = apiBase + '/pipeline/pipelineName/preview/' + previewerId;
        return $http({
          method: 'DELETE',
          url: url
        });
      },

      /**
       * Fetch all Pipeline Status
       *
       * @returns {*}
       */
      getAllPipelineStatus: function() {
        var url = apiBase + '/pipelines/status';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetch the Pipeline Status
       *
       * @returns {*}
       */
      getPipelineStatus: function(pipelineName, rev) {
        var url = apiBase + '/pipeline/' + pipelineName + '/status?rev=' + rev;
        return $http({
          method: 'GET',
          url: url
        });
      },


      /**
       * Validate the Pipeline
       *
       * @param name
       * @returns {*}
       */
      validatePipeline: function(name) {
        var url = apiBase + '/pipeline/' + name + '/validate?timeout=500000';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Start the Pipeline
       *
       * @param pipelineName
       * @param rev
       * @param runtimeParameters
       * @returns {*}
       */
      startPipeline: function(pipelineName, rev, runtimeParameters) {
        var url = apiBase + '/pipeline/' + pipelineName + '/start?rev=' + rev ;
        return $http({
          method: 'POST',
          url: url,
          data: runtimeParameters
        });
      },

      /**
       * Start multiple Pipelines
       *
       * @param pipelineNames
       * @returns {*}
       */
      startPipelines: function(pipelineNames) {
        var url = apiBase + '/pipelines/start';
        return $http({
          method: 'POST',
          url: url,
          data: pipelineNames
        });
      },

      /**
       * Stop the Pipeline
       *
       * @returns {*}
       */
      stopPipeline: function(pipelineName, rev, forceStop) {
        var url = apiBase + '/pipeline/' + pipelineName + '/stop?rev=' + rev ;
        if (forceStop) {
          url = apiBase + '/pipeline/' + pipelineName + '/forceStop?rev=' + rev ;
        }
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Stop multiple Pipelines
       *
       * @param pipelineNames
       * @returns {*}
       */
      stopPipelines: function(pipelineNames, forceStop) {
        var url = apiBase + '/pipelines/stop';
        if (forceStop) {
          url = apiBase + '/pipelines/forceStop';
        }
        return $http({
          method: 'POST',
          url: url,
          data: pipelineNames
        });
      },

      /**
       * Fetch the Pipeline Metrics
       *
       * @returns {*}
       */
      getPipelineMetrics: function(pipelineName, rev) {
        var url = apiBase + '/pipeline/' + pipelineName + '/metrics?rev=' + rev ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get List of available snapshots.
       *
       * @returns {*}
       */
      getSnapshotsInfo: function() {
        var url = apiBase + '/pipelines/snapshots' ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Capture Snapshot of running pipeline.
       *
       * @param pipelineName
       * @param rev
       * @param snapshotName
       * @param snapshotLabel
       * @param batchSize
       * @param startPipeline
       * @returns {*}
       */
      captureSnapshot: function(pipelineName, rev, snapshotName, snapshotLabel, batchSize, startPipeline) {
        var url = apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName +
          '?batchSize=' + batchSize +
          '&snapshotLabel=' + snapshotLabel +
          '&rev=' + rev;

        if (startPipeline) {
          url += '&startPipeline=true';
        }

        return $http({
          method: 'PUT',
          url: url
        });
      },

      /**
       * Update Snapshot label
       *
       * @param pipelineName
       * @param rev
       * @param snapshotName
       * @param snapshotLabel
       * @returns {*}
       */
      updateSnapshotLabel: function(pipelineName, rev, snapshotName, snapshotLabel) {
        var url = apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName +
          '?snapshotLabel=' + snapshotLabel +
          '&rev=' + rev;
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Get Status of Snapshot.
       *
       * @param pipelineName
       * @param rev
       * @param snapshotName
       * @returns {*}
       */
      getSnapshotStatus: function(pipelineName, rev, snapshotName) {
        var url = apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName + '/status?rev=' + rev;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get captured snapshot for given pipeline name.
       *
       * @param pipelineName
       * @param rev
       * @param snapshotName
       * @returns {*}
       */
      getSnapshot: function(pipelineName, rev, snapshotName) {
        var url = apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName + '?rev=' + rev;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Download captured snapshot for given pipeline name.
       *
       * @param pipelineName
       * @param rev
       * @param snapshotName
       * @returns {*}
       */
      downloadSnapshot: function(pipelineName, rev, snapshotName) {
        var url = apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName + '?attachment=true&rev=' + rev;
        window.open(url, '_blank', '');
      },

      /**
       * Delete captured snapshot for given pipeline name.
       *
       * @param pipelineName
       * @param rev
       * @param snapshotName
       * @returns {*}
       */
      deleteSnapshot: function(pipelineName, rev, snapshotName) {
        var url = apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName + '?rev=' + rev;
        return $http({
          method: 'DELETE',
          url: url
        });
      },

      /**
       * Get error records for the given stage instance name of running pipeline if it is provided otherwise
       * return error records for the pipeline.
       *
       * @param pipelineName
       * @param rev
       * @param stageInstanceName
       * @returns {*}
       */
      getErrorRecords: function(pipelineName, rev, stageInstanceName) {
        var url = apiBase + '/pipeline/' + pipelineName + '/errorRecords?rev=' + rev +
          '&stageInstanceName=' + stageInstanceName;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get error messages for the given stage instance name of running pipeline if is provided otherwise
       * return error messages for the pipeline.
       *
       * @param pipelineName
       * @param rev
       * @param stageInstanceName
       * @returns {*}
       */
      getErrorMessages: function(pipelineName, rev, stageInstanceName) {
        var url = apiBase + '/pipeline/' + pipelineName + '/errorMessages?rev=' + rev +
          '&stageInstanceName=' + stageInstanceName;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Raw Source Preview
       *
       * @param name
       * @param rev
       * @param configurations
       * @returns {*}
       */
      rawSourcePreview: function(name, rev, configurations) {
        var url = apiBase + '/pipeline/' + name + '/rawSourcePreview?rev=' + rev;

        angular.forEach(configurations, function(config) {
          if (config.name && config.value !== undefined) {
            url+= '&' + config.name + '=' + config.value;
          }
        });

        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get history of the pipeline
       *
       * @param name
       * @param rev
       * @returns {*}
       */
      getHistory: function(name, rev) {
        var url = apiBase + '/pipeline/' + name + '/history';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Clear history of the pipeline
       *
       * @param name
       * @param rev
       * @returns {*}
       */
      clearHistory: function(name, rev) {
        var url = apiBase + '/pipeline/' + name + '/history';
        return $http({
          method: 'DELETE',
          url: url
        });
      },

      /**
       * Reset Offset for Pipeline
       *
       * @param name
       */
      resetOffset: function(name) {
        var url = apiBase + '/pipeline/' + name + '/resetOffset';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Reset Offset for Multiple Pipelines
       *
       * @param pipelineNames
       */
      resetOffsets: function(pipelineNames) {
        var url = apiBase + '/pipelines/resetOffsets';
        return $http({
          method: 'POST',
          url: url,
          data: pipelineNames
        });
      },

      /**
       * Add Labels to Multiple Pipelines
       *
       * @param labels
       * @param pipelineNames
       */
      addLabelsToPipelines: function(labels, pipelineNames) {
        var url = apiBase + '/pipelines/addLabels';
        return $http({
          method: 'POST',
          url: url,
          data: {
            labels: labels,
            pipelineNames: pipelineNames
          }
        });
      },

      /**
       * Fetches Pipeline Rules.
       *
       * @param name
       * @returns {*}
       */
      getPipelineRules: function(name) {
        var url;

        url = apiBase + '/pipeline/' + name + '/rules';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Sends updated Pipeline rules to server for update.
       *
       * @param name - Pipeline Name
       * @param rules - Modified Pipeline Configuration
       * @returns Updated Pipeline Rules
       */
      savePipelineRules: function(name, rules) {
        var url = apiBase + '/pipeline/' + name + '/rules';
        return $http({
          method: 'POST',
          url: url,
          data: rules
        });
      },

      /**
       * Sends updated Pipeline UI Info to server for update.
       *
       * @param name - Pipeline Name
       * @param uiInfo - Modified Pipeline UI Info
       * @returns Updated Pipeline Rules
       */
      savePipelineUIInfo: function(name, uiInfo) {
        var url = apiBase + '/pipeline/' + name + '/uiInfo';
        return $http({
          method: 'POST',
          url: url,
          data: uiInfo
        });
      },

      /**
       * Sends updated Pipeline metadata to server for update.
       *
       * @param name - Pipeline Name
       * @param metadata - Modified Pipeline UI Info
       * @returns Updated Pipeline metadata
       */
      savePipelineMetadata: function(name, metadata) {
        var url = apiBase + '/pipeline/' + name + '/metadata';
        return $http({
          method: 'POST',
          url: url,
          data: metadata
        });
      },

      /**
       * Get Sampled data for given sampling rule id.
       *
       * @param pipelineName
       * @param samplingRuleId
       * @param sampleSize
       * @returns {*}
       */
      getSampledRecords: function(pipelineName, samplingRuleId, sampleSize) {
        var url = apiBase + '/pipeline/' + pipelineName + '/sampledRecords?sampleId=' + samplingRuleId +
          '&sampleSize=' + sampleSize;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get all pipeline alers
       */
      getAllAlerts: function() {
        var url = apiBase + '/pipelines/alerts' ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Delete Alert
       *
       * @param name
       * @param ruleId
       * @returns {*}
       */
      deleteAlert: function(name, ruleId) {
        var url = apiBase + '/pipeline/' + name + '/alerts?alertId=' + ruleId;

        return $http({
          method: 'DELETE',
          url: url
        });
      }
    };

    api.remote = {
      publishPipeline: function(remoteBaseURL, ssoToken, name, commitPipelineModel) {
        var deferred = $q.defer();
        var remoteURL = remoteBaseURL + 'pipelinestore/rest/v1/pipelines';
        var url = apiBase + '/pipeline/' + name + '/export?includeLibraryDefinitions=true';
        var newMetadata;
        $http({
          method: 'GET',
          url: url
        }).then(function(res) {
          var pipeline = res.data;

          commitPipelineModel.pipelineDefinition = JSON.stringify(pipeline.pipelineConfig);
          commitPipelineModel.libraryDefinitions = JSON.stringify(pipeline.libraryDefinitions);
          commitPipelineModel.rulesDefinition = JSON.stringify(pipeline.pipelineRules);

          return $http({
            method: 'PUT',
            url: remoteURL,
            data: commitPipelineModel,
            useXDomain: true,
            withCredentials : false,
            headers:  {
              'Content-Type': 'application/json; charset=utf-8',
              'X-SS-User-Auth-Token': ssoToken
            }
          });
        }).then(function(result) {
          var remoteStorePipeline = result.data;
          var pipelineDefinition = JSON.parse(remoteStorePipeline.pipelineDefinition);
          var rulesDefinition = JSON.parse(remoteStorePipeline.currentRules.rulesDefinition);
          newMetadata = pipelineDefinition.metadata;
          newMetadata['lastConfigId'] = pipelineDefinition.uuid;
          newMetadata['lastRulesId'] = rulesDefinition.uuid;
          return $q.all([
            api.pipelineAgent.savePipelineMetadata(name, newMetadata)
          ]);
        }).then(function(res) {
          deferred.resolve(newMetadata);
        }, function(err) {
          deferred.reject(err);
        });
        return deferred.promise;
      },

      fetchPipelines: function(remoteBaseURL, ssoToken) {
        var remoteURL = remoteBaseURL + 'pipelinestore/rest/v1/pipelines';
        return $http({
          method: 'GET',
          url: remoteURL,
          headers:  {
            'Content-Type': 'application/json; charset=utf-8',
            'X-SS-User-Auth-Token': ssoToken
          }
        });
      },

      getPipeline: function(remoteBaseURL, ssoToken, remotePipeline) {
        var remoteURL = remoteBaseURL + 'pipelinestore/rest/v1/pipelineCommit/' + remotePipeline.commitId;
        return $http({
          method: 'GET',
          url: remoteURL,
          headers:  {
            'Content-Type': 'application/json; charset=utf-8',
            'X-SS-User-Auth-Token': ssoToken
          }
        });
      },

      getPipelineCommitHistory: function(remoteBaseURL, ssoToken, pipelineId, offset, len, order) {
        if (offset === undefined) {
          offset = 0;
        }
        if (len === undefined) {
          len = -1;
        }
        if (order === undefined) {
          order = 'DESC';
        }
        var remoteURL = remoteBaseURL + 'pipelinestore/rest/v1/pipeline/' + pipelineId + '/log?' +
          'offset=' + offset +
          '&len=' + len +
          '&order=' + order;

        return $http({
          method: 'GET',
          url: remoteURL,
          headers:  {
            'Content-Type': 'application/json; charset=utf-8',
            'X-SS-User-Auth-Token': ssoToken
          }
        });
      },

      getRemoteRoles: function(remoteBaseURL, ssoToken) {
        var remoteURL = remoteBaseURL + 'security/rest/v1/currentUser';
        return $http({
          method: 'GET',
          url: remoteURL,
          headers:  {
            'Content-Type': 'application/json; charset=utf-8',
            'X-SS-User-Auth-Token': ssoToken
          }
        });
      },

      generateApplicationToken: function(remoteBaseURL, ssoToken, orgId) {
        var newComponentsModel = {
          organization: orgId,
          componentType: 'dc',
          numberOfComponents: 1,
          active: true
        };
        var remoteURL = remoteBaseURL + 'security/rest/v1/organization/' + orgId + '/components';
        return $http({
          method: 'PUT',
          url: remoteURL,
          data: newComponentsModel,
          headers:  {
            'Content-Type': 'application/json; charset=utf-8',
            'X-SS-User-Auth-Token': ssoToken
          }
        });
      },

      getRemoteUsers: function(remoteBaseURL, ssoToken, orgId, offset, len, orderBy, order, active, filterText) {
        if (offset === undefined) {
          offset = 0;
        }
        if (len === undefined) {
          len = -1;
        }
        var url = remoteBaseURL + 'security/rest/v1/organization/' + orgId + '/users?offset=' + offset + '&len=' + len;
        if (orderBy) {
          url += '&orderBy=' + orderBy;
        }
        if (order) {
          url += '&order=' + order;
        }
        if (active !== undefined) {
          url += '&active=' + active;
        }
        if (filterText && filterText.trim().length) {
          url += '&filterText=' + filterText;
        }
        return $http({
          method: 'GET',
          url: url,
          headers:  {
            'Content-Type': 'application/json; charset=utf-8',
            'X-SS-User-Auth-Token': ssoToken
          }
        });
      },

      getRemoteGroups: function(remoteBaseURL, ssoToken, orgId, offset, len, orderBy, order, filterText) {
        if (offset === undefined) {
          offset = 0;
        }
        if (len === undefined) {
          len = -1;
        }
        var url = remoteBaseURL + 'security/rest/v1/organization/' + orgId + '/groups?offset=' + offset + '&len=' + len;
        if (orderBy) {
          url += '&orderBy=' + orderBy;
        }
        if (order) {
          url += '&order=' + order;
        }
        if (filterText && filterText.trim().length) {
          url += '&filterText=' + filterText;
        }
        return $http({
          method: 'GET',
          url: url,
          headers:  {
            'Content-Type': 'application/json; charset=utf-8',
            'X-SS-User-Auth-Token': ssoToken
          }
        });
      }
    };

    api.acl = {
      /**
       * Fetches Pipeline ACL Information
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfigAcl: function(name) {
        var url = apiBase + '/acl/' + name;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Sends updated Pipeline ACL to server for update.
       *
       * @param name - Pipeline Name
       * @param acl - Modified ACL
       * @returns Updated ACL
       */
      savePipelineAcl: function(name, acl) {
        var url = apiBase + '/acl/' + name;
        return $http({
          method: 'POST',
          url: url,
          data: acl
        });
      },

      /**
       * Fetch the Pipeline Permissions for current user
       *
       * @returns {*}
       */
      getPipelinePermissions: function(pipelineName) {
        var url = apiBase + '/acl/' + pipelineName + '/permissions';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get all Subjects in Pipeline ACL
       *
       * @returns {*}
       */
      getSubjects: function () {
        var url = apiBase + '/acl/pipelines/subjects';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Update Subjects in Pipeline ACL
       *
       * @returns {*}
       */
      updateSubjects: function (subjectMapping) {
        var url = apiBase + '/acl/pipelines/subjects';
        return $http({
          method: 'POST',
          url: url,
          data: subjectMapping
        });
      }
    };

    api.system = {
      /**
       * Get all support bundle generators
       *
       * @returns {*}
       */
      getSupportBundleGenerators: function () {
        var url = apiBase + '/system/bundle/list';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get plain/text URL to download generated bundle file
       *
       * @returns {*}
       */
      getGenerateSupportBundleUrl: function (generators) {
        var url = apiBase + '/system/bundle/generate?generators=';
        return url + generators.join(',');
      },

      /**
       * Upload support bundle to StreamSets
       *
       * @returns {*}
       */
      uploadSupportBundle: function (generators) {
        var url = apiBase + '/system/bundle/upload?generators=';
        return $http({
          method: 'GET',
          url: url + generators.join(',')
        });
      }
    };

    api.activation = {
      /**
       * Returns SDC activation information
       *
       * @returns {*}
       */
      getActivation: function () {
        var url = apiBase + '/activation';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Uploads the SDC activation key
       *
       * @returns {*}
       */
      updateActivation: function (activationKey) {
        var url = apiBase + '/activation';
        return $http({
          method: 'POST',
          url: url,
          data: activationKey,
          headers:  {
            'Content-Type': 'text/plain'
          }
        });
      }
    };

    return api;
  });
