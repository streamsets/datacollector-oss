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
 * Service for providing access to the backend API via HTTP.
 */

angular.module('dataCollectorApp.common')
  .factory('api', function($rootScope, $http, $q, tracking, trackingEvent) {
    var apiVersion = 'v1';
    var apiBase = 'rest/' + apiVersion;
    var api = {
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

        return $http.get(url);
      },

      /**
       * Fetch list of Log file names
       *
       * @returns {*}
       */
      getFilesList: function() {return $http.get(apiBase + '/system/logs/files');},

      /**
       * Get Log Config
       * @param def
       * @returns {*}
       */
      getLogConfig: function(def) {return $http.get(apiBase + '/system/log/config?default=' + def);},

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
      getJMX: function() {return $http.get(apiBase + '/system/jmx');},

      /**
       * Fetches JVM Thread Dump
       */
      getThreadDump: function() {return $http.get(apiBase + '/system/threads');},

      /**
       * Fetches User Information
       */
      getUserInfo: function() {return $http.get(apiBase + '/system/info/currentUser');},

      /**
       * Fetches Build Information
       */
      getBuildInfo: function() {return $http.get(apiBase + '/system/info');},

      /**
       * Fetches Remote Server Info
       */
      getRemoteServerInfo: function() {return $http.get(apiBase + '/system/info/remote');},

      /**
       * Fetches SDC ID
       */
      getSdcId: function() {return $http.get(apiBase + '/system/info/id');},

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
       * Enable Control Hub
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
       * Disable Control Hub
       */
      disableDPM: function() {
        var url = apiBase + '/system/disableDPM';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Create Control Hub Groups & Users
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
        tracking.mixpanel.track(trackingEvent.LOGOUT, {
          //'Time to First Pipeline': '',
          'Logout Type': 'User Logout',
        });
        if (isDPMEnabled) {
          return $http({
            method: 'GET',
            url: 'logout'
          });
        } else {
          return $http({
            method: 'POST',
            url: apiBase + '/authentication/logout'
          });
        }
      },

      /**
       * Returns SDC Directories
       * @returns {*}
       */
      getSDCDirectories: function() {return $http.get(apiBase + '/system/directories');},

      /**
       * Returns Server Time
       */
      getServerTime: function() {return $http.get(apiBase + '/system/info/serverTime');},

      /**
       * Returns Groups
       * @returns {*}
       */
      getGroups: function() {return $http.get(apiBase + '/system/groups');},

      /* - User list (old API)
        returns [{
          "name": "admin",
          "roles": ["admin"],
          "groups": ["all"]
        },...]
      */
      getUsers: function() {return $http.get(apiBase + '/system/users');},

      /* - User list (new API)
        returns {
          "type" : "PAGINATED_DATA",
          "httpStatusCode" : 200,
          "data" : [ {
            "id" : "admin",
            "email" : "",
            "groups" : ["all"],
            "roles" : ["admin"]
          },...],
          "breadcrumbValue" : null,
          "paginationInfo" : {...},
          "envelopeVersion" : "1"
          }
       */
      getUsers2: function() {return $http.get(apiBase + '/usermanagement/users');},

      deleteUser: function(id) {return $http.delete(apiBase + '/usermanagement/users/' + id);},

      updateUser: function(user) {return $http.post(apiBase + '/usermanagement/users/' + user.id, {
        data: user,
        envelopeVersion: "1"
      });},

      insertUser: function(user) {return $http.post(apiBase + '/usermanagement/users/', {
        data: user,
        envelopeVersion: "1"
      });},

      changeUserPassword: function(id, oldPwd, newPwd) {
        return $http.post(
          apiBase + '/usermanagement/users/' + id + '/changePassword',
          {
            data: {
              id: id,
              oldPassword: oldPwd,
              newPassword: newPwd
            },
            envelopeVersion: "1"
          }
        );
      },

      resetUserPassword: function(userId) {
        return $http.post(apiBase + '/usermanagement/users/' + userId + '/resetPassword', {
          data: null,
          envelopeVersion: "1"
        });
      },

      getAsterRegistrationInfo: function() {
        return $http.get(apiBase + '/aregistration');
      },
    };

    api.pipelineAgent = {
      /**
       * Fetches Configuration from dist/src/main/etc/pipeline.properties
       *
       * @returns {*}
       */
      getConfiguration: function() {return $http.get(apiBase + '/system/configuration');},

      /**
       * Fetches UI Configuration
       *
       * @returns {*}
       */
      getUIConfiguration: function() {return $http.get(apiBase + '/system/configuration/ui');},

      /**
       * Fetches all configuration definitions of Pipeline and Stage Configuration.
       *
       * @returns {*}
       */
      getDefinitions: function() {return $http.get(apiBase + '/definitions');},

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
        return $http.get(url);
      },

      /**
       * Install library from archives/nightly
       *
       * @returns {*}
       */
      installLibraries: function(libraryUrlList, withStageLibVersion) {
        var url = apiBase + '/stageLibraries/install';
        return $http({
          method: 'POST',
          url: url,
          data: libraryUrlList,
          params: {
            withStageLibVersion: !!withStageLibVersion
          }
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
      getStageLibrariesExtras: function(libraryId) {
        var url = apiBase + '/stageLibraries/extras/list';
        return $http({
          method: 'GET',
          url: url,
          params: {
            libraryId: libraryId ? libraryId : ''
          }
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
      getPipelinesCount: function() {return $http.get(apiBase + '/pipelines/count');},

      /**
       * Returns System Pipeline Labels.
       *
       * @returns {*}
       */
      getSystemPipelineLabels: function() {return $http.get(apiBase + '/pipelines/systemLabels');},

      /**
       * Returns all Pipeline labels.
       *
       * @returns {*}
       */
      getPipelineLabels: function() {return $http.get(apiBase + '/pipelines/labels');},

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

        return $http.get(url);
      },

      /**
       * Fetches Pipeline Configuration.
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfig: function(name) {return $http.get(apiBase + '/pipeline/' + name);},


      getSamplePipeline: function(pipelineId) {
        return $http.get(apiBase + '/pipeline/' + pipelineId + '?get=samplePipeline');
      },

      /**
       * Fetches Pipeline Configuration Information
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfigInfo: function(name) {return $http.get(apiBase + '/pipeline/' + name + '?get=info');},

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
       * @param pipelineType
       */
      createNewPipelineConfig: function(name, description, pipelineType, pipelineLabel) {
        var url = apiBase + '/pipeline/' + encodeURIComponent(name);
        return $http({
          method: 'PUT',
          url: url,
          params: {
            autoGeneratePipelineId: true,
            description: description,
            pipelineType: pipelineType,
            pipelineLabel: pipelineLabel
          }
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
            duplicatePipelineObject.testOriginStage = pipelineObject.testOriginStage;
            duplicatePipelineObject.fragments = pipelineObject.fragments;
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
          .then(function() {
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
       * @param includePlainTextCredentials
       */
      exportPipelineConfig: function(name, includeLibraryDefinitions, includePlainTextCredentials) {
        var url = apiBase + '/pipeline/' + name + '/export?attachment=true&includePlainTextCredentials=' +
          !!includePlainTextCredentials;
        if (includeLibraryDefinitions) {
          url += '&includeLibraryDefinitions=true';
        }
        window.open(url, '_blank', '');
        if (!includePlainTextCredentials) {
          $rootScope.common.infoList = [{
            message: 'Exporting the pipeline stripped of all plain text credentials. ' +
              'To include credentials in the export, use Export with Plain Text Credentials.'
          }];
        }
        tracking.mixpanel.track(trackingEvent.PIPELINE_EXPORT, {'Pipeline ID': name});
      },

      /**
       * Export Pipelines.
       *
       * @param pipelineIds
       * @param includeLibraryDefinitions
       * @param includePlainTextCredentials
       */
      exportSelectedPipelines: function(pipelineIds, includeLibraryDefinitions, includePlainTextCredentials) {

        var url = apiBase + '/pipelines/export?includePlainTextCredentials=' + !!includePlainTextCredentials;
        if (includeLibraryDefinitions) {
          url += '&includeLibraryDefinitions=true';
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
        if (!includePlainTextCredentials) {
          $rootScope.common.infoList = [{
            message: 'Exporting the pipeline stripped of all plain text credentials. ' +
              'To include credentials in the export, use Export with Plain Text Credentials.'
          }];
        }
        tracking.mixpanel.track(trackingEvent.PIPELINE_EXPORT_BULK, {'Pipeline IDs': pipelineIds});
      },

      /**
       * Import Pipeline Configuration.
       *
       * @param pipelineName
       * @param pipelineEnvelope
       * @param overwrite
       * @param autoGeneratePipelineId
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

      importPipelineFromUrl: function(pipelineTitle, pipelineHttpUrl) {
        var url = apiBase + '/pipeline/' + pipelineTitle + '/importFromURL';
        return $http({
          method: 'POST',
          url: url,
          params: {
            pipelineHttpUrl: pipelineHttpUrl,
            autoGeneratePipelineId: true
          }
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
       * Download Sample Edge pipelines
       * @param edgeHttpUrl
       */
      downloadPipelinesFromEdge: function(edgeHttpUrl) {
        var url = apiBase + '/pipelines/downloadFromEdge' ;
        return $http({
          method: 'POST',
          url: url,
          data: edgeHttpUrl
        });
      },

      /**
       * Publish Pipelines to Data Collector Edge
       *
       * @param pipelineIds
       * @param edgeHttpUrl
       * @returns {*}
       */
      publishPipelinesToEdge: function(pipelineIds, edgeHttpUrl) {
        var url = apiBase + '/pipelines/publishToEdge' ;
        return $http({
          method: 'POST',
          url: url,
          data: {
            pipelineIds: pipelineIds,
            edgeHttpUrl: edgeHttpUrl
          }
        });
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
       * @param edgeHttpUrl
       * @param testOrigin
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
        timeout,
        edgeHttpUrl,
        testOrigin
      ) {
        if (!batchSize) {
          batchSize = 10;
        }
        if (!timeout || timeout <=0) {
          timeout = 30000;
        }
        var url = apiBase + '/pipeline/' + name + '/preview';
        return $http({
          method: 'POST',
          url: url,
          params: {
            batchSize: batchSize,
            rev: rev,
            skipTargets: skipTargets,
            timeout: timeout,
            skipLifecycleEvents: skipLifecycleEvents,
            endStage: endStage,
            edge: !!edgeHttpUrl,
            testOrigin: !!testOrigin
          },
          data: stageOutputList || []
        });
      },


      /**
       * Fetches Preview Status
       *
       * @param pipelineId
       * @param previewerId
       * @param edgeHttpUrl
       */
      getPreviewStatus: function(pipelineId, previewerId, edgeHttpUrl) {
        var url = apiBase + '/pipeline/' + pipelineId + '/preview/' + previewerId + '/status' ;
        return $http({
          method: 'GET',
          url: url,
          params: {
            edge: !!edgeHttpUrl
          }
        });
      },

      /**
       * Fetches Preview Data
       *
       * @param pipelineId
       * @param previewerId
       * @param edgeHttpUrl
       */
      getPreviewData: function(pipelineId, previewerId, edgeHttpUrl) {
        var url = apiBase + '/pipeline/' + pipelineId + '/preview/' + previewerId;
        return $http({
          method: 'GET',
          url: url,
          params: {
            edge: !!edgeHttpUrl
          }
        });
      },

      /**
       * Stop Preview
       *
       * @param pipelineId
       * @param previewerId
       * @param edgeHttpUrl
       */
      cancelPreview: function(pipelineId, previewerId, edgeHttpUrl) {
        tracking.mixpanel.track(trackingEvent.PREVIEW_CANCELLED, {
          'Pipeline ID': pipelineId,
          'Stop Type': 'User',
        });
        var url = apiBase + '/pipeline/' + pipelineId + '/preview/' + previewerId;
        return $http({
          method: 'DELETE',
          url: url,
          params: {
            edge: !!edgeHttpUrl
          }
        });
      },

      /**
       * Fetch all Pipeline Status
       *
       * @returns {*}
       */
      getAllPipelineStatus: function() {return $http.get(apiBase + '/pipelines/status');},

      /**
       * Fetch the Pipeline Status
       *
       * @returns {*}
       */
      getPipelineStatus: function(pipelineName, rev) {return $http.get(apiBase + '/pipeline/' + pipelineName + '/status?rev=' + rev);},

      /**
       * Validate the Pipeline
       *
       * @param pipelineId
       * @param edgeHttpUrl
       * @returns {*}
       */
      validatePipeline: function(pipelineId, edgeHttpUrl) {
        var url = apiBase + '/pipeline/' + pipelineId + '/validate';
        return $http({
          method: 'GET',
          url: url,
          params: {
            timeout: 500000,
            edge: !!edgeHttpUrl
          }
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
       * @param pipelineIds
       * @param forceStop
       * @returns {*}
       */
      stopPipelines: function(pipelineIds, forceStop) {
        var url = apiBase + '/pipelines/stop';
        if (forceStop) {
          url = apiBase + '/pipelines/forceStop';
        }
        return $http({
          method: 'POST',
          url: url,
          data: pipelineIds
        });
      },

      /**
       * Fetch the Pipeline Metrics
       *
       * @returns {*}
       */
      getPipelineMetrics: function(pipelineId, rev) {return $http.get(apiBase + '/pipeline/' + pipelineId + '/metrics?rev=' + rev);},

      /**
       * Get List of available snapshots.
       *
       * @returns {*}
       */
      getSnapshotsInfo: function() {return $http.get(apiBase + '/pipelines/snapshots');},

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
      getSnapshotStatus: function(pipelineName, rev, snapshotName) {return $http.get(apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName + '/status?rev=' + rev);},

      /**
       * Get captured snapshot for given pipeline name.
       *
       * @param pipelineName
       * @param rev
       * @param snapshotName
       * @returns {*}
       */
      getSnapshot: function(pipelineName, rev, snapshotName) {return $http.get(apiBase + '/pipeline/' + pipelineName + '/snapshot/' + snapshotName + '?rev=' + rev);},

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
       * @param edge
       * @returns {*}
       */
      getErrorRecords: function(pipelineName, rev, stageInstanceName, edge) {
        var url = apiBase + '/pipeline/' + pipelineName + '/errorRecords';
        return $http({
          method: 'GET',
          url: url,
          params: {
            stageInstanceName: stageInstanceName,
            edge: !!edge
          }
        });
      },

      /**
       * Get error messages for the given stage instance name of running pipeline if is provided otherwise
       * return error messages for the pipeline.
       *
       * @param pipelineName
       * @param rev
       * @param stageInstanceName
       * @param edge
       * @returns {*}
       */
      getErrorMessages: function(pipelineName, rev, stageInstanceName, edge) {
        var url = apiBase + '/pipeline/' + pipelineName + '/errorMessages';
        return $http({
          method: 'GET',
          url: url,
          params: {
            stageInstanceName: stageInstanceName,
            edge: !!edge
          }
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

        return $http.get(url);
      },

      /**
       * Get history of the pipeline
       *
       * @param name
       * @returns {*}
       */
      getHistory: function(name) {return $http.get(apiBase + '/pipeline/' + name + '/history');},

      /**
       * Clear history of the pipeline
       *
       * @param name
       * @returns {*}
       */
      clearHistory: function(name) {
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
        return $http.get(apiBase + '/pipeline/' + name + '/rules');
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
        return $http.get(apiBase + '/pipeline/' + pipelineName + '/sampledRecords?sampleId=' + samplingRuleId +
          '&sampleSize=' + sampleSize);
      },

      /**
       * Get all pipeline alers
       */
      getAllAlerts: function() {return $http.get(apiBase + '/pipelines/alerts');},

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
      },

      /**
       * Fetch list of static stage icons
       *
       * @returns {*}
       */
      getStageAssetIcons: function() {
        return $http.get('assets/stage/stageIcons.json');
      }
    };

    api.controlHub = {
      publishPipeline: function(pipelineId, commitMessage) {
        var url = apiBase + '/controlHub/publishPipeline/' + pipelineId;
        return $http({
          method: 'POST',
          url: url,
          params: {
            commitMessage: commitMessage
          }
        });
      },

      fetchPipelines: function() {
        var url = apiBase + '/controlHub/pipelines';
        return $http({
          method: 'GET',
          url: url,
          params: {
            executionModes: 'STANDALONE,CLUSTER_BATCH,CLUSTER_YARN_STREAMING,CLUSTER_MESOS_STREAMING,EDGE,EMR_BATCH'
          }
        });
      },

      getPipeline: function(remotePipeline) {
        return $http.get(apiBase + '/controlHub/pipeline/' + remotePipeline.commitId);
      },

      getPipelineCommitHistory: function(pipelineId, offset, len, order) {
        if (offset === undefined) {
          offset = 0;
        }
        if (len === undefined) {
          len = -1;
        }
        if (order === undefined) {
          order = 'DESC';
        }
        var url = apiBase + '/controlHub/pipeline/' + pipelineId + '/log';
        return $http({
          method: 'GET',
          url: url,
          params: {
            offset: offset,
            len: len,
            order: order
          }
        });
      },

      getRemoteRoles: function() {return $http.get(apiBase + '/controlHub/currentUser');},

      getRemoteUsers: function(offset, len) {
        if (offset === undefined) {
          offset = 0;
        }
        if (len === undefined) {
          len = -1;
        }
        var url = apiBase + '/controlHub/users';
        return $http({
          method: 'GET',
          url: url,
          params:  {
            offset: offset,
            len: len
          }
        });
      },

      getRemoteGroups: function(offset, len) {
        if (offset === undefined) {
          offset = 0;
        }
        if (len === undefined) {
          len = -1;
        }
        var url = apiBase + '/controlHub/groups';
        return $http({
          method: 'GET',
          url: url,
          params:   {
            offset: offset,
            len: len
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
      getPipelineConfigAcl: function(name) {return $http.get(apiBase + '/acl/' + name);},

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
      getPipelinePermissions: function(pipelineName) {return $http.get(apiBase + '/acl/' + pipelineName + '/permissions');},

      /**
       * Get all Subjects in Pipeline ACL
       *
       * @returns {*}
       */
      getSubjects: function() {return $http.get(apiBase + '/acl/pipelines/subjects');},

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
       * Get stats and opt in/out status
       *
       * @returns {*}
       */
      getStats: function() {return $http.get(apiBase + '/system/stats');},

      /**
       * Set opt in/out status for stats
       *
       * @returns {*}
       */
      setOptInStatus: function(isOptIn) {return $http({
          method: 'POST',
          url: apiBase + '/system/stats?active=' + (!!isOptIn)
      });},

      /**
       * Get all support bundle generators
       *
       * @returns {*}
       */
      getSupportBundleGenerators: function() {return $http.get(apiBase + '/system/bundle/list');},

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
      uploadSupportBundle: function(generators) {
        return $http.get(apiBase + '/system/bundle/upload?generators=' + generators.join(','));
      },

      /**
       * Returns list of available health inspectors.
       *
       * @returns {*}
       */
      getHealthCheckCategories: function() {
        return $http.get(apiBase + '/system/health/categories');
      },

      /**
       * Get Health Inspector Report.
       *
       * @returns {*}
       */
      getHealthReport: function(categories) {
        return $http.get(apiBase + '/system/health/report?categories=' + categories);
      },
    };

    api.activation = {
      /**
       * Returns SDC activation information
       *
       * Response shape:
         {
          "info" : {
            "type" : "rsa-signed",
            "firstUse" : 1584580807579,
            "userInfo" : "n/a",
            "sdcId" : "c50a96bb-697f-11ea-8a0c-9dcc684d15db",
            "validSdcIds" : [ "c50a96bb-697f-11ea-8a0c-9dcc684d15db" ],
            "additionalInfo" : { },
            "valid" : false,
            "expiration" : 1584580807579
          },
          "type" : "rsa-signed",
          "enabled" : true
        }
       *
       * @returns {*}
       */
      getActivation: function() {return $http.get(apiBase + '/activation');},

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

    api.secret = {
      /**
       * Create new text secret (aka password), return http promise
       */
      createOrUpdateTextSecret: function (vaultName, secretName, secretValue) {
        var url = apiBase + '/secrets/text/ctx=SecretManage';
        return $http({
          method: 'POST',
          url: url,
          data: {
            envelopeVersion : '1',
            data: {
              vault: vaultName,
              name: secretName,
              type: 'TEXT',
              value: secretValue,
            }
          },
          headers: {
            'Content-Type': 'application/json'
          }
        });
      },

      /**
       * Create new file secret, return http promise
       */
      createOrUpdateFileSecret: function (vaultName, secretName, secretFile) {
        var url = apiBase + '/secrets/file/ctx=SecretManage';
        var formData = new FormData();
        formData.append('vault', vaultName);
        formData.append('name', secretName);
        formData.append('uploadedFile', secretFile);
        return $http.post(url, formData, {
          // assign content-type as undefined, the browser
          // will assign the correct boundary for us
          headers: { 'Content-Type': undefined},
          // prevents serializing payload
          transformRequest: angular.identity
        });
      },

      /**
       * Returns 200 if available, 404 if not
       */
      checkSecretsAvailability: function() {return $http.get(apiBase + '/secrets/get');},

      /**
       * Gets the default ssh public key
       */
      getSSHPublicKey: function() {return $http.get(apiBase + '/secrets/sshTunnelPublicKey');}
    };

    /**
     * Sends registration to external server
     */
    api.externalRegistration = {
      sendRegistration: function(
        registrationURL, firstName, lastName, companyName, email,
        role, country, postalCode, sdcId, productVersion,
        activationUrl
        ) {
        if (!registrationURL) {
          throw Error('missing registrationURL');
        }
        postalCode = postalCode || '';

        var registrationObject = {
          firstName: firstName,
          lastName: lastName,
          company: companyName,
          email: email,
          role: role,
          country: country,
          postalCode: postalCode,
          id: sdcId,
          type: 'DATA_COLLECTOR',
          version: productVersion,
          activationUrl: activationUrl
        };

        return $http.post(
          registrationURL,
          registrationObject, {
            // CORS requires Content-Type 'text-plain' and no other headers
            headers: {
              'Content-Type': 'text/plain',
              'X-Requested-By': undefined
            }
          }
        );
      }
    };

    return api;
  });
