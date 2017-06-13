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
 * Service for providing access to the Configuration from dist/src/main/etc/pipeline.properties.
 */
angular.module('dataCollectorApp.common')
  .service('configuration', function($rootScope, api, $q) {
    var self = this;
    var UI_HEADER_TITLE = 'ui.header.title';
    var REFRESH_INTERVAL = 'ui.refresh.interval.ms';
    var JVM_METRICS_REFRESH_INTERVAL = 'ui.jvmMetrics.refresh.interval.ms';
    var UI_LOCAL_HELP_BASE_URL = 'ui.local.help.base.url';
    var UI_HOSTED_HELP_BASE_URL = 'ui.hosted.help.base.url';
    var UI_ENABLE_USAGE_DATA_COLLECTION = 'ui.enable.usage.data.collection';
    var UI_ENABLE_WEB_SOCKET = 'ui.enable.webSocket';
    var UI_SERVER_TIMEZONE = 'ui.server.timezone';
    var UI_DEBUG = 'ui.debug';
    var HTTP_AUTHENTICATION = 'http.authentication';
    var PIPELINE_EXECUTION_MODE = 'pipeline.execution.mode';
    var CALLBACK_SERVER_URL = 'callback.server.url';
    var UI_UNDO_LIMIT = 'ui.undo.limit';
    var METRICS_TIME_SERIES_ENABLE = 'metrics.timeSeries.enable';
    var MONITOR_MEMORY = 'monitor.memory';
    var DPM_ENABLED = 'dpm.enabled';
    var DPM_BASE_URL = 'dpm.base.url';
    var CLOUDERA_MANAGER_MANAGED = 'clouderaManager.managed';
    var SUPPORT_BUNDLE_ENABLED = 'bundle.upload.enabled';
    var DPM_LABELS ='dpm.remote.control.job.labels';
    var PIPELINE_ACCESS_CONTROL_ENABLED = 'pipeline.access.control.enabled';

    this.initializeDefer = undefined;
    this.config = undefined;

    this.init = function() {
      if (!self.initializeDefer) {
        self.initializeDefer = $q.defer();
        $q.all([
          api.pipelineAgent.getConfiguration(),
          api.pipelineAgent.getUIConfiguration()
        ]).then(function(results) {
          self.config = results[0].data;
          angular.forEach(results[1].data, function(value, key) {
            self.config[key] = value;
          });
          self.initializeDefer.resolve(self.config);
        });
      }

      return self.initializeDefer.promise;
    };

    /**
     * Returns Configuration Properties
     * @returns {Object}
     */

    this.getConfiguration = function() {
      if (self.config) {
        return self.config;
      }

      return undefined;
    };

    /**
     * Returns refresh interval in milliseconds
     *
     * @returns number
     */
    this.getRefreshInterval = function() {
      if (self.config && self.config[REFRESH_INTERVAL] !== undefined) {
        return self.config[REFRESH_INTERVAL];
      }
      return 2000;
    };

    /**
     * Returns refresh interval in milliseconds
     *
     * @returns number
     */
    this.getJVMMetricsRefreshInterval = function() {
      if (self.config && self.config[JVM_METRICS_REFRESH_INTERVAL] !== undefined) {
        return self.config[JVM_METRICS_REFRESH_INTERVAL];
      }
      return 4000;
    };

    /**
     * Returns UI Local Help Base URL
     *
     * @returns string
     */
    this.getUILocalHelpBaseURL = function() {
      if (self.config && self.config[UI_LOCAL_HELP_BASE_URL]) {
        return self.config[UI_LOCAL_HELP_BASE_URL];
      }
      return '/docs';
    };

    /**
     * Returns UI Local Help Base URL
     *
     * @returns string
     */
    this.getUIHostedHelpBaseURL = function() {
      if (self.config && self.config[UI_HOSTED_HELP_BASE_URL]) {
        return self.config[UI_HOSTED_HELP_BASE_URL];
      }
      return 'https://streamsets.com/documentation/datacollector/latest/help/';
    };

    /**
     * Returns ui.enable.usage.data.collection flag value
     * @returns {*}
     */
    this.isAnalyticsEnabled = function() {
      if (self.config) {
        return self.config[UI_ENABLE_USAGE_DATA_COLLECTION] === 'true';
      }
      return false;
    };

    /**
     * Returns http.authentication value from sdc.properties
     * @returns {*}
     */
    this.getAuthenticationType = function() {
      if (self.config) {
        return self.config[HTTP_AUTHENTICATION];
      }
      return 'form';
    };

    /**
     * Returns true if SDC running in Slave mode otherwise false
     * @returns {*}
     */
    this.isSlaveNode = function() {
      return self.config && self.config[PIPELINE_EXECUTION_MODE] &&
        self.config[PIPELINE_EXECUTION_MODE].toLowerCase() === 'slave';
    };

    /**
     * Returns SDC Cluster Manager URL
     * @returns {*}
     */
    this.getSDCClusterManagerURL = function() {
      if (self.config && self.config[CALLBACK_SERVER_URL]) {
        return self.config[CALLBACK_SERVER_URL].replace('/rest/v1/cluster/callback', '');
      }
      return 'http://localhost:18630';
    };

    /*
     * Returns ui.enable.webSocket flag value
     * @returns {*}
     */
    this.isWebSocketUseEnabled = function() {
      if (self.config && self.config[UI_ENABLE_WEB_SOCKET] !== undefined) {
        return self.config[UI_ENABLE_WEB_SOCKET] === 'true';
      }
      return true;
    };

    /*
     * Returns ui.undo.limit configuration value
     * @returns {*}
     */
    this.getUndoLimit = function() {
      if (self.config && self.config[UI_UNDO_LIMIT] !== undefined) {
        return self.config[UI_UNDO_LIMIT];
      }
      return 10;
    };

    /**
     * Returns Server timezone
     *
     * @returns string
     */
    this.getServerTimezone = function() {
      if (self.config && self.config[UI_SERVER_TIMEZONE]) {
        return self.config[UI_SERVER_TIMEZONE];
      }
      return 'UTC';
    };

    /*
     * Returns ui.debug flag value
     * @returns {*}
     */
    this.isUIDebugEnabled = function() {
      if (self.config && self.config[UI_DEBUG] !== undefined) {
        return self.config[UI_DEBUG] === 'true';
      }
      return false;
    };

    /*
     * Returns metrics.timeSeries.enable flag value
     * @returns {*}
     */
    this.isMetricsTimeSeriesEnabled = function() {
      if (self.config) {
        return self.config[METRICS_TIME_SERIES_ENABLE] === 'true';
      }
      return true;
    };

    /**
     * Returns monitor.memory flag value
     */
    this.isMonitorMemoryEnabled = function() {
      if (self.config) {
        return self.config[MONITOR_MEMORY] === 'true';
      }
      return false;
    };

    /**
     * Returns customizable header title for SDC UI
     * @returns {*}
     */
    this.getUIHeaderTitle = function() {
      if (self.config) {
        return self.config[UI_HEADER_TITLE];
      }
      return '';
    };

    /**
     * Returns http.authentication.sso.service.url config value
     * @returns {*}
     */
    this.getRemoteBaseUrl = function() {
      if (self.config) {
        return self.config[DPM_BASE_URL];
      }
      return '';
    };

    /*
     * Returns dpm.enabled flag value
     * @returns {*}
     */
    this.isDPMEnabled = function() {
      if (self.config && self.config[DPM_ENABLED] !== undefined) {
        return self.config[DPM_ENABLED] === 'true';
      }
      return false;
    };

    /*
     * Returns dpm.remote.control.job.labels configuration value
     * @returns {*}
     */
    this.getDPMLabels = function() {
      if (self.config && self.config[DPM_LABELS] !== undefined && self.config[DPM_LABELS].trim().length) {
        return self.config[DPM_LABELS].split(',');
      }
      return [];
    };

    /*
     * Returns clouderaManager.managed flag value
     * @returns {*}
     */
    this.isManagedByClouderaManager = function() {
      if (self.config && self.config[CLOUDERA_MANAGER_MANAGED] !== undefined) {
        return self.config[CLOUDERA_MANAGER_MANAGED] === 'true';
      }
      return false;
    };

    /*
     * Returns bundle.upload.enabled flag value
     * @returns {*}
     */
    this.isSupportBundleUplodEnabled = function() {
      if (self.config && self.config[SUPPORT_BUNDLE_ENABLED] !== undefined) {
        return self.config[SUPPORT_BUNDLE_ENABLED] === 'true';
      }
      return true;
    };

    /*
     * Returns pipeline.access.control.enabled flag value
     * @returns {*}
     */
    this.isACLEnabled = function() {
      if (self.config && self.config[PIPELINE_ACCESS_CONTROL_ENABLED] !== undefined) {
        return self.config[PIPELINE_ACCESS_CONTROL_ENABLED] === 'true' &&
          (self.config[HTTP_AUTHENTICATION] !== 'none' || this.isDPMEnabled());
      }
      return true;
    };
  });
