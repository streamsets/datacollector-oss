/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
    var self = this,
      UI_HEADER_TITLE = 'ui.header.title',
      REFRESH_INTERVAL = 'ui.refresh.interval.ms',
      JVM_METRICS_REFRESH_INTERVAL = 'ui.jvmMetrics.refresh.interval.ms',
      UI_LOCAL_HELP_BASE_URL = 'ui.local.help.base.url',
      UI_HOSTED_HELP_BASE_URL = 'ui.hosted.help.base.url',
      UI_ENABLE_USAGE_DATA_COLLECTION = 'ui.enable.usage.data.collection',
      UI_ENABLE_WEB_SOCKET = 'ui.enable.webSocket',
      HTTP_AUTHENTICATION = 'http.authentication',
      PIPELINE_EXECUTION_MODE = 'pipeline.execution.mode',
      CALLBACK_SERVER_URL = 'callback.server.url',
      UI_UNDO_LIMIT = 'ui.undo.limit',
      METRICS_TIME_SERIES_ENABLE = 'metrics.timeSeries.enable',
      MONITOR_MEMORY = 'monitor.memory',
      DPM_ENABLED = 'dpm.enabled',
      DPM_BASE_URL = 'dpm.base.url';

    this.initializeDefer = undefined;
    this.config = undefined;

    this.init = function() {
      if (!self.initializeDefer) {
        self.initializeDefer = $q.defer();
        api.pipelineAgent.getConfiguration().then(function(res) {
          self.config = res.data;
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
  });
