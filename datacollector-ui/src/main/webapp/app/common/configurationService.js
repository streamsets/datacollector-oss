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
  .service('configuration', function(api, $q, Analytics, tracking) {
    var self = this;
    var UI_HEADER_TITLE = 'ui.header.title';
    var REFRESH_INTERVAL = 'ui.refresh.interval.ms';
    var JVM_METRICS_REFRESH_INTERVAL = 'ui.jvmMetrics.refresh.interval.ms';
    var UI_LOCAL_HELP_BASE_URL = 'ui.local.help.base.url';
    var UI_HOSTED_HELP_BASE_URL = 'ui.hosted.help.base.url';
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
    var UI_DEFAULT_CONFIGURATION_VIEW = 'ui.default.configuration.view';
    var UI_REGISTRATION_URL = 'ui.registration.url';
    var UI_ACCOUNT_REGISTRATION_URL = 'aster.url';

    this.initializeDefer = undefined;
    this.config = undefined;

    function createAndAppendScript(content) {
      var s = window.document.createElement('script');
      s.type = 'text/javascript';
      s.innerHTML = content;
      window.document.getElementsByTagName('head')[0].appendChild(s);
    }

    this.init = function() {
      if (!self.initializeDefer) {
        self.initializeDefer = $q.defer();
        $q.all([
          api.pipelineAgent.getConfiguration(),
          api.pipelineAgent.getUIConfiguration(),
          api.system.getStats()
        ]).then(function(results) {
          self.config = results[0].data;
          angular.forEach(results[1].data, function(value, key) {
            self.config[key] = value;
          });
          self.stats = results[2].data;
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
     * Returns stats.active flag value
     * @returns {*}
     */
    this.isAnalyticsEnabled = function() {
      if (self.stats) {
        return self.stats.active;
      }
      return false;
    };

    /**
     * Set stats.active flag value
     * @param {boolean} enabled
     */
    this.setAnalyticsEnabled = function(enabled) {
      if (self.stats) {
        self.stats.active = enabled;
        if (enabled) {
          // This may throw a warning to the console if it is already set
          Analytics.createAnalyticsScriptTag();
          this.createFullStoryScriptTag();
          tracking.mixpanel.opt_in_tracking();
        } else {
          tracking.mixpanel.opt_out_tracking();
          tracking.FS.shutdown();
        }
      }
    };

    this.createFullStoryScriptTag = function() {
      // Only use FullStory on localhost
      if (window.location.host === 'localhost:18630') {
        var fullStoryScript = "window['_fs_debug'] = false; window['_fs_host'] = 'fullstory.com'; window['_fs_script'] = 'edge.fullstory.com/s/fs.js'; window['_fs_org'] = 'TPR36'; window['_fs_namespace'] = 'FS'; " +
        "(function(m,n,e,t,l,o,g,y){if (e in m) {if(m.console && m.console.log) { m.console.log('FullStory namespace conflict. Please set window[\"_fs_namespace\"].');} return;} g=m[e]=function(a,b,s){g.q?g.q.push([a,b,s]):g._api(a,b,s);};g.q=[]; o=n.createElement(t);o.async=1;o.crossOrigin='anonymous';o.src='https://'+_fs_script; y=n.getElementsByTagName(t)[0];y.parentNode.insertBefore(o,y); g.identify=function(i,v,s){g(l,{uid:i},s);if(v)g(l,v,s)};g.setUserVars=function(v,s){g(l,v,s)};g.event=function(i,v,s){g('event',{n:i,p:v},s)}; g.anonymize=function(){g.identify(!!0)}; g.shutdown=function(){g(\"rec\",!1)};g.restart=function(){g(\"rec\",!0)}; g.log = function(a,b){g(\"log\",[a,b])}; g.consent=function(a){g(\"consent\",!arguments.length||a)}; g.identifyAccount=function(i,v){o='account';v=v||{};v.acctId=i;g(o,v)}; g.clearUserCookie=function(){}; g._w={};y='XMLHttpRequest';g._w[y]=m[y];y='fetch';g._w[y]=m[y]; if(m[y])m[y]=function(){return g._w[y].apply(this,arguments)}; g._v=\"1.2.0\"; })(window,document,window['_fs_namespace'],'script','user');";
        createAndAppendScript(fullStoryScript);
      }
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

    /*
     * Returns ui.default.configuration.view converted to a boolean
     * @returns {Boolean}
     */
    this.defaultShowAdvancedConfigs = function() {
      if (self.config && self.config[UI_DEFAULT_CONFIGURATION_VIEW] !== undefined) {
        return self.config[UI_DEFAULT_CONFIGURATION_VIEW] === 'ADVANCED';
      }
      return false;
    };

    /**
     * Gets the URL for product registration, ui.regisration.url
     */
    this.getRegistrationURL = function() {
      if (self.config && self.config[UI_REGISTRATION_URL] !== undefined) {
        return self.config[UI_REGISTRATION_URL];
      }
      return 'https://registration.streamsets.com/register';
    };

    /**
     * Gets the URL for product registration with an account, ui.account.regisration.url
     */
    this.getAccountRegistrationURL = function() {
      if (self.config && self.config[UI_ACCOUNT_REGISTRATION_URL] !== undefined) {
        return self.config[UI_ACCOUNT_REGISTRATION_URL];
      }
      return '';
    };
  });
