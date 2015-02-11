/**
 * Service for providing access to the Configuration from dist/src/main/etc/pipeline.properties.
 */
angular.module('dataCollectorApp.common')
  .service('configuration', function($rootScope, api, $q) {
    var self = this,
      REFRESH_INTERVAL = 'ui.refresh.interval.ms',
      JVM_METRICS_REFRESH_INTERVAL = 'ui.jvmMetrics.refresh.interval.ms',
      UI_LOCAL_HELP_BASE_URL = 'ui.local.help.base.url',
      UI_ONLINE_HELP_BASE_URL = 'ui.online.help.base.url';

    this.initializeDefer = undefined;
    this.config = undefined;

    this.init = function() {
      if(!self.initializeDefer) {
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
      if(self.config) {
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
      if(self.config) {
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
      if(self.config) {
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
      if(self.config) {
        return self.config[UI_LOCAL_HELP_BASE_URL];
      }
      return '/docs';
    };

    /**
     * Returns UI Local Help Base URL
     *
     * @returns string
     */
    this.getUIOnlineHelpBaseURL = function() {
      if(self.config) {
        return self.config[UI_ONLINE_HELP_BASE_URL];
      }
      return '/docs';
    };

  });