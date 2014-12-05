/**
 * Service for providing access to the UI configuration from dist/src/main/etc/pipeline.properties.
 */
angular.module('pipelineAgentApp.common')
  .service('configuration', function($rootScope, api, $q) {
    var self = this,
      REFRESH_INTERVAL = 'ui.refresh.interval.ms';

    this.initializeDefer = undefined;
    this.uiConfig = undefined;

    this.init = function() {
      if(!self.initializeDefer) {
        self.initializeDefer = $q.defer();
        api.pipelineAgent.getConfiguration().then(function(res) {
          self.uiConfig = res.data;
          self.initializeDefer.resolve(self.uiConfig);
        });
      }

      return self.initializeDefer.promise;
    };


    /**
     * Returns refresh interval in milliseconds
     *
     * @returns Long
     */
    this.getRefreshInterval = function() {
      if(self.uiConfig) {
        return self.uiConfig[REFRESH_INTERVAL];
      }
      return 2000;
    };

  });