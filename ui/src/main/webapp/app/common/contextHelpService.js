/**
 * Service for providing access to the Configuration from dist/src/main/etc/pipeline.properties.
 */
angular.module('dataCollectorApp.common')
  .service('contextHelpService', function($q, configuration, api) {

    var helpIds = {};

    this.configInitPromise = $q.all([api.admin.getHelpRef(), configuration.init()]).then(function(results) {
      helpIds = results[0].data;
    });

    this.launchHelp = function(helpId) {
      this.configInitPromise.then(function() {
        var relativeURL = helpIds[helpId],
          uiHelpBaseURL = configuration.getUILocalHelpBaseURL(),
          helpURL = uiHelpBaseURL + '/' + (relativeURL || 'index.html');
        window.open(helpURL);
      });
    };

  });