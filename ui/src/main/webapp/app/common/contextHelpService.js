/**
 * Service for launching Contextual Help.
 */
angular.module('dataCollectorApp.common')
  .service('contextHelpService', function($rootScope, $q, configuration, api, pipelineConstant) {

    var helpIds = {};

    this.configInitPromise = $q.all([api.admin.getHelpRef(), configuration.init()]).then(function(results) {
      helpIds = results[0].data;
    });

    this.launchHelp = function(helpId) {
      this.configInitPromise.then(function() {
        var uiHelpBaseURL, helpURL,
          relativeURL = helpIds[helpId];

        if ($rootScope.$storage.helpLocation === pipelineConstant.ONLINE_HELP) {
          uiHelpBaseURL = configuration.getUIOnlineHelpBaseURL();
        } else {
          uiHelpBaseURL = configuration.getUILocalHelpBaseURL();
        }

        helpURL = uiHelpBaseURL + '/' + (relativeURL || 'index.html');
        window.open(helpURL);
      });
    };

    this.launchHelpContents = function() {
      this.configInitPromise.then(function() {
        var uiHelpBaseURL, helpURL;
        if ($rootScope.$storage.helpLocation === pipelineConstant.ONLINE_HELP) {
          uiHelpBaseURL = configuration.getUIOnlineHelpBaseURL();
        } else {
          uiHelpBaseURL = configuration.getUILocalHelpBaseURL();
        }

        helpURL = uiHelpBaseURL + '/index.html';
        window.open(helpURL);
      });
    };

  });