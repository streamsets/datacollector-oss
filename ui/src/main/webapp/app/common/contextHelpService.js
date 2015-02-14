/**
 * Service for launching Contextual Help.
 */
angular.module('dataCollectorApp.common')
  .service('contextHelpService', function($rootScope, $q, configuration, api, pipelineConstant) {

    var helpIds = {},
      helpWindow;

    this.configInitPromise = $q.all([api.admin.getHelpRef(), configuration.init()]).then(function(results) {
      helpIds = results[0].data;
    });

    this.launchHelp = function(helpId) {
      this.configInitPromise.then(function() {
        var uiHelpBaseURL, helpURL,
          relativeURL = helpIds[helpId];

        if ($rootScope.$storage.helpLocation === pipelineConstant.HOSTED_HELP) {
          uiHelpBaseURL = configuration.getUIHostedHelpBaseURL();
        } else {
          uiHelpBaseURL = configuration.getUILocalHelpBaseURL();
        }

        helpURL = uiHelpBaseURL + '/' + (relativeURL || 'index.html');

        if(typeof(helpWindow) == 'undefined' || helpWindow.closed) {
          helpWindow = window.open(helpURL);
        } else {
          helpWindow.location.href = helpURL;
          helpWindow.focus();
        }

      });
    };

    this.launchHelpContents = function() {
      this.configInitPromise.then(function() {
        var uiHelpBaseURL, helpURL;
        if ($rootScope.$storage.helpLocation === pipelineConstant.HOSTED_HELP) {
          uiHelpBaseURL = configuration.getUIHostedHelpBaseURL();
        } else {
          uiHelpBaseURL = configuration.getUILocalHelpBaseURL();
        }

        helpURL = uiHelpBaseURL + '/index.html';

        if(typeof(helpWindow) == 'undefined' || helpWindow.closed) {
          helpWindow = window.open(helpURL);
        } else {
          helpWindow.location.href = helpURL;
          helpWindow.focus();
        }

      });
    };

  });