angular.module('pipelineAgentApp')
  .config(function($routeProvider, $locationProvider, $translateProvider, tmhDynamicLocaleProvider,
                   uiSelectConfig, $httpProvider){
    $locationProvider.html5Mode(true);
    $routeProvider.otherwise({
      redirect: '/'
    });

    // Initialize angular-translate
    $translateProvider.useStaticFilesLoader({
      prefix: 'i18n/',
      suffix: '.json'
    });

    $translateProvider.preferredLanguage('en');

    $translateProvider.useCookieStorage();

    tmhDynamicLocaleProvider.localeLocationPattern('bower_components/angular-i18n/angular-locale_{{locale}}.js');
    tmhDynamicLocaleProvider.useCookieStorage('NG_TRANSLATE_LANG_KEY');

    uiSelectConfig.theme = 'bootstrap';

    //Reload the page when the server is down.
    $httpProvider.interceptors.push(function($q) {
      return {
        responseError: function(rejection) {
          if(rejection.status === 0) {
            window.location.reload();
            return;
          }
          return $q.reject(rejection);
        }
      };
    });

  })
  .run(function ($location, $rootScope, $modal, api) {
    var defaultTitle = 'StreamSets Data Collector';
    $rootScope.common = $rootScope.common || {
      title : defaultTitle,
      active: {
        home: 'active'
      },
      namePattern: '^[a-zA-Z0-9 _]+$',
      saveOperationInProgress: false,
      pipelineStatus: {},
      errors: [],
      activeDetailTab: undefined,
      dontShowHelpAlert: false,

      /**
       * Open the Shutdown Modal Dialog
       */
      shutdownCollector: function() {
        $modal.open({
          templateUrl: 'shutdownModalContent.html',
          controller: 'ShutdownModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      /**
       * Logout header link command handler
       */
      logout: function() {
        api.admin.logout()
          .success(function() {
            location.reload();
          })
          .error(function() {

          });
      },

      /**
       * Open the About Modal Dialog
       */
      showAbout: function() {
        $modal.open({
          templateUrl: 'aboutModalContent.html',
          controller: 'AboutModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      /**
       * Return logs collected from Log WebSocket
       * @returns {string}
       */
      getLogMessages: function() {
        return logMessages.join('\n');
      }
    };

    var logMessages = [],
      loc = window.location,
      webSocketLogURL = ((loc.protocol === "https:") ? "wss://" : "ws://") + loc.hostname + (((loc.port != 80) && (loc.port != 443)) ? ":" + loc.port : "") + '/log/',
      logWebSocket = new WebSocket(webSocketLogURL);


    logWebSocket.onmessage = function (evt) {
      var received_msg = evt.data;
      if(logMessages.length > 1000) {
        logMessages.shift();
      }
      logMessages.push(received_msg);
    };

    $rootScope.$on('$destroy', function() {
      logWebSocket.close();
    });

    // set actions to be taken each time the user navigates
    $rootScope.$on('$routeChangeSuccess', function (event, current, previous) {
      // set page title
      if(current.$$route) {
        $rootScope.common.title = current.$$route.title || defaultTitle;

        // set active menu class for the left navigation (.sidenav)
        var currentCtrl = current.controller.substring(0, current.controller.indexOf('Controller')).toLowerCase();
        $rootScope.common.active[currentCtrl] = 'active';
        if (previous && previous.controller) {
          var previousCtrl = previous.controller.substring(0, previous.controller.indexOf('Controller')).toLowerCase();
          delete $rootScope.common.active[previousCtrl];
        }
      }
    });

    $rootScope.go = function ( path ) {
      $location.path( path );
    };
  });