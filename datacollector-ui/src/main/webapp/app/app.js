angular.module('dataCollectorApp')
  .config(function($routeProvider, $locationProvider, $translateProvider, tmhDynamicLocaleProvider,
                   uiSelectConfig, $httpProvider, AnalyticsProvider){
    $locationProvider.html5Mode({enabled: true, requireBase: false});
    $routeProvider.otherwise({
      templateUrl: 'app/home/home.tpl.html',
      controller: 'HomeController',
      resolve: {
        myVar: function(authService) {
          return authService.init();
        }
      },
      data: {
        authorizedRoles: ['admin', 'creator', 'manager', 'guest']
      }
    });

    // Initialize angular-translate
    $translateProvider.useStaticFilesLoader({
      prefix: '/i18n/',
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
          console.log(rejection);
          if(rejection.status === 0) {
            window.location.reload();
            return;
          }
          return $q.reject(rejection);
        }
      };
    });

    AnalyticsProvider.setAccount('UA-60917135-1');
    AnalyticsProvider.trackPages(true);
    AnalyticsProvider.trackUrlParams(true);
    AnalyticsProvider.setDomainName('none');
    AnalyticsProvider.useAnalytics(true);
    AnalyticsProvider.delayScriptTag(true);

  })
  .run(function ($location, $rootScope, $modal, api, pipelineConstant, $localStorage, contextHelpService,
                 $translate, authService, userRoles, configuration, Analytics) {
    var defaultTitle = 'StreamSets Data Collector';

    $rootScope.pipelineConstant = pipelineConstant;
    $rootScope.$storage = $localStorage.$default({
      displayDensity: pipelineConstant.DENSITY_COMFORTABLE,
      helpLocation: pipelineConstant.LOCAL_HELP,
      readNotifications: []
    });

    $rootScope.common = $rootScope.common || {
      title : defaultTitle,
      userName: 'Account',
      authenticationType: 'none',
      active: {
        home: 'active'
      },
      namePattern: '^[a-zA-Z0-9 _]+$',
      saveOperationInProgress: 0,
      pipelineStatus: {},
      errors: [],
      infoList: [],
      successList: [],
      activeDetailTab: undefined,
      dontShowHelpAlert: false,
      logEndingOffset: -1,
      fetchingLog: false,
      counters: {},

      /**
       * Open the Shutdown Modal Dialog
       */
      shutdownCollector: function() {
        $modal.open({
          templateUrl: 'common/administration/shutdown/shutdownModal.tpl.html',
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
       * Launch Local or Online Help based on settings.
       *
       */
      launchHelpContents: function() {
        contextHelpService.launchHelpContents();
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
       * Open the Settings Modal Dialog
       */
      showSettings: function() {
        $modal.open({
          templateUrl: 'app/help/settings/settingsModal.tpl.html',
          controller: 'SettingsModalInstanceController',
          size: '',
          backdrop: true
        });
      },

      showSDCDirectories: function() {
        $modal.open({
          templateUrl: 'common/administration/sdcDirectories/sdcDirectoriesModal.tpl.html',
          controller: 'SDCDirectoriesModalInstanceController',
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
      },

      /**
       * Clear Local Storage Contents
       */
      clearLocalStorage: function() {
        $localStorage.$reset();
      }
    };

    var logMessages = [];

    authService.init().then(function() {
      $rootScope.common.userName = authService.getUserName();
      $rootScope.common.userRoles = authService.getUserRoles().join(', ');
      $rootScope.userRoles = userRoles;
      $rootScope.isAuthorized = authService.isAuthorized;
    });

    configuration.init().then(function() {
      $rootScope.common.authenticationType = configuration.getAuthenticationType();
      if(configuration.isAnalyticsEnabled()) {
        Analytics.createAnalyticsScriptTag();
      }
    });

    // set actions to be taken each time the user navigates
    $rootScope.$on('$routeChangeSuccess', function (event, current, previous) {
      // set page title
      if(current.$$route && current.$$route.data) {
        var authorizedRoles = current.$$route.data.authorizedRoles;
        $rootScope.notAuthorized = !authService.isAuthorized(authorizedRoles);
      }

      //To fix NVD3 JS errors - https://github.com/novus/nvd3/pull/396
      window.nv.charts = {};
      window.nv.graphs = [];
      window.nv.logs = {};
      window.onresize = null;
    });

    $rootScope.go = function ( path ) {
      $location.path( path );
    };

    var unloadMessage = 'If you leave this page you are going to lose all unsaved changes, are you sure you want to leave?';

    $translate('global.messages.info.unloadMessage').then(function(translation) {
      unloadMessage = translation;
    });

    window.onbeforeunload = function (event) {
      //Check if there was any change, if no changes, then simply let the user leave
      if($rootScope.common.saveOperationInProgress <= 0){
        return;
      }

      if (typeof event == 'undefined') {
        event = window.event;
      }
      if (event) {
        event.returnValue = unloadMessage;
      }
      return unloadMessage;
    };

  });