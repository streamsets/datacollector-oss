angular.module('pipelineAgentApp', [
  'ngRoute',
  'ngCookies',
  'tmh.dynamicLocale',
  'pascalprecht.translate',
  'templates-app',
  'templates-common',
  'pipelineAgentApp.common',
  'pipelineAgentApp.home'
])
  .config(function($routeProvider, $locationProvider, $translateProvider, tmhDynamicLocaleProvider){
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

  })
  .run(function ($location, $rootScope) {
    var defaultTitle = 'StreamSets | Data In Motion';
    $rootScope.common = $rootScope.common || {
      title : defaultTitle,
      active: {
        home: 'active'
      },
      saveOperationInProgress: false,
      pipelineStatus: {},
      namePattern: '^[a-zA-Z0-9_]*$',
      errors: [],
      activeDetailTab: undefined,

      /**
       * Import link command handler
       */
      importPipelineConfig: function() {
        $rootScope.$broadcast('importPipelineConfig');
      },

      /**
       * Export link command handler
       */
      exportPipelineConfig: function() {
        $rootScope.$broadcast('exportPipelineConfig');
      },

      /**
       * Logout header link command handler
       */
      logout: function() {

      }
    };

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