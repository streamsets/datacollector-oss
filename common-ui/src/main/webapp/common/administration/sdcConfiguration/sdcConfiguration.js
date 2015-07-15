/**
 * SDC Configuration module.
 */

angular
  .module('dataCollectorApp.sdcConfiguration')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/configuration',
      {
        templateUrl: 'common/administration/sdcConfiguration/sdcConfiguration.tpl.html',
        controller: 'SDCConfigurationController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin', 'creator', 'manager']
        }
      }
    );
  }])
  .controller('SDCConfigurationController', function ($scope, $rootScope, $q, Analytics, configuration, _) {

    angular.extend($scope, {
      initDefer: undefined,
      configKeys: [],
      sdcConfiguration: {}
    });

    console.log('inside method SDCConfigurationController');

    $scope.initDefer = $q.all([configuration.init()]).then(function() {
      console.log('inside method SDCConfigurationController after init');
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/configuration');
      }

      $scope.sdcConfiguration = configuration.getConfiguration();
      $scope.configKeys = _.keys($scope.sdcConfiguration);
      $scope.configKeys.sort();
    });

  });