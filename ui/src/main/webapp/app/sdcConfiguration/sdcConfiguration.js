/**
 * SDC Configuration module.
 */

angular
  .module('dataCollectorApp.sdcConfiguration')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/configuration',
      {
        templateUrl: 'app/sdcConfiguration/sdcConfiguration.tpl.html',
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
      configKeys: [],
      sdcConfiguration: {}
    });

    $q.all([configuration.init()]).then(function() {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/logs');
      }

      $scope.sdcConfiguration = configuration.getConfiguration();
      $scope.configKeys = _.keys($scope.sdcConfiguration);
      $scope.configKeys.sort();
    });
  });