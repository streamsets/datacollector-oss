/**
 * SDC Configuration module.
 */

angular
  .module('dataCollectorApp.sdcConfiguration')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/configuration',
      {
        templateUrl: 'app/sdcConfiguration/sdcConfiguration.tpl.html',
        controller: 'SDCConfigurationController'
      }
    );
  }])
  .controller('SDCConfigurationController', function ($scope, $rootScope, $q, configuration, _) {
    angular.extend($scope, {
      configKeys: [],
      sdcConfiguration: {}
    });

    $q.all([configuration.init()]).then(function() {
      $scope.sdcConfiguration = configuration.getConfiguration();
      $scope.configKeys = _.keys($scope.sdcConfiguration);
      $scope.configKeys.sort();
    });
  });