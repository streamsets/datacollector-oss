/**
 * Logs module for displaying logs page content.
 */

angular
  .module('dataCollectorApp.logs')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/logs',
      {
        templateUrl: 'app/logs/logs.tpl.html',
        controller: 'LogsController',
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
  .controller('LogsController', function ($scope, $rootScope, $interval, api, configuration, Analytics) {

    configuration.init().then(function() {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/logs');
      }
    });

    angular.extend($scope, {
      logMessages: $rootScope.common.getLogMessages(),
      logFiles: [],

      refreshLog: function() {
        $scope.logMessages = $rootScope.common.getLogMessages();
      }
    });


    api.log.getFilesList().then(function(res) {
      $scope.logFiles = res.data;
    });


    var intervalPromise = $interval(function() {
      $scope.logMessages = $rootScope.common.getLogMessages();
    }, 2000);

    $scope.$on('$destroy', function() {
      if(angular.isDefined(intervalPromise)) {
        $interval.cancel(intervalPromise);
      }
    });

  });