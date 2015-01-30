/**
 * Logs module for displaying logs page content.
 */

angular
  .module('dataCollectorApp.logs')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/logs',
      {
        templateUrl: 'app/logs/logs.tpl.html',
        controller: 'LogsController'
      }
    );
  }])
  .controller('LogsController', function ($scope, $rootScope, $interval) {
    angular.extend($scope, {
      logMessages: $rootScope.common.getLogMessages(),

      refreshLog: function() {
        $scope.logMessages = $rootScope.common.getLogMessages();
      }
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