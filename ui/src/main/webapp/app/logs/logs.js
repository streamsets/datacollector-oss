/**
 * Logs module for displaying logs page content.
 */

angular
  .module('pipelineAgentApp.logs')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/logs',
      {
        templateUrl: 'app/logs/logs.tpl.html',
        controller: 'LogsController'
      }
    );
  }])
  .controller('LogsController', function ($scope, api) {
    angular.extend($scope, {
      serverLog: 'Coming Soon...'
    });


    /*api.pipelineAgent.getServerLog()
      .success(function(data){
        $scope.serverLog = data;
      })
      .error(function(data, status, headers, config) {
        $rootScope.common.errors = [data];
      });
    */

  });