/**
 * JVMMetrics module for displaying JVM Metrics page content.
 */

angular
  .module('pipelineAgentApp.jvmMetrics')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/jvmMetrics',
      {
        templateUrl: 'app/jvmMetrics/jvmMetrics.tpl.html',
        controller: 'JVMMetricsController'
      }
    );
  }])
  .controller('JVMMetricsController', function ($scope, $q) {
    angular.extend($scope, {
      jvmMetrics: {}
    });


    /**
     * Fetch JMV Metrics JSON .
     */
    $q.all([
      api.pipelineAgent.getJVMMetrics()
    ])
      .then(function (results) {
        $scope.jvmMetrics = results[0].data;
      });

  });