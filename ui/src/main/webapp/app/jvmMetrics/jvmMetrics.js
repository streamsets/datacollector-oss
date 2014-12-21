/**
 * JVMMetrics module for displaying JVM Metrics page content.
 */

angular
  .module('pipelineAgentApp.jvmMetrics')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/collector/jvmMetrics',
      {
        templateUrl: 'app/jvmMetrics/jvmMetrics.tpl.html',
        controller: 'JVMMetricsController'
      }
    );
  }])
  .controller('JVMMetricsController', function ($scope, $q, api) {
    angular.extend($scope, {
      memoryMetrics: {}
    });


    /**
     * Fetch JMV Metrics JSON .
     */
    $q.all([
      api.pipelineAgent.getJVMMetrics()
    ])
      .then(function (results) {
        var jvmMetrics = results[0].data;
        updateMetrics(jvmMetrics);
      });


    /**
     * Update Metrics
     *
     * @param jvmMetrics
     */
    var updateMetrics = function(jvmMetrics) {
      angular.forEach(jvmMetrics.beans, function(metrics) {
        if(metrics.name === 'java.lang:type=Memory') {
          $scope.memoryMetrics = metrics;
        }
      });
    };
  });