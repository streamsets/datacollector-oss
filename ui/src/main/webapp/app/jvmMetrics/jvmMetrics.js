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
  .controller('JVMMetricsController', function ($scope) {

  });