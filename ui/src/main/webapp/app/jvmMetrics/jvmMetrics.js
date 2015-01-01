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
  .controller('JVMMetricsController', function ($scope, $rootScope, $q, $timeout, api, configuration) {
    var jvmMetricsTimer,
      destroyed = false;

    angular.extend($scope, {
      metrics: {}
    });

    /**
     * Fetch the JVM Metrics every Refresh Interval time.
     *
     */
    var refreshPipelineJVMMetrics = function() {

      jvmMetricsTimer = $timeout(
        function() {
        },
        configuration.getRefreshInterval()
      );

      jvmMetricsTimer.then(
        function() {
          api.admin.getJVMMetrics()
            .success(function(data) {
              $scope.metrics = data;
              refreshPipelineJVMMetrics();
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };

    refreshPipelineJVMMetrics();

    $scope.$on('$destroy', function(){
      $timeout.cancel(jvmMetricsTimer);
      destroyed = true;
    });

  });