/**
 * Controller for Batch Throughput Bar Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('BatchCountBarChartController', function($scope) {
    angular.extend($scope, {
      chartData: [],

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>Batches </p><p>' + $scope.getDurationLabel(x) + ' throughput: ' + e.value.toFixed(2) + '</p>';
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      if($scope.summaryMeters && $scope.summaryMeters.batchCount) {
        $scope.chartData = [
          {
            key: "Batch Throughput",
            values: [
              ["1m" , $scope.summaryMeters.batchCount.m1_rate ],
              ["5m" , $scope.summaryMeters.batchCount.m5_rate ],
              ["15m" , $scope.summaryMeters.batchCount.m15_rate ],
              ["30m" , $scope.summaryMeters.batchCount.m30_rate ],
              ["1h" , $scope.summaryMeters.batchCount.h1_rate ],
              ["6h" , $scope.summaryMeters.batchCount.h6_rate ],
              ["12h" , $scope.summaryMeters.batchCount.h12_rate ],
              ["1d" , $scope.summaryMeters.batchCount.h24_rate ],
              ["Mean" , $scope.summaryMeters.batchCount.mean_rate ]
            ]
          }
        ];
      }

    });

  });