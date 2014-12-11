/**
 * Controller for Batch Throughput Bar Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('BatchCountBarChartController', function($scope) {
    angular.extend($scope, {
      chartData: []
    });

    $scope.$on('summaryDataUpdated', function() {
      $scope.chartData = [
        {
          key: "Batch Throughput",
          values: [
            ["M1" , $scope.summaryMeters.batchCount.m1_rate ],
            ["M5" , $scope.summaryMeters.batchCount.m5_rate ],
            ["M15" , $scope.summaryMeters.batchCount.m15_rate ],
            ["M30" , $scope.summaryMeters.batchCount.m30_rate ],
            ["H1" , $scope.summaryMeters.batchCount.h1_rate ],
            ["H6" , $scope.summaryMeters.batchCount.h6_rate ],
            ["H12" , $scope.summaryMeters.batchCount.h12_rate ],
            ["H24" , $scope.summaryMeters.batchCount.h24_rate ],
            ["Mean" , $scope.summaryMeters.batchCount.mean_rate ]
          ]
        }
      ];
    });

  });