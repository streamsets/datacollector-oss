/**
 * Controller for Meter Bar Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('MeterBarChartController', function($scope) {
    var colorArray = ['#1f77b4', '#5cb85c', '#FF3333'];

    angular.extend($scope, {
      chartData: [],

      getColor: function() {
        return function(d, i) {
          return colorArray[i];
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {
      $scope.units = $scope.summaryMeters.inputRecords.units;
      $scope.chartData = [
        {
          key: "Input Records",
          values: [
            ["M1" , $scope.summaryMeters.inputRecords.m1_rate ],
            ["M5" , $scope.summaryMeters.inputRecords.m5_rate ],
            ["M15" , $scope.summaryMeters.inputRecords.m15_rate ],
            ["M30" , $scope.summaryMeters.inputRecords.m30_rate ],
            ["H1" , $scope.summaryMeters.inputRecords.h1_rate ],
            ["H6" , $scope.summaryMeters.inputRecords.h6_rate ],
            ["H12" , $scope.summaryMeters.inputRecords.h12_rate ],
            ["H24" , $scope.summaryMeters.inputRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.inputRecords.mean_rate ]
          ]
        },
        {
          key: "Output Records",
          values: [
            ["M1" , $scope.summaryMeters.outputRecords.m1_rate ],
            ["M5" , $scope.summaryMeters.outputRecords.m5_rate ],
            ["M15" , $scope.summaryMeters.outputRecords.m15_rate ],
            ["M30" , $scope.summaryMeters.outputRecords.m30_rate ],
            ["H1" , $scope.summaryMeters.outputRecords.h1_rate ],
            ["H6" , $scope.summaryMeters.outputRecords.h6_rate ],
            ["H12" , $scope.summaryMeters.outputRecords.h12_rate ],
            ["H24" , $scope.summaryMeters.outputRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.outputRecords.mean_rate ]
          ]
        },
        {
          key: "Error Records",
          values: [
            ["M1" , $scope.summaryMeters.errorRecords.m1_rate ],
            ["M5" , $scope.summaryMeters.errorRecords.m5_rate ],
            ["M15" , $scope.summaryMeters.errorRecords.m15_rate ],
            ["M30" , $scope.summaryMeters.errorRecords.m30_rate ],
            ["H1" , $scope.summaryMeters.errorRecords.h1_rate ],
            ["H6" , $scope.summaryMeters.errorRecords.h6_rate ],
            ["H12" , $scope.summaryMeters.errorRecords.h12_rate ],
            ["H24" , $scope.summaryMeters.errorRecords.h24_rate ],
            ["Mean" , $scope.summaryMeters.errorRecords.mean_rate ]
          ]
        }
      ];
    });

  });