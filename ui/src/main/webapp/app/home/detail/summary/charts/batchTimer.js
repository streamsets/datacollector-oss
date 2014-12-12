/**
 * Controller for Batch Timer Chart.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('BatchTimerChartController', function($scope) {
    var color = {
      'Frequency (batches/second)' :'#1f77b4',
      'Duration (Seconds)': '#AEC7E8',
      'Timer (Percentiles)':'#FF7F0E'
    };

    angular.extend($scope, {
      frequencyData: [],
      durationData: [],
      timerData: [],
      getColor: function() {
        return function(d) {
          return color[d.key];
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {

      $scope.frequencyData = [
        {
          key: "Frequency (batches/second)",
          values: [
            ["1 min" , $scope.summaryTimer.m1_rate ],
            ["5 min" , $scope.summaryTimer.m5_rate ],
            ["15 min" , $scope.summaryTimer.m15_rate ],
            ["Mean" , $scope.summaryTimer.mean_rate ]
          ]
        }
      ];

      $scope.durationData = [
        {
          key: "Duration (Seconds)",
          values: [
            ["Min" , $scope.summaryTimer.min ],
            ["Mean" , $scope.summaryTimer.mean ],
            ["Max" , $scope.summaryTimer.max ],
            ["Std Dev" , $scope.summaryTimer.stddev ]
          ]
        }
      ];


      $scope.timerData = [
        {
          key: "Timer (Percentiles)",
          values: [
            ["99.9%" , $scope.summaryTimer.p999 ],
            ["99%" , $scope.summaryTimer.p99 ],
            ["98%" , $scope.summaryTimer.p98 ],
            ["95%" , $scope.summaryTimer.p95 ],
            ["75%" , $scope.summaryTimer.p75 ],
            ["50%" , $scope.summaryTimer.p50 ]
          ]
        }
      ];

    });

  });