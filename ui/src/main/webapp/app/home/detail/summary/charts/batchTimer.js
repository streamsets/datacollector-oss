/**
 * Controller for Batch Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('BatchTimerChartController', function($scope, $translate) {
    var label = {
        frequency : 'Frequency (batches/sec)',
        duration: 'Duration (Seconds)',
        timer: 'Timer (Percentiles)'
      };

    $translate('home.detailPane.summaryTab.frequency').then(function(translation) {
      label.frequency = translation;
    });

    $translate('home.detailPane.summaryTab.duration').then(function(translation) {
      label.duration = translation;
    });

    $translate('home.detailPane.summaryTab.timer').then(function(translation) {
      label.timer = translation;
    });

    angular.extend($scope, {
      frequencyData: [],
      durationData: [],
      timerData: [],
      getColor: function() {
        return function(d) {
          return d.color;
        };
      }
    });

    $scope.$on('summaryDataUpdated', function() {

      $scope.frequencyData = [
        {
          key: label.frequency,
          values: [
            ["1 min" , $scope.summaryTimer.m1_rate ],
            ["5 min" , $scope.summaryTimer.m5_rate ],
            ["15 min" , $scope.summaryTimer.m15_rate ],
            ["Mean" , $scope.summaryTimer.mean_rate ]
          ],
          color: '#1f77b4'
        }
      ];

      $scope.durationData = [
        {
          key: label.duration,
          values: [
            ["Min" , $scope.summaryTimer.min ],
            ["Mean" , $scope.summaryTimer.mean ],
            ["Max" , $scope.summaryTimer.max ],
            ["Std Dev" , $scope.summaryTimer.stddev ]
          ],
          color: '#AEC7E8'
        }
      ];

      $scope.timerData = [
        {
          key: label.timer,
          values: [
            //["Min" , $scope.summaryTimer.min ],
            ["Mean" , $scope.summaryTimer.mean ],
            //["Max" , $scope.summaryTimer.max ],
            ["Std Dev" , $scope.summaryTimer.stddev ],
            ["99.9%" , $scope.summaryTimer.p999 ],
            ["99%" , $scope.summaryTimer.p99 ],
            ["98%" , $scope.summaryTimer.p98 ],
            ["95%" , $scope.summaryTimer.p95 ],
            ["75%" , $scope.summaryTimer.p75 ],
            ["50%" , $scope.summaryTimer.p50 ]
          ],
          color: '#FF7F0E'
        }
      ];

    });

  });