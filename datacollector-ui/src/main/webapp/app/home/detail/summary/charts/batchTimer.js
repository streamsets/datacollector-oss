/**
 * Controller for Batch Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('BatchTimerChartController', function($scope, $translate) {
    var label = {
        timer: 'Timer (Percentiles)'
      };

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
      if(!$scope.summaryTimer) {
        return;
      }

      $scope.timerData = [
        {
          key: label.timer,
          values: [
            ["Mean" , $scope.summaryTimer.mean ],
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