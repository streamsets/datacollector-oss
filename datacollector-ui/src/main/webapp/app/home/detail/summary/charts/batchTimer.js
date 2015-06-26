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

    var getColor = function(d) {
      return d.color;
    };

    angular.extend($scope, {
      chartOptions: {
        chart: {
          type: 'multiBarHorizontalChart',
          stacked: true,
          height: 250,
          showLabels: true,
          duration: 0,
          x: function(d) {
            return d[0];
          },
          y: function(d) {
            return d[1];
          },
          color: getColor,
          showLegend: true,
          staggerLabels: true,
          showValues: false,
          yAxis: {
            tickValues: 0
          },
          valueFormat: $scope.formatValue(),
          margin: {
            left: 55,
            top: 20,
            bottom: 20,
            right: 20
          },
          reduceXTicks: false,
          showControls: false
        }
      },

      timerData: [
        {
          key: label.timer,
          values: [],
          color: '#FF7F0E'
        }
      ]
    });

    var refreshChartData = function() {
      if(!$scope.summaryTimer) {
        return;
      }

      $scope.timerData[0].values = [
        ["Mean" , $scope.summaryTimer.mean ],
        ["Std Dev" , $scope.summaryTimer.stddev ],
        ["99.9%" , $scope.summaryTimer.p999 ],
        ["99%" , $scope.summaryTimer.p99 ],
        ["98%" , $scope.summaryTimer.p98 ],
        ["95%" , $scope.summaryTimer.p95 ],
        ["75%" , $scope.summaryTimer.p75 ],
        ["50%" , $scope.summaryTimer.p50 ]
      ];
    };

    $scope.$on('summaryDataUpdated', function() {
      refreshChartData();
    });

    refreshChartData();
  });