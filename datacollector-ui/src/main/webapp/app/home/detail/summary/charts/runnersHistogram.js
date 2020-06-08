/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Controller for Pipeline Runners Histogram Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RunnersHistogramChartController', function($scope, $rootScope) {
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
      timerData:[],

      init: function(type) {
        recordType = type;
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + key + ' </p><p>' + x + ': ' + y +'</p>';
        };
      }
    });

    var refreshData = function() {
      var histograms = $scope.detailPaneMetrics.histograms;
      var list = $scope.timerData;
      list.splice(0, list.length);
      if (histograms && histograms['pipeline.runners.histogramM5']) {
        var data = histograms['pipeline.runners.histogramM5'];
        list.push({
          key: 'Runners',
          values: [
            ["Mean" , data.mean ],
            ["Std Dev" , data.stddev ],
            ["99.9%" , data.p999 ],
            ["99%" , data.p99 ],
            ["98%" , data.p98 ],
            ["95%" , data.p95 ],
            ["75%" , data.p75 ],
            ["50%" , data.p50 ]
          ],
          color: '#1f77b4'
        });
      }
    };

    $scope.$on('summaryDataUpdated', function() {
      refreshData();
    });

    refreshData();

  });
