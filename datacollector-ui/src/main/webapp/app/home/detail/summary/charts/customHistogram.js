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
 * Controller for Custom Stage Histogram Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CustomHistogramController', function($scope, $rootScope, pipelineConstant) {
    angular.extend($scope, {
      chartOptions: {
        chart: {
          type: 'multiBarHorizontalChart',
          stacked: true,
          height: 220,
          showLabels: true,
          duration: 0,
          x: function(d) {
            return d[0];
          },
          y: function(d) {
            return d[1];
          },
          showLegend: false,
          staggerLabels: true,
          showValues: false,
          yAxis: {
            tickValues: 0
          },
          margin: {
            left: 55,
            top: 20,
            bottom: 20,
            right: 20
          },
          reduceXTicks: false,
          showControls: false,
          tooltip: {
            valueFormatter: function(d) {
              return d.toFixed(4);
            }
          }
        }
      },
      chartData: [{
        key: undefined,
        values: []
      }],
      count: 0
    });

    function updateChartData() {
      var customStageHistogram = $scope.customStageHistogram,
        pipelineMetrics = $scope.detailPaneMetrics;

      if(pipelineMetrics && pipelineMetrics.histograms) {
        var histogramData = pipelineMetrics.histograms[customStageHistogram.histogramKey];

        if(histogramData) {
          $scope.count = histogramData.count;

          $scope.chartData[0].key = customStageHistogram.label;
          $scope.chartData[0].values = [
            ["Mean" , histogramData.mean ],
            ["Std Dev" , histogramData.stddev ],
            ["99.9%" , histogramData.p999 ],
            ["99%" , histogramData.p99 ],
            ["98%" , histogramData.p98 ],
            ["95%" , histogramData.p95 ],
            ["75%" , histogramData.p75 ],
            ["50%" , histogramData.p50 ]
          ];
        }

      }
    }

    $scope.$watch('detailPaneMetrics', function() {
      if($scope.detailPaneMetrics &&
        $scope.selectedType === pipelineConstant.STAGE_INSTANCE &&
        !$scope.monitoringPaused) {
        updateChartData();
      }
    });

    updateChartData();
  });
