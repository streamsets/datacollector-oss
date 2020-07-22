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
 * Controller for Custom Stage Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CustomTimerController', function($scope, $rootScope, pipelineConstant) {
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
      var customStageTimer = $scope.customStageTimer,
        pipelineMetrics = $scope.detailPaneMetrics;

      if(pipelineMetrics && pipelineMetrics.timers) {
        var timerData = pipelineMetrics.timers[customStageTimer.timerKey];

        if(timerData) {
          $scope.count = timerData.count;

          $scope.chartData[0].key = customStageTimer.label;
          $scope.chartData[0].values = [
            ["Mean" , timerData.mean ],
            ["Std Dev" , timerData.stddev ],
            ["99.9%" , timerData.p999 ],
            ["99%" , timerData.p99 ],
            ["98%" , timerData.p98 ],
            ["95%" , timerData.p95 ],
            ["75%" , timerData.p75 ],
            ["50%" , timerData.p50 ]
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
