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
 * Controller for Custom Stage Meter Bar Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CustomMeterController', function($scope, $rootScope, pipelineConstant) {

    var yAxisLabel = '( events / sec )';

    angular.extend($scope, {
      chartOptions: {
        chart: {
          type: 'discreteBarChart',
          height: 220,
          showLabels: true,
          duration: 0,
          x: function(d) {
            return d[0];
          },
          y: function(d) {
            return d[1];
          },
          //showLegend: true,
          staggerLabels: false,
          showValues: true,
          yAxis: {
            tickValues: 0,
            axisLabel: yAxisLabel,
            axisLabelDistance: -10
          },
          valueFormat: function(d) {
            return d3.format(',d')(d);
          },
          margin: {
            left: 55,
            top: 5,
            bottom: 25,
            right: 20
          },
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
      var customStageMeter = $scope.customStageMeter,
        pipelineMetrics = $scope.detailPaneMetrics;

      if(pipelineMetrics && pipelineMetrics.meters) {
        var meterData = pipelineMetrics.meters[customStageMeter.meterKey];

        if(meterData) {
          $scope.count = meterData.count;

          $scope.chartData[0].key = customStageMeter.label;
          $scope.chartData[0].values = [
            ["1m" , meterData.m1_rate ],
            ["5m" , meterData.m5_rate ],
            ["15m" , meterData.m15_rate ],
            ["30m" , meterData.m30_rate ],
            ["1h" , meterData.h1_rate ],
            ["6h" , meterData.h6_rate ],
            ["12h" , meterData.h12_rate ],
            ["1d" , meterData.h24_rate ],
            ["Mean" , meterData.mean_rate ]
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
