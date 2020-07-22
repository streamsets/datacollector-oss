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
 * Controller for Batch Throughput Bar Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('BatchCountBarChartController', function($scope, $translate, api, pipelineConstant) {
    var color = $scope.recordsColor,
      baseQuery = "select m1_rate,metric from meters where (pipeline='" + $scope.pipelineConfig.info.pipelineId + "') and ",
      yAxisLabel = '( batches / sec )';

    $translate('home.detailPane.batchesPerSecond').then(function(translation) {
      yAxisLabel = translation;
    });

    var getColor = function(d) {
      if(d && d.key && color[d.key]) {
        return color[d.key];
      }
    };

    angular.extend($scope, {
      chartOptions: {
        chart: {
          type: 'discreteBarChart',
          showControls: false,
          height: 250,
          showLabels: true,
          duration: 0,
          x: function(d) {
            return d[0];
          },
          y: function(d) {
            return d[1];
          },
          staggerLabels: true,
          showValues: true,
          yAxis: {
            tickValues: 0,
            axisLabel: yAxisLabel,
            axisLabelDistance: -25
          },
          valueFormat: $scope.formatValue(),
          margin: {
            left: 40,
            top: 20,
            bottom: 40,
            right: 20
          },
          reduceXTicks: false
        }
      },
      chartData: [
        {
          key: "Batch Throughput",
          values: []
        }
      ],


      timeSeriesChartOptions: {
        chart: {
          type: 'lineChart',
          height: 250,
          showLabels: true,
          duration: 0,
          x:function(d){
            return (new Date(d[0])).getTime();
          },
          y: function(d) {
            return d[1];
          },
          color: getColor,
          legendColor: getColor,
          showLegend: true,
          xAxis: {
            tickFormat: $scope.dateFormat()
          },
          yAxis: {
            tickFormat: $scope.formatValue(),
            axisLabel: yAxisLabel,
            axisLabelDistance: -15
          },
          margin: {
            left: 50,
            top: 20,
            bottom: 30,
            right: 20
          },
          useInteractiveGuideline: true
        }
      },

      timeSeriesChartData: [],

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>Batches </p><p>' + $scope.getDurationLabel(x) + ' throughput: ' + e.value.toFixed(2) + '</p>';
        };
      },

      getColor: function() {
        return function(d) {
          if(d && d.key && color[d.key]) {
            return color[d.key];
          }
        };
      },

      xValue: function(){
        return function(d){
          return (new Date(d[0])).getTime();
        };
      },

      yValue: function(){
        return function(d){
          return d[1];
        };
      }
    });

    var refreshChartData = function() {
      if ($scope.summaryMeters && $scope.summaryMeters.batchCount) {
        $scope.chartData[0].values = [
          ["1m" , $scope.summaryMeters.batchCount.m1_rate ],
          ["5m" , $scope.summaryMeters.batchCount.m5_rate ],
          ["15m" , $scope.summaryMeters.batchCount.m15_rate ],
          ["30m" , $scope.summaryMeters.batchCount.m30_rate ],
          ["1h" , $scope.summaryMeters.batchCount.h1_rate ],
          ["6h" , $scope.summaryMeters.batchCount.h6_rate ],
          ["12h" , $scope.summaryMeters.batchCount.h12_rate ],
          ["1d" , $scope.summaryMeters.batchCount.h24_rate ],
          ["Mean" , $scope.summaryMeters.batchCount.mean_rate ]
        ];
      }
    };

    $scope.$on('onSelectionChange', function(event, options) {
      refreshChartData();
    });

    $scope.$on('summaryDataUpdated', function() {
      refreshChartData();
    });

    refreshChartData();
  });
