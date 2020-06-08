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
  .controller('CustomBarChartController', function($scope, $rootScope, pipelineConstant) {

      angular.extend($scope, {
          chartOptions: {
              chart: {
                  type: 'multiBarChart',
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
                      axisLabel: "",
                      axisLabelDistance: -10
                  },
                  xAxis: {
                      axisLabel: "",
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
          chartData: [],
          count: 0
      });

      function updateChartData() {
          $scope.chartData.splice(0, $scope.chartData.length);
          var customStageGauge = $scope.customStageGauge,
            pipelineMetrics = $scope.detailPaneMetrics;

          if(pipelineMetrics && pipelineMetrics.gauges) {
              var gaugeData = pipelineMetrics.gauges[customStageGauge.gaugeKey].value;
              if(gaugeData) {
                  $scope.chartTitle = gaugeData.title;
                  $scope.chartOptions.chart.yAxis.axisLabel = gaugeData.yAxis;
                  for (var data in gaugeData.data) {
                      for (var key in gaugeData.data[data]) {
                          var mapData = {};
                          mapData['key'] = key;
                          mapData['values'] = [];
                          for (var x in gaugeData.data[data][key]) {
                              var arrayVal = [];
                              arrayVal.push(moment(Number(x)).format("DD-MM-YYYY h:mm:ss"));
                              arrayVal.push(gaugeData.data[data][key][x]);
                              mapData['values'].push(arrayVal);
                          }
                          $scope.chartData.push(mapData);
                      }
                  }
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
