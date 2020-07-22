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
 * Controller for Meter Bar Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('MeterBarChartController', function($scope, $rootScope, pipelineConstant, $filter, $translate, api) {
    var color = $scope.recordsColor;
    var yAxisLabel = '( records / sec )';

    $translate('home.detailPane.recordsPerSecond').then(function(translation) {
      yAxisLabel = translation;
    });

    var getColor = function(d) {
      if (color[d.key]) {
        return color[d.key];
      } else {
        return color.Output;
      }
    };

    angular.extend($scope, {
      chartOptions: {
        chart: {
          type: 'multiBarChart',
          stacked: false,
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
          showValues: true,
          yAxis: {
            tickValues: 0,
            axisLabel: yAxisLabel,
            axisLabelDistance: -10
          },
          valueFormat: $scope.formatValue(),
          margin: {
            left: 55,
            top: 20,
            bottom: 40,
            right: 20
          },
          reduceXTicks: false
        }
      },
      chartData: [],


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
            axisLabelDistance: -13
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

      getColor: function() {
        return function(d) {
          if (color[d.key]) {
            return color[d.key];
          } else {
            return color.Output;
          }
        };
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + key + ' Records </p><p>' + $scope.getDurationLabel(x) + ' throughput: ' + e.value.toFixed(2) + '</p>';
        };
      },

      /**
       * Value format function for D3 NVD3 charts.
       *
       * @returns {Function}
       */
      valueFormatFunction: function() {
        return function(d) {
          return $filter('abbreviateNumber')(d);
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

    var updatedLatestData = function() {
      if (!$scope.summaryMeters || !$scope.summaryMeters.inputRecords || !$scope.summaryMeters.outputRecords ||
        !$scope.summaryMeters.errorRecords) {
        return;
      }

      var stageInstance = $scope.detailPaneConfig;
      var pipelineMetrics = $scope.detailPaneMetrics;
      var meterChartData = {};

      meterChartData['Input'] = [
        ['1m' , $scope.summaryMeters.inputRecords.m1_rate ],
        ['5m' , $scope.summaryMeters.inputRecords.m5_rate ],
        ['15m' , $scope.summaryMeters.inputRecords.m15_rate ],
        ['30m' , $scope.summaryMeters.inputRecords.m30_rate ],
        ['1h' , $scope.summaryMeters.inputRecords.h1_rate ],
        ['6h' , $scope.summaryMeters.inputRecords.h6_rate ],
        ['12h' , $scope.summaryMeters.inputRecords.h12_rate ],
        ['1d' , $scope.summaryMeters.inputRecords.h24_rate ],
        ['Mean' , $scope.summaryMeters.inputRecords.mean_rate ]
      ];

      meterChartData['Output'] = [
        ['1m' , $scope.summaryMeters.outputRecords.m1_rate ],
        ['5m' , $scope.summaryMeters.outputRecords.m5_rate ],
        ['15m' , $scope.summaryMeters.outputRecords.m15_rate ],
        ['30m' , $scope.summaryMeters.outputRecords.m30_rate ],
        ['1h' , $scope.summaryMeters.outputRecords.h1_rate ],
        ['6h' , $scope.summaryMeters.outputRecords.h6_rate ],
        ['12h' , $scope.summaryMeters.outputRecords.h12_rate ],
        ['1d' , $scope.summaryMeters.outputRecords.h24_rate ],
        ['Mean' , $scope.summaryMeters.outputRecords.mean_rate ]
      ];

      meterChartData['Error'] = [
        ['1m' , $scope.summaryMeters.errorRecords.m1_rate ],
        ['5m' , $scope.summaryMeters.errorRecords.m5_rate ],
        ['15m' , $scope.summaryMeters.errorRecords.m15_rate ],
        ['30m' , $scope.summaryMeters.errorRecords.m30_rate ],
        ['1h' , $scope.summaryMeters.errorRecords.h1_rate ],
        ['6h' , $scope.summaryMeters.errorRecords.h6_rate ],
        ['12h' , $scope.summaryMeters.errorRecords.h12_rate ],
        ['1d' , $scope.summaryMeters.errorRecords.h24_rate ],
        ['Mean' , $scope.summaryMeters.errorRecords.mean_rate ]
      ];



      if ($scope.stageSelected && stageInstance.uiInfo.stageType === pipelineConstant.PROCESSOR_STAGE_TYPE &&
        stageInstance.outputLanes.length > 1) {

        //Lane Selector
        angular.forEach(stageInstance.outputLanes, function(outputLane, index) {
          var laneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' + outputLane + '.outputRecords.meter'];
          if (laneMeter) {
            meterChartData['Output ' + (index + 1)]  =  [
              ['1m' , laneMeter.m1_rate ],
              ['5m' , laneMeter.m5_rate ],
              ['15m' , laneMeter.m15_rate ],
              ['30m' , laneMeter.m30_rate ],
              ['1h' , laneMeter.h1_rate ],
              ['6h' , laneMeter.h6_rate ],
              ['12h' , laneMeter.h12_rate ],
              ['1d' , laneMeter.h24_rate ],
              ['Mean' , laneMeter.mean_rate ]
            ];
          }
        });
      }

      if (stageInstance.eventLanes && stageInstance.eventLanes.length) {
        var eventLaneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' + stageInstance.eventLanes[0] + '.outputRecords.meter'];
        if (eventLaneMeter) {
          meterChartData['Event']  =  [
            ['1m' , eventLaneMeter.m1_rate ],
            ['5m' , eventLaneMeter.m5_rate ],
            ['15m' , eventLaneMeter.m15_rate ],
            ['30m' , eventLaneMeter.m30_rate ],
            ['1h' , eventLaneMeter.h1_rate ],
            ['6h' , eventLaneMeter.h6_rate ],
            ['12h' , eventLaneMeter.h12_rate ],
            ['1d' , eventLaneMeter.h24_rate ],
            ['Mean' , eventLaneMeter.mean_rate ]
          ];
        }
      }

      var chartData = $scope.chartData;
      angular.forEach(chartData, function(cData) {
        if (meterChartData[cData.key]) {
          cData.values = meterChartData[cData.key];
        }
      });
    };

    var refreshChartDataOnSelectionChange = function() {
      var stageInstance = $scope.detailPaneConfig,
        input = {
          key: 'Input',
          values: []
        },
        output = {
          key: 'Output',
          values: []
        },
        bad = {
          key: 'Error',
          values: []
        },
        event = {
          key: 'Event',
          values: []
        },
        chartData = $scope.chartData;

      chartData.splice(0, chartData.length);

      if ($scope.stageSelected) {
        switch(stageInstance.uiInfo.stageType) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            chartData.push(output);
            chartData.push(bad);
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            chartData.push(input);

            if (stageInstance.outputLanes.length < 2) {
              chartData.push(output);
            } else {
              //Lane Selector
              angular.forEach(stageInstance.outputLanes, function(outputLane, index) {
                chartData.push({
                  key: 'Output ' + (index + 1),
                  values: []
                });
              });
            }

            chartData.push(bad);
            break;
          case pipelineConstant.EXECUTOR_STAGE_TYPE:
            chartData.push(input);
            chartData.push(bad);
            break;
          case pipelineConstant.TARGET_STAGE_TYPE:
            chartData.push(input);
            chartData.push(bad);
            break;
        }

        if (stageInstance.eventLanes && stageInstance.eventLanes.length) {
          chartData.push(event);
        }

      } else {
        chartData.push(input);
        chartData.push(output);
        chartData.push(bad);
      }

      updatedLatestData();
    };

    $scope.$on('summaryDataUpdated', function() {
      updatedLatestData();
    });

    $scope.$watch('timeRange', function() {
      if ($scope.timeRange !== 'latest') {
        refreshTimeSeriesData();
      }
    });

    $scope.$on('onSelectionChange', function(event, options) {
      if (options.type !== pipelineConstant.LINK) {
        refreshChartDataOnSelectionChange();
      }
    });

    refreshChartDataOnSelectionChange();

  });
