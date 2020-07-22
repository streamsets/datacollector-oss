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
 * Controller for Record Processed Bar Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RecordCountBarChartController', function($scope, $rootScope, pipelineConstant, api, $filter) {
    var color = $scope.recordsColor,
      baseQuery = "select count,metric from meters where (pipeline='" + $scope.pipelineConfig.info.pipelineId + "') and ";

    var getColor = function(d) {
      if (d && d.key && color[d.key]) {
        return color[d.key];
      } else if (color[d[0]]) {
        return color[d[0]];
      } else {
        return color.Output;
      }
    };

    angular.extend($scope, {
      chartOptions: {
        chart: {
          type: 'discreteBarChart',
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
          staggerLabels: false,
          showValues: true,
          yAxis: {
            tickValues: 0
          },
          valueFormat: function(d) {
            return d3.format(',d')(d);
          },
          margin: {
            left: -4,
            top: 20,
            bottom: 25,
            right: 0
          }
        }
      },
      barChartData: [
        {
          key: "Processed Records",
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
            tickFormat: $scope.formatValue()
          },
          margin: {
            left: 40,
            top: 20,
            bottom: 30,
            right: 20
          },
          useInteractiveGuideline: true
        }
      },

      timeSeriesChartData: [],

      getYAxisLabel: function() {
        return function() {
          return '';
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

    var refreshData = function() {
      var stageInstance = $scope.detailPaneConfig;
      var pipelineMetrics = $scope.detailPaneMetrics;
      var valueList = [];
      var inputRecordsMeter = $scope.summaryMeters.inputRecords;
      var outputRecordsMeter = $scope.summaryMeters.outputRecords;
      var errorRecordsMeter = $scope.summaryMeters.errorRecords;

      if (!inputRecordsMeter || !outputRecordsMeter || !errorRecordsMeter) {
        $scope.barChartData[0].values = [];
        return;
      }

      if ($scope.stageSelected) {
        switch(stageInstance.uiInfo.stageType) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            valueList.push(["Output" , outputRecordsMeter.count ]);
            valueList.push(["Error" , errorRecordsMeter.count ]);
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            valueList.push(["Input" , inputRecordsMeter.count ]);

            if (stageInstance.outputLanes.length < 2) {
              valueList.push(["Output" , outputRecordsMeter.count ]);
            } else {
              //Lane Selector
              angular.forEach(stageInstance.outputLanes, function(outputLane, index) {
                var laneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' + outputLane + '.outputRecords.meter'];
                if (laneMeter) {
                  valueList.push(["Output " + (index + 1), laneMeter.count ]);
                }
              });
            }

            valueList.push(["Error" , errorRecordsMeter.count ]);
            break;
          case pipelineConstant.EXECUTOR_STAGE_TYPE:
            valueList.push(["Input" , inputRecordsMeter.count ]);
            valueList.push(["Error" , errorRecordsMeter.count ]);
            break;
          case pipelineConstant.TARGET_STAGE_TYPE:
            valueList.push(["Input" , inputRecordsMeter.count ]);
            valueList.push(["Error" , errorRecordsMeter.count ]);
            break;
        }

        // Event Lanes
        if (stageInstance.eventLanes && stageInstance.eventLanes.length) {
          var eventLaneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' + stageInstance.eventLanes[0] + '.outputRecords.meter'];
          if (eventLaneMeter) {
            valueList.push(["Event" , eventLaneMeter.count ]);
          }
        }
      } else {
        valueList.push(["Input" , inputRecordsMeter.count ]);
        valueList.push(["Output" , outputRecordsMeter.count ]);
        valueList.push(["Error" , errorRecordsMeter.count ]);
      }

      $scope.barChartData[0].values = valueList;
    };


    $scope.$on('summaryDataUpdated', function() {
      refreshData();
    });

    if ($scope.summaryMeters) {
      refreshData();
    }

  });
