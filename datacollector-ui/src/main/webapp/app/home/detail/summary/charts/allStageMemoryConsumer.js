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
 * Controller for Batch Timer Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('AllStageMemoryConsumedChartController', function($rootScope, $scope, api, pipelineConstant) {
    var baseQuery = "select count,metric from counters where (pipeline='" + $scope.pipelineConfig.info.pipelineId + "') and ",
      sizeFormat = function(d){
        var mbValue = d;
        return mbValue.toFixed(1) + ' MB';
      };

    angular.extend($scope, {
      allDataZero: true,
      totalValue: 0,
      chartOptions: {
        chart: {
          type: 'pieChart',
          height: 500,
          x: function(d) {
            return d.key;
          },
          y: function(d){
            return d.value;
          },
          showLabels: false,
          showLegend: true,
          donut: true,
          //donutRatio: '.45',
          labelsOutside: true,
          transitionDuration: 500,
          labelThreshold: 0.01,
          legend: {
            margin: {
              left:10,
              top:10,
              bottom:10,
              right:10
            }
          }
        }
      },
      chartData: [],

      timeSeriesChartOptions: {
        chart: {
          type: 'lineChart',
          height: 500,
          showLabels: true,
          duration: 0,
          x:function(d){
            return (new Date(d[0])).getTime();
          },
          y: function(d) {
            return d[1];
          },
          showLegend: true,
          xAxis: {
            tickFormat: $scope.dateFormat()
          },
          yAxis: {
            tickFormat: sizeFormat
          },
          margin: {
            left: 60,
            top: 20,
            bottom: 30,
            right: 20
          },
          useInteractiveGuideline: true
        }
      },

      timeSeriesChartData: [],

      getLabel: function(){
        return function(d) {
          return d.key;
        };
      },

      getValue: function() {
        return function(d){
          if(d.value > 0) {
            return d.value.toFixed(2);
          } else {
            return 0;
          }
        };
      },

      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          var mbValue = y.value /1;
          return '<p>' + key + '</p><p>' +  mbValue.toFixed(2) + ' MB </p>';
        };
      },

      sizeFormat: function(){
        return function(d){
          var mbValue = d;
          return mbValue.toFixed(1) + ' MB';
        };
      },

      xValue: function(){
        return function(d){
          return (new Date(d[0])).getTime();
        };
      },

      yValue: function(){
        return function(d){
          if(d[1] > 0) {
            return (d[1]).toFixed(2);
          } else {
            return 0;
          }
        };
      }
    });


    var stages = $scope.stageInstances;

    angular.forEach(stages, function(stage) {
      $scope.chartData.push({
        instanceName: stage.instanceName,
        key: stage.uiInfo.label,
        value: 0
      });
    });

    $scope.$on('summaryDataUpdated', function() {
      var pipelineMetrics = $scope.detailPaneMetrics,
        values = [],
        total = 0;

      $scope.allDataZero = true;

      if(!pipelineMetrics.counters) {
        return;
      }

      angular.forEach($scope.chartData, function(data) {
        var stageCounter = pipelineMetrics.counters['stage.' + data.instanceName + '.memoryConsumed.counter'];
        if(stageCounter) {
          data.value =  (stageCounter.count);
          values.push(data);
          total += stageCounter.count;

          if(data.value > 0) {
            $scope.allDataZero = false;
          }
        }
      });

      $scope.chartData = values;
      $scope.totalValue = (total).toFixed(2);
    });
  });
