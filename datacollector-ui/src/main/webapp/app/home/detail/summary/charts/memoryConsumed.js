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
  .controller('MemoryConsumedLineChartController', function($scope, $rootScope, pipelineConstant, api) {
    var color = $scope.recordsColor,
      baseQuery = "select count,metric from counters where (pipeline='" + $scope.pipelineConfig.info.pipelineId + "') and ",
      getColor = function(d) {
        if(d && d.key && color[d.key]) {
          return color[d.key];
        } else if(color[d[0]]) {
          return color[d[0]];
        } else {
          return color.Output;
        }
      },
      sizeFormat = function(d){
        var mbValue = d;
        return mbValue.toFixed(2) + ' MB';
      };

    angular.extend($scope, {
      chartOptions: {
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
            tickFormat: sizeFormat
          },
          margin: {
            left: 60,
            top: 20,
            bottom: 30,
            right: 30
          },
          useInteractiveGuideline: true
        }
      },

      lineChartData: [
        {
          key: "Total",
          values: []
        }
      ],

      timeSeriesChartOptions: {
        chart: {
          type: 'lineChart',
          height: ($scope.selectedType === pipelineConstant.STAGE_INSTANCE) ? 250 : 500,
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
            tickFormat: sizeFormat
          },
          margin: {
            left: 60,
            top: 20,
            bottom: 30,
            right: 30
          },
          useInteractiveGuideline: true
        }
      },

      timeSeriesChartData: []

    });

    $scope.$on('summaryDataUpdated', function() {
      var currentSelection = $scope.detailPaneConfig,
        memoryConsumed = $rootScope.common.counters.memoryConsumed || {},
        pipelineConfig = $scope.pipelineConfig,
        values;


      if($scope.stageSelected) {
        values =  memoryConsumed[currentSelection.instanceName];
      } else {
        values = memoryConsumed['pipeline.' + pipelineConfig.info.pipelineId];
      }

      if(!values) {
        return;
      }

      $scope.lineChartData[0].values = values;
    });

  });
