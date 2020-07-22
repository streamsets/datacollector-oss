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
 * Controller for Record Percentage Pie Chart.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RecordPercentagePieChartController', function($rootScope, $scope) {
    var colorArray = ['#5cb85c', '#FF3333', '#ff9f4a'];

    angular.extend($scope, {
      allDataZero: false,
      chartOptions: {
        chart: {
          type: 'pieChart',
          height: 250,
          x: function(d) {
            switch(d.key) {
              case 'goodRecords':
                return 'Good Records';
              case 'errorRecords':
                return 'Error Records';
              case 'eventRecords':
                return 'Event Records';
            }
          },
          y: function(d){
            return d.value;
          },
          showLabels: true,
          color: function(d, i) {
            return colorArray[i];
          },
          legendColor: function(d, i) {
            return colorArray[i];
          },
          showLegend: true,
          labelType: "percent",
          donut: true,
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
      pieChartData: [
        {
          key: "goodRecords",
          value: 0
        },
        {
          key: "errorRecords",
          value: 0
        },
        {
          key: "eventRecords",
          value: 0
        }
      ]
    });

    var refreshChartData = function() {
      var stageInstance = $scope.detailPaneConfig;
      var pipelineMetrics = $scope.detailPaneMetrics;
      $scope.allDataZero = true;
      if ($scope.summaryMeters && $scope.summaryMeters.outputRecords && $scope.summaryMeters.errorRecords) {
        angular.forEach($scope.pieChartData, function(chartData) {
          if (chartData.key === 'goodRecords') {
            chartData.value = $scope.summaryMeters.outputRecords.count;
          } else if (chartData.key === 'errorRecords') {
            chartData.value = $scope.summaryMeters.errorRecords.count;
          } else if (chartData.key === 'eventRecords') {
            if (stageInstance.eventLanes && stageInstance.eventLanes.length) {
              var eventLaneMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + ':' +
              stageInstance.eventLanes[0] + '.outputRecords.meter'];
              if (eventLaneMeter) {
                chartData.value = eventLaneMeter.count;
              }
            } else {
              chartData.value = 0;
            }
          }

          if (chartData.value > 0) {
            $scope.allDataZero = false;
          }
        });
      }
    };

    $scope.$on('summaryDataUpdated', function() {
      refreshChartData();
    });

    refreshChartData();
  });
