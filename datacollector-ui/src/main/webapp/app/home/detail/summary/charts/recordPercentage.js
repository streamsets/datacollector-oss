/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
  .controller('RecordPercentagePieChartController', function($scope) {
    var colorArray = ['#5cb85c', '#FF3333'];

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
        }
      ]
    });

    var refreshChartData = function() {
      $scope.allDataZero = true;
      if($scope.summaryMeters.outputRecords && $scope.summaryMeters.errorRecords) {
        angular.forEach($scope.pieChartData, function(chartData) {
          if(chartData.key === 'goodRecords') {
            chartData.value = $scope.summaryMeters.outputRecords.count;
          } else if(chartData.key === 'errorRecords') {
            chartData.value = $scope.summaryMeters.errorRecords.count;
          }

          if(chartData.value > 0) {
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