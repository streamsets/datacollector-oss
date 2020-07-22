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
 * Controller for Data Summary Tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('DataSummaryController', function ($scope) {
    angular.extend($scope, {
      allDataRuleDefinitions: $scope.pipelineRules.dataRuleDefinitions.concat($scope.pipelineRules.driftRuleDefinitions),
      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + x + '</p><p>' + e.value.toFixed(2) + '</p>';
        };
      }
    });
  })

  .controller('DataSummaryMeterController', function ($scope, $rootScope, $translate, pipelineConstant) {
    var yAxisLabel = '( records / sec )';

    $translate('home.detailPane.recordsPerSecond').then(function(translation) {
      yAxisLabel = translation;
    });

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
          showLegend: true,
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
            left:65,top:10,bottom:40,right:20
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
      var dataRuleDefn = $scope.dataRuleDefn,
        pipelineMetrics = $scope.detailPaneMetrics;

      if(pipelineMetrics && pipelineMetrics.meters) {
        var meterData = pipelineMetrics.meters['user.' + dataRuleDefn.id + '.meter'];

        if(meterData) {
          $scope.count = meterData.count;

          $scope.chartData[0].key = dataRuleDefn.label;
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
        $scope.selectedType === pipelineConstant.LINK && !$scope.monitoringPaused) {
        updateChartData();
      }
    });

    updateChartData();


  })

  .controller('DataSummaryHistogramController', function ($scope, $rootScope, pipelineConstant) {
    angular.extend($scope, {
      chartData: [],
      count: 0
    });

    function updateChartData() {
      var dataRuleDefn = $scope.dataRuleDefn,
        pipelineMetrics = $scope.detailPaneMetrics;

      if(pipelineMetrics && pipelineMetrics.histograms) {
        var histogramData = pipelineMetrics.histograms['user.' + dataRuleDefn.id + '.histogramM5'];

        if(histogramData) {
          $scope.count = histogramData.count;
          $scope.chartData = [{
            key: dataRuleDefn.label,
            values: [
              ["Mean" , histogramData.mean ],
              ["Std Dev" , histogramData.stddev ],
              ["99.9%" , histogramData.p999 ],
              ["99%" , histogramData.p99 ],
              ["98%" , histogramData.p98 ],
              ["95%" , histogramData.p95 ],
              ["75%" , histogramData.p75 ],
              ["50%" , histogramData.p50 ]
            ]
          }];
        }

      }
    }

    $scope.$watch('detailPaneMetrics', function() {
      if($scope.detailPaneMetrics &&
        $scope.selectedType === pipelineConstant.LINK && !$scope.monitoringPaused) {
        updateChartData();
      }
    });

    updateChartData();
  })
  .controller('DataSummarySamplingController', function ($scope, api) {
    angular.extend($scope, {
      sampledRecordsType: 'matched',
      showRecordsLoading: false,
      samplingRecords: [],

      downloadSamplingRecords: function($event) {
        var data = JSON.stringify($scope.samplingRecords);
        var uri = 'data:application/octet-stream;charset=utf-16le,' + encodeURI(data);
        angular.element($event.target).attr('href', uri);
      },

      refreshSamplingRecords: function() {
        updateSamplingRecords();
      },

      getRecordHeader: function(sampledRecord, index) {
        if(sampledRecord.matchedCondition) {
          return '<span class="condition-matched"> Record' + (index + 1) + ' ( A Match ) </span>';
        } else {
          return '<span class="condition-not-matched"> Record' + (index + 1) + ' ( Not A Match ) </span>';
        }
      },

      filterSampledRecords: function(sampledRecord) {
        switch($scope.sampledRecordsType) {
          case 'matched':
            return sampledRecord.matchedCondition === true;
          case 'notMatched':
            return sampledRecord.matchedCondition === false;
          case 'all':
            return true;
        }
      }
    });

    function updateSamplingRecords() {
      $scope.showRecordsLoading = true;
      api.pipelineAgent.getSampledRecords($scope.pipelineConfig.info.pipelineId, $scope.dataRuleDefn.id,
        $scope.dataRuleDefn.samplingRecordsToRetain)
        .then(function(response) {
          var res = response.data;
          $scope.showRecordsLoading = false;
          if(res && res.length) {
            $scope.samplingRecords = res;
            $scope.samplingRecordsSizeInBytes = (new TextEncoder('utf-16le').encode(JSON.stringify(res))).length;
          } else {
            $scope.samplingRecords = [];
          }
        })
        .catch(function(res) {
          $scope.showRecordsLoading = false;
          $rootScope.common.errors = [res.data];
        });
    }

    updateSamplingRecords();

  });
