/**
 * Controller for Data Summary Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('DataSummaryController', function ($scope) {
    angular.extend($scope, {
      getTooltipContent: function() {
        return function(key, x, y, e, graph) {
          return '<p>' + x + '</p><p>' + e.value.toFixed(2) + '</p>';
        };
      }
    });
  })

  .controller('DataSummaryMeterController', function ($scope, $rootScope, pipelineConstant) {
    angular.extend($scope, {
      chartData: [],
      count: 0
    });

    function updateChartData() {
      var metricRule = $scope.metricRule,
        pipelineMetrics = $rootScope.common.pipelineMetrics;

      if(pipelineMetrics && pipelineMetrics.meters) {
        var meterData = pipelineMetrics.meters['user.' + metricRule.id + '.meter'];

        if(meterData) {
          $scope.count = meterData.count;
          $scope.chartData = [{
            key: metricRule.label,
            values: [
              ["1m" , meterData.m1_rate ],
              ["5m" , meterData.m5_rate ],
              ["15m" , meterData.m15_rate ],
              ["30m" , meterData.m30_rate ],
              ["1h" , meterData.h1_rate ],
              ["6h" , meterData.h6_rate ],
              ["12h" , meterData.h12_rate ],
              ["1d" , meterData.h24_rate ],
              ["Mean" , meterData.mean_rate ]
            ]
          }];
        }

      }
    }

    $rootScope.$watch('common.pipelineMetrics', function() {
      if($scope.isPipelineRunning &&
        $rootScope.common.pipelineMetrics &&
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
      var metricRule = $scope.metricRule,
        pipelineMetrics = $rootScope.common.pipelineMetrics;

      if(pipelineMetrics && pipelineMetrics.histograms) {
        var histogramData = pipelineMetrics.histograms['user.' + metricRule.id + '.histogramM5'];

        if(histogramData) {
          $scope.count = histogramData.count;
          $scope.chartData = [{
            key: metricRule.label,
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

    $rootScope.$watch('common.pipelineMetrics', function() {
      if($scope.isPipelineRunning &&
        $rootScope.common.pipelineMetrics &&
        $scope.selectedType === pipelineConstant.LINK && !$scope.monitoringPaused) {
        updateChartData();
      }
    });

    updateChartData();
  })
  .controller('DataSummarySamplingController', function ($scope, api) {
    angular.extend($scope, {
      showRecordsLoading: false,
      samplingRecords: [],

      refreshSamplingRecords: function() {
        updateSamplingRecords();
      }
    });

    function updateSamplingRecords() {
      $scope.showRecordsLoading = true;
      api.pipelineAgent.getSampledRecords($scope.samplingRule.id)
        .success(function(res) {
          $scope.showRecordsLoading = false;
          if(res && res.length) {
            $scope.samplingRecords = res;
          } else {
            $scope.samplingRecords = [];
          }
        })
        .error(function(data) {
          $scope.showRecordsLoading = false;
          $rootScope.common.errors = [data];
        });
    }

    updateSamplingRecords();

  });

