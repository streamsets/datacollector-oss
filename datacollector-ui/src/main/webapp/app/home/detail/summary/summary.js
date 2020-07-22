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
 * Controller for Summary Tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('SummaryController', function (
    $scope, $rootScope, $modal, $filter, $timeout,
    pipelineConstant, pipelineTracking
  ) {
    var chartList = [
      {
        label: 'home.detailPane.summaryTab.slaveSDCInstances',
        templateId: 'slaveSDCInstancesTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.recordsProcessed',
        templateId: 'summaryRecordPercentagePieChartTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.recordCountBarChartTitle',
        templateId: 'summaryRecordCountBarChartTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.recordThroughput',
        templateId: 'summaryRecordsThroughputMeterBarChartTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.batchThroughput',
        templateId: 'summaryBatchThroughputBarChartTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.stageBatchProcessingTimer',
        templateId: 'summaryAllStageBatchTimerBarChartTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.runtimeStatistics',
        templateId: 'summaryRuntimeStatisticsTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.batchProcessingTimer',
        templateId: 'summaryRecordsProcessedTemplate'
      },
      {
        label: 'global.form.histogram',
        templateId: 'summaryRecordHistogramTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.stageMemoryConsumed',
        templateId: 'summaryAllStageMemoryConsumedBarChartTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.memoryConsumed',
        templateId: 'memoryConsumedLineChartTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.runtimeParameters',
        templateId: 'summaryRuntimeConstantsTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.runnersHistogram',
        templateId: 'summaryPipelineRunnersTemplate'
      }
    ];

    angular.extend($scope, {
      summaryCounters: {},
      summaryHistograms: {},
      summaryMeters: {},
      summaryTimer: {},
      histogramList:[],
      recordsColor: {
        'Input' :'#1f77b4',
        'Output': '#5cb85c',
        'Error':'#FF3333',
        'Output 1': '#5cb85c',
        'Output 2': '#B2EC5D',
        'Output 3': '#77DD77',
        'Output 4': '#85BB65',
        'Output 5': '#03C03C',
        'Output 6': '#138808',
        'Output 7': '#556B2F',
        'Batch Throughput': '#5cb85c',
        'Total': '#5cb85c',
        'Event': '#ff9f4a'
      },
      stageNameToLabelMap: _.reduce($scope.stageInstances, function(nameToLabelMap, stageInstance){
        nameToLabelMap[stageInstance.instanceName] = stageInstance.uiInfo.label;
        return nameToLabelMap;
      }, {}),
      customStageMeters: undefined,
      customStageTimers: undefined,
      customStageHistograms: undefined,
      customStageGauges: undefined,
      runnerGauges: undefined,
      restServiceAPIEndpoint: undefined,

      getDurationLabel: function(key) {
        switch(key) {
          case '1m':
            return '1 minute';
          case '5m':
            return '5 minute';
          case '15m':
            return '15 minute';
          case '30m':
            return '30 minute';
          case '1h':
            return '1 hour';
          case '6h':
            return '6 hour';
          case '12h':
            return '12 hour';
          case '1d':
            return '1 day';
        }
        return key;
      },

      removeChart: function(chart, index) {
        $rootScope.$storage.summaryPanelList_v1.splice(index, 1);
      },

      dateFormat: function() {
        return function(d){
          var timeRange = $scope.timeRange;
          switch(timeRange) {
            case 'last5m':
            case 'last15m':
            case 'last1h':
            case 'last6h':
            case 'last12h':
              return d3.time.format('%H:%M')(new Date(d));
            case 'last24h':
            case 'last2d':
              return d3.time.format('%m/%d %H:%M')(new Date(d));
            case 'last7d':
            case 'last30d':
              return d3.time.format('%m/%d')(new Date(d));
            default:
              return d3.time.format('%H:%M:%S')(new Date(d));
          }
        };
      },

      formatValue: function() {
        return function(d){
          return $filter('abbreviateNumber')(d);
        };
      }
    });

    if (!$rootScope.$storage.summaryPanelList_v1) {
      $rootScope.$storage.summaryPanelList_v1 = chartList;
    }

    if (!$rootScope.$storage.counters) {
      $rootScope.$storage.counters = {};
    }


    /**
     * Update Summary Tab Data
     */
    var updateSummaryData = function() {
      var timerProperty;
      var pipelineConfig = $scope.pipelineConfig;
      var pipelineMetrics = $scope.detailPaneMetrics;
      var currentSelection = $scope.detailPaneConfig;
      var isStageSelected = $scope.stageSelected;

      // reset all chart values
      $scope.summaryHistograms = undefined;
      $scope.runnerGauges = undefined;
      $scope.summaryMeters = undefined;
      $scope.customStageMeters = undefined;
      $scope.customStageTimers = undefined;
      $scope.customStageHistograms = undefined;
      $scope.customStageGauges = undefined;
      $scope.summaryTimer = undefined;

      if (!pipelineMetrics) {
        return;
      }

      // Workaround for refreshing the graph after data is loaded
      $timeout(function() {
        $scope.summaryDataLoaded = true;
      });

      if (angular.equals({},pipelineMetrics)) {
        return;
      }

      //histogram
      if (isStageSelected && pipelineMetrics.histograms) {
        var inputRecordsHistogram =
          pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.inputRecords.histogramM5'];
        var outputRecordsHistogram =
          pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.outputRecords.histogramM5'];
        var errorRecordsHistogram =
          pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.errorRecords.histogramM5'];
        var errorsHistogram =
          pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.stageErrors.histogramM5'];

        switch(currentSelection.uiInfo.stageType) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            $scope.histogramList = ['outputRecords', 'errorRecords', 'errors'];
            $scope.summaryHistograms = {
              outputRecords: outputRecordsHistogram,
              errorRecords: errorRecordsHistogram,
              errors: errorsHistogram
            };
            break;
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            $scope.histogramList = ['inputRecords', 'outputRecords', 'errorRecords', 'errors'];
            $scope.summaryHistograms = {
              inputRecords: inputRecordsHistogram,
              outputRecords: outputRecordsHistogram,
              errorRecords: errorRecordsHistogram,
              errors: errorsHistogram
            };

            break;
          case pipelineConstant.EXECUTOR_STAGE_TYPE:
            $scope.histogramList = ['inputRecords', 'errorRecords', 'errors'];
            $scope.summaryHistograms = {
              inputRecords: inputRecordsHistogram,
              errorRecords: errorRecordsHistogram,
              errors: errorsHistogram
            };
            break;
          case pipelineConstant.TARGET_STAGE_TYPE:
            $scope.histogramList = ['inputRecords', 'errorRecords', 'errors'];
            $scope.summaryHistograms = {
              inputRecords: inputRecordsHistogram,
              errorRecords: errorRecordsHistogram,
              errors: errorsHistogram
            };
            break;
        }
      } else if (pipelineMetrics && pipelineMetrics.histograms){
        $scope.histogramList = ['inputRecords', 'outputRecords', 'errorRecords', 'errors'];
        $scope.summaryHistograms = {
          inputRecords:
            pipelineMetrics.histograms['pipeline.inputRecordsPerBatch.histogramM5'],

          outputRecords:
            pipelineMetrics.histograms['pipeline.outputRecordsPerBatch.histogramM5'],

          errorRecords:
            pipelineMetrics.histograms['pipeline.errorRecordsPerBatch.histogramM5'],

          errors:
            pipelineMetrics.histograms['pipeline.errorsPerBatch.histogramM5']
        };

      }


      // Counters
      if (pipelineMetrics && pipelineMetrics.counters) {
        var persistedCounters = ['memoryConsumed'];
        var pipelineCounterKey = 'pipeline.' + pipelineConfig.info.pipelineId;
        var currentTime = new Date().getTime();
        var counterValue;

        angular.forEach(persistedCounters, function(persistedCounter) {
          var counters = $rootScope.common.counters[persistedCounter];

          if (!counters) {
            counters = $rootScope.common.counters[persistedCounter] = {};
          }

          //Pipeline
          counterValue = pipelineMetrics.counters['pipeline.memoryConsumed.counter'] ?
            pipelineMetrics.counters['pipeline.memoryConsumed.counter'].count : 0;
          if (!counters[pipelineCounterKey]) {
            counters[pipelineCounterKey] =  [[currentTime, counterValue]];
          } else {
            counters[pipelineCounterKey].push([currentTime, counterValue]);

            if (counters[pipelineCounterKey].length > 500) {
              counters[pipelineCounterKey].splice(0, 1);
            }
          }

          angular.forEach(pipelineConfig.stages, function(stageInst) {
            if (pipelineMetrics.counters['stage.' + stageInst.instanceName + '.memoryConsumed.counter']) {
              counterValue = pipelineMetrics.counters['stage.' + stageInst.instanceName + '.memoryConsumed.counter'].count;

              if (!counters[ stageInst.instanceName ]) {
                counters[ stageInst.instanceName ]=  [[currentTime, counterValue]];
              } else {
                counters[ stageInst.instanceName ].push([currentTime, counterValue]);
                if (counters[ stageInst.instanceName ].length > 500) {
                  counters[ stageInst.instanceName ].splice(0, 1);
                }

              }
            }
          });

        });

        pipelineTracking.trackRunReported(pipelineMetrics, pipelineConfig);
      }

      // Gauges
      if (pipelineMetrics.gauges) {
        if (!isStageSelected) {
          $scope.runnerGauges = [];
          angular.forEach(pipelineMetrics.gauges, function(gaugeObj, gaugeKey) {
            if (gaugeKey.indexOf('runner.') !== -1) {
              var runnerId = gaugeKey
                .replace('runner.', '')
                .replace('.gauge', '')
                .replace(/([A-Z])/g, ' $1');
              $scope.runnerGauges.push({
                gaugeKey: gaugeKey,
                runnerId: runnerId
              });
            }

            if (gaugeKey.indexOf('.restService.') !== -1 && gaugeObj && gaugeObj.value) {
              $scope.restServiceAPIEndpoint = gaugeObj.value.apiEndpoint;
            }
          });
        }
      }

      // meters
      if (pipelineMetrics.meters) {
        if (isStageSelected) {
          $scope.summaryMeters = {
            batchCount:
              pipelineMetrics.meters['pipeline.batchCount.meter'],
            inputRecords:
              pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.inputRecords.meter'],

            outputRecords:
              pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.outputRecords.meter'],

            errorRecords:
              pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter']
          };

          // Custom Meters
          if ($scope.customStageMeters === undefined) {
            $scope.customStageMeters = [];
            angular.forEach(pipelineMetrics.meters, function(meterObj, meterKey) {
              if (meterKey.startsWith('custom.' + currentSelection.instanceName)) {
                var label = meterKey
                  .replace('custom.' + currentSelection.instanceName + '.', '')
                  .replace('.meter', '')
                  .replace(/([A-Z])/g, ' $1')
                  .replace(/^./, function(str){ return str.toUpperCase(); });
                $scope.customStageMeters.push({
                  meterKey: meterKey,
                  label: label
                });
              }
            });
          }

          // Custom Timers
          if ($scope.customStageTimers === undefined) {
            $scope.customStageTimers = [];
            angular.forEach(pipelineMetrics.timers, function(timerObj, timerKey) {
              if (timerKey.startsWith('custom.' + currentSelection.instanceName)) {
                var label = timerKey
                  .replace('custom.' + currentSelection.instanceName + '.', '')
                  .replace('.timer', '')
                  .replace(/([A-Z])/g, ' $1')
                  .replace(/^./, function(str){ return str.toUpperCase(); });
                $scope.customStageTimers.push({
                  timerKey: timerKey,
                  label: label
                });
              }
            });
          }

          // Custom Histograms
          if ($scope.customStageHistograms === undefined) {
            $scope.customStageHistograms = [];
            angular.forEach(pipelineMetrics.histograms, function(histogramObj, histogramKey) {
              if (histogramKey.startsWith('custom.' + currentSelection.instanceName)) {
                var label = histogramKey
                  .replace('custom.' + currentSelection.instanceName + '.', '')
                  .replace('.histogram', '')
                  .replace(/([A-Z])/g, ' $1')
                  .replace(/^./, function(str){ return str.toUpperCase(); });
                $scope.customStageHistograms.push({
                  histogramKey: histogramKey,
                  label: label
                });
              }
            });
          }

          // Custom Gauges
          if ($scope.customStageGauges === undefined) {
            $scope.customStageGauges = [];
            angular.forEach(pipelineMetrics.gauges, function(gaugeObj, gaugeKey) {
              if (gaugeKey.startsWith('custom.' + currentSelection.instanceName)) {
                var label = gaugeKey
                  .replace('custom.' + currentSelection.instanceName + '.', '')
                  .replace('.gauge', '')
                  .replace(/([A-Z])/g, ' $1')
                  .replace(/^./, function(str){ return str.toUpperCase(); });
                $scope.customStageGauges.push({
                  gaugeKey: gaugeKey,
                  label: label
                });
              }
            });
          }
        } else {
          $scope.summaryMeters = {
            batchCount:
              pipelineMetrics.meters['pipeline.batchCount.meter'],
            inputRecords:
              pipelineMetrics.meters['pipeline.batchInputRecords.meter'],

            outputRecords:
              pipelineMetrics.meters['pipeline.batchOutputRecords.meter'],

            errorRecords:
              pipelineMetrics.meters['pipeline.batchErrorRecords.meter']
          };
        }
      }

      // timers
      if (pipelineMetrics.timers) {
        timerProperty = 'pipeline.batchProcessing.timer';
        if (isStageSelected) {
          timerProperty = 'stage.' + currentSelection.instanceName + '.batchProcessing.timer';
        }

        $scope.summaryTimer = pipelineMetrics.timers[timerProperty];
      }

      $scope.$broadcast('summaryDataUpdated');
    };

    $scope.$on('onSelectionChange', function(event, options) {
      if ($scope.detailPaneMetrics &&
        options.type !== pipelineConstant.LINK) {
        $scope.customStageMeters = undefined;
        $scope.customStageTimers = undefined;
        $scope.customStageHistograms = undefined;
        $scope.customStageGauges = undefined;
        $scope.runnerGauges = undefined;
        $scope.restServiceAPIEndpoint = undefined;
        updateSummaryData();
      }
    });

    $scope.$watch('detailPaneMetrics', function() {
      if ($scope.detailPaneMetrics &&
        $scope.selectedType !== pipelineConstant.LINK && !$scope.monitoringPaused) {
        updateSummaryData();
      }
    });

    $scope.$on('launchSummarySettings', function() {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/detail/summary/settings/settingsModal.tpl.html',
        controller: 'SummarySettingsModalInstanceController',
        backdrop: 'static',
        resolve: {
          availableCharts: function () {
            return chartList;
          },
          selectedCharts: function() {
            var selectedChartList = $rootScope.$storage.summaryPanelList;
            return _.filter(chartList, function(chart) {
              return _.find(selectedChartList, function(sChart) {
                return sChart.label === chart.label;
              });
            });
          }
        }
      });

      modalInstance.result.then(function (selectedCharts) {
        $rootScope.$storage.summaryPanelList_v1 = selectedCharts;
      }, function () {

      });
    });

    updateSummaryData();

  })

  .controller('SummarySettingsModalInstanceController', function ($scope, $modalInstance, availableCharts, selectedCharts) {
    angular.extend($scope, {
      showLoading: false,
      common: {
        errors: []
      },
      availableCharts: availableCharts,
      selectedCharts: {
        selected : selectedCharts
      },

      save : function () {
        $modalInstance.close($scope.selectedCharts.selected);
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

    $scope.$broadcast('show-errors-check-validity');
  });

