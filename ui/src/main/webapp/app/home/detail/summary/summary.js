/**
 * Controller for Summary Tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('SummaryController', function ($scope, $rootScope, $modal, pipelineConstant) {
    var chartList = [
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
        label: 'global.form.histogram',
        templateId: 'summaryRecordHistogramTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.batchProcessingTimer',
        templateId: 'summaryRecordsProcessedTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.runtimeStatistics',
        templateId: 'summaryRuntimeStatisticsTemplate'
      },
      {
        label: 'home.detailPane.summaryTab.memoryConsumed',
        templateId: 'memoryConsumedLineChartTemplate'
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
        'Bad':'#FF3333',
        'Output 1': '#5cb85c',
        'Output 2': '#B2EC5D',
        'Output 3': '#77DD77',
        'Output 4': '#85BB65',
        'Output 5': '#03C03C',
        'Output 6': '#138808',
        'Output 7': '#556B2F'
      },
      stageNameToLabelMap: _.reduce($scope.pipelineConfig.stages, function(nameToLabelMap, stageInstance){
        nameToLabelMap[stageInstance.instanceName] = stageInstance.uiInfo.label;
        return nameToLabelMap;
      }, {}),

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
        $rootScope.$storage.summaryChartList.splice(index, 1);
      }
    });

    if(!$rootScope.$storage.summaryChartList) {
      $rootScope.$storage.summaryChartList = chartList;
    }

    if(!$rootScope.$storage.counters) {
      $rootScope.$storage.counters = {};
    }


    /**
     * Update Summary Tab Data
     */
    var updateSummaryData = function() {
      var timerProperty,
        pipelineConfig = $scope.pipelineConfig,
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        currentSelection = $scope.detailPaneConfig,
        isStageSelected = $scope.stageSelected;

      if(angular.equals({},pipelineMetrics)) {
        return;
      }

      //histogram
      if(isStageSelected) {
        var inputRecordsHistogram =
            pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.inputRecords.histogramM5'],
          outputRecordsHistogram=
            pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.outputRecords.histogramM5'],
          errorRecordsHistogram=
            pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.errorRecords.histogramM5'],
          errorsHistogram =
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
          case pipelineConstant.TARGET_STAGE_TYPE:
            $scope.histogramList = ['inputRecords', 'errorRecords', 'errors'];
            $scope.summaryHistograms = {
              inputRecords: inputRecordsHistogram,
              errorRecords: errorRecordsHistogram,
              errors: errorsHistogram
            };
            break;
        }
      } else if(pipelineMetrics && pipelineMetrics.histograms){
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



      var persistedCounters = ['memoryConsumed'],
        pipelineCounterKey = 'pipeline.' + pipelineConfig.info.name,
        currentTime = new Date().getTime(),
        counterValue;

      angular.forEach(persistedCounters, function(persistedCounter) {
        var counters = $rootScope.common.counters[persistedCounter];

        if(!counters) {
          counters = $rootScope.common.counters[persistedCounter] = {};
        }

        //Pipeline
        counterValue = pipelineMetrics.counters['pipeline.memoryConsumed.counter'] ?
          pipelineMetrics.counters['pipeline.memoryConsumed.counter'].count : 0;
        if(!counters[pipelineCounterKey]) {
          counters[pipelineCounterKey] =  [[currentTime, counterValue]];
        } else {
          counters[pipelineCounterKey].push([currentTime, counterValue]);

          if (counters[pipelineCounterKey].length > 500) {
            counters[pipelineCounterKey].splice(0, 1);
          }
        }

        angular.forEach(pipelineConfig.stages, function(stageInst) {
          counterValue = pipelineMetrics.counters['stage.' + stageInst.instanceName + '.memoryConsumed.counter'].count;

          if(!counters[ stageInst.instanceName ]) {
            counters[ stageInst.instanceName ]=  [[currentTime, counterValue]];
          } else {
            counters[ stageInst.instanceName ].push([currentTime, counterValue]);
            if (counters[ stageInst.instanceName ].length > 500) {
              counters[ stageInst.instanceName ].splice(0, 1);
            }

          }
        });

      });


      //meters
      if(isStageSelected) {
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

      //timers
      timerProperty = 'pipeline.batchProcessing.timer';
      if(isStageSelected) {
        timerProperty = 'stage.' + currentSelection.instanceName + '.batchProcessing.timer';
      }

      $scope.summaryTimer = pipelineMetrics.timers[timerProperty];

      $scope.$broadcast('summaryDataUpdated');
    };

    $scope.$on('onSelectionChange', function(event, options) {
      if($scope.isPipelineRunning &&
        $rootScope.common.pipelineMetrics &&
        options.type !== pipelineConstant.LINK) {
        updateSummaryData();
      }
    });

    $rootScope.$watch('common.pipelineMetrics', function() {
      if($scope.isPipelineRunning &&
        $rootScope.common.pipelineMetrics &&
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
            var selectedChartList = $rootScope.$storage.summaryChartList;
            return _.filter(chartList, function(chart) {
              return _.find(selectedChartList, function(sChart) {
                return sChart.label === chart.label;
              });
            });
          }
        }
      });

      modalInstance.result.then(function (selectedCharts) {
        $rootScope.$storage.summaryChartList = selectedCharts;
      }, function () {

      });
    });

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

