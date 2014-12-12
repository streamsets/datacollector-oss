/**
 * Controller for Summary Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('SummaryController', function ($scope, $rootScope, pipelineConstant) {
    angular.extend($scope, {
      summaryCounters: {},
      summaryHistograms: {},
      summaryMeters: {},
      summaryTimer: {},
      histogramList:[]
    });

    /**
     * Update Summary Tab Data
     */
    var updateSummaryData = function() {
      var timerProperty,
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        currentSelection = $scope.detailPaneConfig,
        isStageSelected = $scope.stageSelected;

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
      } else {
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

    $scope.$on('onStageSelection', function() {
      if($scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {
        updateSummaryData();
      }
    });

    $rootScope.$watch('common.pipelineMetrics', function() {
      if($scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {
        updateSummaryData();
      }
    });

  });

