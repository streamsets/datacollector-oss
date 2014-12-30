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
      }
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

