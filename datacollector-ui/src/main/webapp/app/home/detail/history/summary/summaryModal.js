/**
 * Controller for Snapshots Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('SummaryModalInstanceController', function ($scope, $modalInstance, pipelineConfig, history, prevHistory,
                                                          pipelineConstant) {
    console.log(prevHistory);

    var isStageSelected = false,
      pipelineMetrics = JSON.parse(history.metrics),
      startTime = (prevHistory && prevHistory.state === 'RUNNING') ? prevHistory.lastStatusChange : undefined;

    angular.extend($scope, {
      pipelineStartTime: startTime,
      pipelineStopTime: history.lastStatusChange,
      selectedType: pipelineConstant.PIPELINE,
      summaryMeters: {},
      pipelineConfig: pipelineConfig,
      selectedObject: pipelineConfig,
      common: {
        pipelineMetrics: pipelineMetrics
      },
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
        'Output 7': '#556B2F'
      },

      /**
       * Value format function for D3 NVD3 charts.
       *
       * @returns {Function}
       */
      valueFormatFunction: function() {
        return function(d){
          return d3.format(',d')(d);
        };
      },

      close: function() {
        $modalInstance.dismiss();
      },

      /**
       * Returns label for Detail Pane
       */
      getLabel: function() {
        var selectedType = $scope.selectedType,
          selectedObject = $scope.selectedObject;

        if(selectedObject) {
          switch(selectedType) {
            case pipelineConstant.PIPELINE:
              return selectedObject.info.name;
            case pipelineConstant.STAGE_INSTANCE:
              return selectedObject.uiInfo.label;
            case pipelineConstant.LINK:
              return 'Stream ( ' + selectedObject.source.uiInfo.label + ' - ' + selectedObject.target.uiInfo.label + ' )';
          }
        }
      },

      changeStageSelection: function(options) {
        $scope.selectedObject = options.selectedObject;
        $scope.selectedType = options.type;
        updateSummary();
      }
    });

    var updateSummary = function() {
      var timerProperty,
        currentSelection = $scope.selectedObject,
        isStageSelected = ($scope.selectedType === pipelineConstant.STAGE_INSTANCE);

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


      $scope.$broadcast('summaryDataUpdated');
    };


    updateSummary();

  });