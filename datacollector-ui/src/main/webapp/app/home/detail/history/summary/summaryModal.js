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
 * Controller for History Summary Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('SummaryModalInstanceController', function ($scope, $modalInstance, pipelineConfig, history, prevHistory,
                                                          pipelineConstant) {

    var pipelineMetrics = JSON.parse(history.metrics),
      startTime = (prevHistory && prevHistory.status === 'STARTING') ? prevHistory.timeStamp : undefined;

    angular.extend($scope, {
      pipelineStartTime: startTime,
      pipelineStopTime: history.timeStamp,
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
        'Output 7': '#556B2F',
        'Event': '#ff9f4a'
      },
      timeRange: 'latest',

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

        if (selectedObject) {
          switch(selectedType) {
            case pipelineConstant.PIPELINE:
              return selectedObject.info.title;
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

    var updateSummary = function() {
      var currentSelection = $scope.selectedObject,
        isStageSelected = ($scope.selectedType === pipelineConstant.STAGE_INSTANCE);

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


    if (pipelineMetrics && pipelineMetrics.meters) {
      updateSummary();
    }
  });
