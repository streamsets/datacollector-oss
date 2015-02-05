/**
 * Controller for Rules tab.
 */

angular
  .module('dataCollectorApp.home')

  .controller('RulesController', function ($scope, pipelineConstant, pipelineService) {
    angular.extend($scope, {
      showLoading: false,

      /**
       * Refresh Rules
       */
      refreshRules: function() {
        updateRules($scope.activeConfigInfo.name);
      },

      /**
       * Callback function for Create New Alert Rule button.
       *
       */
      createAlertRule: function() {
        if($scope.selectedType === pipelineConstant.LINK) {
          var edge =  $scope.selectedObject,
            newAlertDefn = {
              id: edge.outputLane + (new Date()).getTime(),
              label: '',
              lane: edge.outputLane,
              predicate: '',
              thresholdType: 'COUNT',
              thresholdValue: null,
              minVolume: null,
              enabled: false
            };

          $scope.pipelineRules.alertDefinitions.push(newAlertDefn);
        }
      },

      /**
       * Callback function for Create New Metric Rule button.
       */
      createMetricRule: function() {
        if($scope.selectedType === pipelineConstant.LINK) {
          var edge =  $scope.selectedObject,
            newMetricDefn = {
              id: edge.outputLane + (new Date()).getTime(),
              label: '',
              metricGroup: '',
              metricType: 'METER',
              lane: edge.outputLane,
              condition: '',
              enabled: false
            };

          $scope.pipelineRules.metricsAlertDefinitions.push(newMetricDefn);
        }
      },

      /**
       * Callback function for Create New Sampling Rule button.
       */
      createSamplingRule: function() {
        if($scope.selectedType === pipelineConstant.LINK) {
          var edge =  $scope.selectedObject,
            newSamplingDefn = {
              id: edge.outputLane + (new Date()).getTime(),
              label: '',
              lane: edge.outputLane,
              predicate: '',
              samplingPercentage: '',
              enabled: false
            };

          $scope.pipelineRules.samplingDefinitions.push(newSamplingDefn);
        }
      },

      /**
       * Callback function for Create New Metric Alert Rule button.
       */
      createMetricAlertRule: function() {
        if($scope.selectedType !== pipelineConstant.LINK) {
          var selectedObject =  $scope.selectedObject,
            id = selectedObject.info.name + (new Date()).getTime(),
            newMetricAlertDefn = {
              id: id,
              label: '',
              condition: '',
              metricId: null,
              metricType: 'COUNTER',
              metricElement: null,
              enabled: false
            };

          $scope.pipelineRules.metricsAlertDefinitions.push(newMetricAlertDefn);
        }
      },

      /**
       * Remove Callback function
       *
       * @param ruleList
       * @param $index
       */
      removeRule: function(ruleList, $index) {
        ruleList.splice($index, 1);
      }
    });

    $scope.metricElementList = pipelineService.getMetricElementList();


    function updateMetricIDList() {
      if($scope.pipelineConfig) {
        $scope.metricIDList = pipelineService.getMetricIDList($scope.pipelineConfig);
      }
    }

    $scope.$on('onSelectionChange', function(event, options) {
      updateMetricIDList();
    });

    updateMetricIDList();

  });