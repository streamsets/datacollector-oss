/**
 * Controller for Bad Records Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('BadRecordsController', function ($scope, $rootScope, _, api) {

    angular.extend($scope, {
      showBadRecordsLoading: false,
      showErrorMessagesLoading: false,
      badRecordsChartData: [],
      errorMessagesChartData: [],
      errorMessagesCount: 0,
      errorMessages: [],
      errorRecordsCount: 0,
      expandAllErrorData: false,
      stageBadRecords:[],

      onExpandAllErrorData: function() {
        $scope.expandAllErrorData = true;
      },

      onCollapseAllErrorData: function() {
        $scope.expandAllErrorData = false;
      },

      refreshBadRecordsData: function() {
        var currentSelection = $scope.detailPaneConfig;
        updateBadRecordsData(currentSelection);
      },

      refreshErrorMessagesData: function() {
        var currentSelection = $scope.detailPaneConfig;
        updateErrorMessagesData(currentSelection);

      },

      getYAxisLabel: function() {
        return function() {
          return '';
        };
      }
    });

    var updateBadRecordsData = function(currentSelection) {
      $scope.showBadRecordsLoading = true;
      api.pipelineAgent.getErrorRecords(currentSelection.instanceName)
        .success(function(res) {
          $scope.showBadRecordsLoading = false;
          if(res && res.length) {
            $scope.stageBadRecords = res.reverse();
          } else {
            $scope.showBadRecordsLoading = [];
          }
        })
        .error(function(data) {
          $scope.showBadRecordsLoading = false;
          $rootScope.common.errors = [data];
        });
    };

    var updateErrorMessagesData = function(currentSelection) {
      $scope.showErrorMessagesLoading = true;
      api.pipelineAgent.getErrorMessages(currentSelection.instanceName)
        .success(function(res) {
          $scope.showErrorMessagesLoading = false;
          if(res && res.length) {
            $scope.errorMessages = res.reverse();
          } else {
            $scope.errorMessages = [];
          }
        })
        .error(function(data) {
          $scope.showErrorMessagesLoading = false;
          $rootScope.common.errors = [data];
        });
    };

    $scope.$on('onStageSelection', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        currentSelection = $scope.detailPaneConfig;
      if($scope.isPipelineRunning && pipelineMetrics && pipelineMetrics.meters) {

        if(currentSelection.instanceName) {  //Stage Instance
          //Bad Records
          var errorRecordsCount = $scope.errorRecordsCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
          $scope.stageBadRecords = [];
          if(errorRecordsCount && parseInt(errorRecordsCount.count) > 0) {
            updateBadRecordsData(currentSelection);
          }

          //Error Messages
          var errorMessagesCount = $scope.errorMessagesCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.stageErrors.meter'];
          $scope.errorMessages = [];
          if(errorMessagesCount && parseInt(errorMessagesCount.count) > 0) {
            updateErrorMessagesData(currentSelection);
          }


          $scope.errorRecordsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.errorRecords.histogramM5'];
          $scope.errorsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.stageErrors.histogramM5'];

        } else {  //Pipeline
          $scope.errorRecordsCount = pipelineMetrics.meters['pipeline.batchErrorRecords.meter'];
          $scope.stageBadRecords = [];

          $scope.errorMessagesCount = pipelineMetrics.meters['pipeline.batchErrorMessages.meter'];
          $scope.errorMessages = [];
        }
      }
    });


    $rootScope.$watch('common.pipelineMetrics', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        currentSelection = $scope.detailPaneConfig,
        stages = $scope.pipelineConfig.stages,
        badRecordsArr = [],
        errorMessagesArr = [],
        errorRecordsHistogram,
        errorsHistogram;

      if($scope.isPipelineRunning && pipelineMetrics && pipelineMetrics.meters) {
        if(currentSelection.instanceName) {
          errorRecordsHistogram = $scope.errorRecordsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.errorRecords.histogramM5'];
          errorsHistogram = $scope.errorsHistogram = pipelineMetrics.histograms['stage.' + currentSelection.instanceName + '.stageErrors.histogramM5'];

          $scope.errorRecordsCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
          $scope.errorMessagesCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.stageErrors.meter'];
        } else {

          angular.forEach(stages, function(stage) {
            badRecordsArr.push([stage.uiInfo.label,
              pipelineMetrics.meters['stage.' + stage.instanceName + '.errorRecords.meter'].count
            ]);

            errorMessagesArr.push([stage.uiInfo.label,
              pipelineMetrics.meters['stage.' + stage.instanceName + '.stageErrors.meter'].count
            ]);
          });

          $scope.badRecordsChartData = [{
            key: "Bad Records",
            values: badRecordsArr
          }];

          $scope.errorMessagesChartData = [{
            key: "Error Messages",
            values: errorMessagesArr
          }];

          errorRecordsHistogram = pipelineMetrics.histograms['pipeline.errorRecordsPerBatch.histogramM5'];
          errorsHistogram = pipelineMetrics.histograms['pipeline.errorsPerBatch.histogramM5'];
        }

        $scope.errorRecordsPercentilesData = [
          {
            key: 'Error Records',
            values: [
              ["Mean" , errorRecordsHistogram.mean ],
              ["Std Dev" , errorRecordsHistogram.stddev ],
              ["99.9%" , errorRecordsHistogram.p999 ],
              ["99%" , errorRecordsHistogram.p99 ],
              ["98%" , errorRecordsHistogram.p98 ],
              ["95%" , errorRecordsHistogram.p95 ],
              ["75%" , errorRecordsHistogram.p75 ],
              ["50%" , errorRecordsHistogram.p50 ]
            ],
            color: '#FF3333'
          }
        ];

        $scope.errorsPercentilesData = [
          {
            key: 'Stage Errors',
            values: [
              ["Mean" , errorsHistogram.mean ],
              ["Std Dev" , errorsHistogram.stddev ],
              ["99.9%" , errorsHistogram.p999 ],
              ["99%" , errorsHistogram.p99 ],
              ["98%" , errorsHistogram.p98 ],
              ["95%" , errorsHistogram.p95 ],
              ["75%" , errorsHistogram.p75 ],
              ["50%" , errorsHistogram.p50 ]
            ],
            color: '#d62728'
          }
        ];

      }
    });

  });