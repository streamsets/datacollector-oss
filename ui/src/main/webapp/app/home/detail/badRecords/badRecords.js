/**
 * Controller for Bad Records Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('BadRecordsController', function ($scope, $rootScope, _, api) {

    angular.extend($scope, {
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
      }
    });

    var updateBadRecordsData = function(currentSelection) {
      api.pipelineAgent.getErrorRecords(currentSelection.instanceName)
        .success(function(res) {
          if(res && res.length) {
            $scope.stageBadRecords = res.reverse();
          } else {
            $scope.stageBadRecords = [];
          }
        })
        .error(function(data) {
          $rootScope.common.errors = [data];
        });
    };

    var updateErrorMessagesData = function(currentSelection) {
      api.pipelineAgent.getErrorMessages(currentSelection.instanceName)
        .success(function(res) {
          if(res && res.length) {
            $scope.errorMessages = res.reverse();
          } else {
            $scope.errorMessages = [];
          }
        })
        .error(function(data) {
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
        errorMessagesArr = [];

      if($scope.isPipelineRunning && pipelineMetrics && pipelineMetrics.meters) {
        if(currentSelection.instanceName) {
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
        }
      }
    });

  });