/**
 * Controller for Bad Records Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('BadRecordsController', function ($scope, $rootScope, _, api) {

    angular.extend($scope, {
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
      if($scope.isPipelineRunning && pipelineMetrics && pipelineMetrics.meters && currentSelection.instanceName) {

        //Bad Records
        var errorRecordsCount = $scope.errorRecordsCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
        $scope.stageBadRecords = [];
        if(errorRecordsCount && parseInt(errorRecordsCount.count) > 0) {
          updateBadRecordsData(currentSelection);
        }

        //Error Messages
        var errorMessagesCount = $scope.errorMessagesCount = pipelineMetrics.meters['pipeline.batchErrorMessages.meter'];
        $scope.errorMessages = [];
        if(errorMessagesCount && parseInt(errorMessagesCount.count) > 0) {
          updateErrorMessagesData(currentSelection);
        }
      }
    });


    $rootScope.$watch('common.pipelineMetrics', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        currentSelection = $scope.detailPaneConfig;

      if($scope.isPipelineRunning && pipelineMetrics && pipelineMetrics.meters) {

        if(currentSelection.instanceName) {
          $scope.errorRecordsCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
        }

        $scope.errorMessagesCount = pipelineMetrics.meters['pipeline.batchErrorMessages.meter'];
      }
    });

  });