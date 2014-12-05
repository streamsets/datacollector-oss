/**
 * Controller for Bad Records Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('BadRecordsController', function ($scope, $rootScope, _, api) {

    angular.extend($scope, {
      errorCount: 0,
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
          $scope.stageBadRecords = res;
        })
        .error(function(data) {
          $rootScope.common.errors = [data];
        });
    };

    $scope.$on('onStageSelection', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        currentSelection = $scope.detailPaneConfig;
      if($scope.isPipelineRunning && pipelineMetrics && pipelineMetrics.meters && currentSelection.instanceName) {
        var errorCount = $scope.errorCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
        $scope.stageBadRecords = [];
        if(errorCount && parseInt(errorCount.count) > 0) {
          updateBadRecordsData(currentSelection);
        }
      }
    });


    $rootScope.$watch('common.pipelineMetrics', function() {
      var pipelineMetrics = $rootScope.common.pipelineMetrics,
        currentSelection = $scope.detailPaneConfig;

      if($scope.isPipelineRunning && pipelineMetrics && pipelineMetrics.meters && currentSelection.instanceName) {
        $scope.errorCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
      }
    });

  });