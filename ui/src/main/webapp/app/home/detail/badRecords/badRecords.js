/**
 * Controller for Bad Records Tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('BadRecordsController', function ($scope, $rootScope, _, api) {

    angular.extend($scope, {
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
      if($scope.isPipelineRunning && pipelineMetrics && currentSelection.instanceName) {
        var errorCount = pipelineMetrics.meters['stage.' + currentSelection.instanceName + '.errorRecords.meter'];
        $scope.stageBadRecords = [];
        if(errorCount && parseInt(errorCount.count) > 0) {
          updateBadRecordsData(currentSelection);
        }
      }
    });
  });