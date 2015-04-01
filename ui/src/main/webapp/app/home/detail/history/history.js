/**
 * Controller for History.
 */

angular
  .module('dataCollectorApp.home')
  .controller('HistoryController', function ($rootScope, $scope, _, api, $modal) {

    angular.extend($scope, {
      showLoading: false,
      runHistory: [],

      refreshHistory: function() {
        updateHistory($scope.activeConfigInfo.name);
      },

      viewSummary: function(history) {
        $modal.open({
          templateUrl: 'app/home/detail/history/summary/summaryModal.tpl.html',
          controller: 'SummaryModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            pipelineConfig: function() {
              return $scope.pipelineConfig;
            },
            history: function () {
              return history;
            }
          }
        });
      }
    });

    var updateHistory = function(pipelineName) {
      $scope.showLoading = true;
      api.pipelineAgent.getHistory(pipelineName).
        success(function(res) {
          if(res && res.length) {
            $scope.runHistory = res;
          } else {
            $scope.runHistory = [];
          }
          $scope.showLoading = false;
        }).
        error(function(data) {
          $scope.showLoading = false;
          $rootScope.common.errors = [data];
        });
    };

    $scope.$on('onPipelineConfigSelect', function(event, configInfo) {
      if(configInfo) {
        updateHistory(configInfo.name);
      }
    });

    $scope.$watch('isPipelineRunning', function(newValue) {
      if($scope.pipelineConfig) {
        updateHistory($scope.pipelineConfig.info.name);
      }
    });

  });