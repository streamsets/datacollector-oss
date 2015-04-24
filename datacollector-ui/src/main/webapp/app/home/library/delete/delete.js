/**
 * Controller for Library Pane Delete Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DeleteModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api) {
    angular.extend($scope, {
      issues: [],
      pipelineInfo: pipelineInfo,

      yes: function() {
        api.pipelineAgent.deletePipelineConfig(pipelineInfo.name).
          success(function() {
            $modalInstance.close(pipelineInfo);
          }).
          error(function(data) {
            $scope.issues = [data];
          });
      },
      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });