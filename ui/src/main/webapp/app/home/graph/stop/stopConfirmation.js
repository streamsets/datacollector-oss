/**
 * Controller for Stop Confirmation Modal.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('StopConfirmationModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api) {
    angular.extend($scope, {
      issues: [],
      pipelineInfo: pipelineInfo,

      yes: function() {
        api.pipelineAgent.stopPipeline().
          success(function(res) {
            $modalInstance.close(res);
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