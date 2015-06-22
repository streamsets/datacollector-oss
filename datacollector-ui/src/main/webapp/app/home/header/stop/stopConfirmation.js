/**
 * Controller for Stop Confirmation Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('StopConfirmationModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      pipelineInfo: pipelineInfo,
      stopping: false,

      yes: function() {
        $scope.stopping = true;
        api.pipelineAgent.stopPipeline().
          success(function(res) {
            $scope.stopping = false;
            $modalInstance.close(res);
          }).
          error(function(data) {
            $scope.stopping = false;
            $scope.common.errors = [data];
          });
      },
      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });