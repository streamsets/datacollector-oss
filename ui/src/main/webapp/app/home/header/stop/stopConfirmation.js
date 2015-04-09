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

      yes: function() {
        api.pipelineAgent.stopPipeline().
          success(function(res) {
            $modalInstance.close(res);
          }).
          error(function(data) {
            $scope.common.errors = [data];
          });
      },
      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });