/**
 * Controller for Library Pane Delete Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ResetOffsetModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api) {
    angular.extend($scope, {
      showLoading: false,
      isOffsetResetSucceed: false,
      common: {
        errors: []
      },
      pipelineInfo: pipelineInfo,

      /**
       * Callback function Yes button
       */
      yes: function() {
        $scope.showLoading = true;
        api.pipelineAgent.resetOffset(pipelineInfo.name).
          success(function() {
            $scope.showLoading = false;
            $scope.isOffsetResetSucceed = true;
          }).
          error(function(data) {
            $scope.showLoading = false;
            $scope.common.errors = [data];
          });
      },

      /**
       * Callback function for No button
       */
      no: function() {
        $modalInstance.dismiss('cancel');
      },

      /**
       * Callback function for Close button
       */
      close: function() {
        $modalInstance.close(pipelineInfo);
      }
    });
  });