/**
 * Controller for Shutdown Modal Dialog.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('ShutdownModalInstanceController', function ($scope, $modalInstance, api, $window) {
    angular.extend($scope, {
      issues: [],
      secret: '',
      isShutdownSucceed: false,

      shutdown: function() {
        api.admin.shutdownCollector($scope.secret)
          .success(function() {
            $scope.isShutdownSucceed = true;
          })
          .error(function(data) {
            $scope.issues = [data];
          });
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });