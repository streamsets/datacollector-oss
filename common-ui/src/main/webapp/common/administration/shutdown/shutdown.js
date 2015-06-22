/**
 * Controller for Shutdown Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('ShutdownModalInstanceController', function ($scope, $modalInstance, api, $window) {
    angular.extend($scope, {
      issues: [],
      isShuttingDown: false,
      isShutdownSucceed: false,

      shutdown: function() {
        $scope.isShuttingDown = true;
        api.admin.shutdownCollector()
          .success(function() {
            $scope.isShutdownSucceed = true;
            $scope.isShuttingDown = false;
          })
          .error(function(data) {
            $scope.issues = [data];
            $scope.isShuttingDown = false;
          });
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });