/**
 * Controller for Shutdown Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('ShutdownModalInstanceController', function ($scope, $modalInstance, api, $window) {
    angular.extend($scope, {
      issues: [],
      isShutdownSucceed: false,

      shutdown: function() {
        api.admin.shutdownCollector()
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