/**
 * Controller for About Modal Dialog.
 */

angular
  .module('pipelineAgentApp')
  .controller('AboutModalInstanceController', function ($scope, $modalInstance, api) {
    angular.extend($scope, {
      buildInfo: {},
      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });

    api.admin.getBuildInfo()
      .success(function(res) {
        $scope.buildInfo = res;
      })
      .error(function(data) {
        $scope.issues = [data];
      });
  });