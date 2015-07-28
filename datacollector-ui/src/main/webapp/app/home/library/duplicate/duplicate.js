/**
 * Controller for Library Pane Duplicate Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DuplicateModalInstanceController', function ($rootScope, $scope, $modalInstance, pipelineInfo, api, $q) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      newConfig : {
        name: pipelineInfo.name + 'copy',
        description: pipelineInfo.description
      },
      save : function () {
        $q.when(api.pipelineAgent.duplicatePipelineConfig($scope.newConfig.name, $scope.newConfig.description,
          pipelineInfo)).
          then(function(configObject) {
            $rootScope.common.refreshStatusAndAlertWebSocket();
            $modalInstance.close(configObject);
          },function(res) {
            $scope.common.errors = [res.data];
          });
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });
  });