/**
 * Controller for Library Pane Duplicate Modal.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('DuplicateModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api, $q) {
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
            $modalInstance.close(configObject);
          },function(data) {
            $scope.common.errors = [data];
          });
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });
  });