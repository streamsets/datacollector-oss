/**
 * Controller for Library Pane Create Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CreateModalInstanceController', function ($scope, $rootScope, $modalInstance, $translate, api) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      selectedSource: '',
      selectedProcessors: {},
      selectedTargets: {},
      newConfig : {
        name: '',
        description: '',
        stages: []
      },

      save : function () {
        if($scope.newConfig.name) {
          api.pipelineAgent.createNewPipelineConfig($scope.newConfig.name, $scope.newConfig.description).
            then(
              function(res) {
                $modalInstance.close(res.data);
                $rootScope.common.refreshStatusAndAlertWebSocket();
              },
              function(res) {
                $scope.common.errors = [res.data];
              }
            );
        } else {
          $translate('home.library.nameRequiredValidation').then(function(translation) {
            $scope.common.errors = [translation];
          });

        }
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });

  });