/**
 * Controller for Library Pane Create Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CreateModalInstanceController', function ($scope, $modalInstance, $translate, api, pipelineService,
                                                         sources, targets, processors) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      sources: sources,
      targets: targets,
      processors: processors,
      selectedSource: '',
      selectedProcessors: {},
      selectedTargets: {},
      newConfig : {
        name: '',
        description: '',
        stages: []
      },

      onProcessorSelect: function($item) {
        var index = _.indexOf($scope.processors, $item),
          itemDuplicate = angular.copy($item);
        $scope.processors.splice(index, 0, itemDuplicate);
      },

      onProcessorRemove: function($item) {
        var index = _.indexOf($scope.processors, $item);
        if(index !== -1) {
          $scope.processors.splice(index, 1);
        }
      },

      onTargetSelect: function($item) {
        var index = _.indexOf($scope.targets, $item),
          itemDuplicate = angular.copy($item);
        $scope.targets.splice(index, 0, itemDuplicate);
      },

      onTargetRemove: function($item) {
        var index = _.indexOf($scope.targets, $item);
        if(index !== -1) {
          $scope.targets.splice(index, 1);
        }
      },

      save : function () {
        if($scope.newConfig.name) {
          api.pipelineAgent.createNewPipelineConfig($scope.newConfig.name, $scope.newConfig.description).
            then(
              function(res) {
                $modalInstance.close(res.data);
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