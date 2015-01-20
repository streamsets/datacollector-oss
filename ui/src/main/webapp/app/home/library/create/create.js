/**
 * Controller for Library Pane Create Modal.
 */

angular
  .module('pipelineAgentApp.home')
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
            /*then(
              function(res) {
                var newPipelineObject = res.data,
                  selectedSource = $scope.selectedSource,
                  selectedProcessors = $scope.selectedProcessors.selected,
                  selectedTargets = $scope.selectedTargets.selected;

                newPipelineObject.stages = [];

                if(selectedSource) {
                  newPipelineObject.stages.push(pipelineService.getNewStageInstance(selectedSource,
                    newPipelineObject));
                }

                if(selectedProcessors && selectedProcessors.length) {
                  angular.forEach(selectedProcessors, function(stage, index) {
                    newPipelineObject.stages.push(pipelineService.getNewStageInstance(stage,
                      newPipelineObject, index));
                  });
                }

                if(selectedTargets && selectedTargets.length) {
                  angular.forEach(selectedTargets, function(stage, index) {
                    newPipelineObject.stages.push(pipelineService.getNewStageInstance(stage,
                      newPipelineObject, index));
                  });
                }

                return api.pipelineAgent.savePipelineConfig($scope.newConfig.name, newPipelineObject);
              },
              function(res) {
                $scope.issues = [res.data];
              }
            ).*/
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