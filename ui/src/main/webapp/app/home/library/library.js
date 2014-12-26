/**
 * Controller for Library Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('LibraryController', function ($scope, $modal, _, api) {

    angular.extend($scope, {

      /**
       * Emit 'onPipelineConfigSelect' event when new configuration is selected in library panel.
       *
       * @param pipeline
       */
      onSelect : function(pipeline) {
        $scope.$emit('onPipelineConfigSelect', pipeline);
      },

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/create.tpl.html',
          controller: 'CreateModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            sources: function () {
              return $scope.sources;
            },
            processors: function () {
              return $scope.processors;
            },
            targets: function () {
              return $scope.targets;
            }
          }
        });

        modalInstance.result.then(function (configObject) {
          var index = _.sortedIndex($scope.pipelines, configObject.info, function(obj) {
            return obj.name.toLowerCase();
          });

          $scope.pipelines.splice(index, 0, configObject.info);
          $scope.$emit('onPipelineConfigSelect', configObject.info);

        }, function () {

        });
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/delete.tpl.html',
          controller: 'DeleteModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

        $event.stopPropagation();
        modalInstance.result.then(function (configInfo) {
          var index = _.indexOf($scope.pipelines, _.find($scope.pipelines, function(pipeline){
            return pipeline.name === configInfo.name;
          }));

          $scope.pipelines.splice(index, 1);


          if(pipelineInfo.name === $scope.activeConfigInfo.name) {
            if($scope.pipelines.length) {
              $scope.$emit('onPipelineConfigSelect', $scope.pipelines[0]);
            } else {
              $scope.$emit('onPipelineConfigSelect');
            }
          }

        }, function () {

        });
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        $event.stopPropagation();

        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/duplicate.tpl.html',
          controller: 'DuplicateModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

        modalInstance.result.then(function (configObject) {
          var index = _.sortedIndex($scope.pipelines, configObject.info, function(obj) {
            return obj.name.toLowerCase();
          });

          $scope.pipelines.splice(index, 0, configObject.info);
          $scope.$emit('onPipelineConfigSelect', configObject.info);

        }, function () {

        });

      },

      /**
       * Import link command handler
       */
      importPipelineConfig: function(pipelineInfo, $event) {
        var modalInstance = $modal.open({
          templateUrl: 'importModalContent.html',
          controller: 'ImportModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

        if($event) {
          $event.stopPropagation();
        }

        modalInstance.result.then(function (configObject) {

          if(configObject) {
            //In case of new object created.
            $scope.pipelines.push(configObject.info);
            $scope.$emit('onPipelineConfigSelect', configObject.info);
          } else {
            //In case of current object replaced.
            $scope.$emit('onPipelineConfigSelect', pipelineInfo);
          }

        }, function () {

        });
      },

      /**
       * Export link command handler
       */
      exportPipelineConfig: function(pipelineInfo, $event) {
        $event.stopPropagation();
        api.pipelineAgent.exportPipelineConfig(pipelineInfo.name);
      }

    });


    $scope.$on('addPipelineConfig', function() {
      $scope.addPipelineConfig();
    });

    $scope.$on('importPipelineConfig', function() {
      $scope.importPipelineConfig();
    });

  })

  .controller('CreateModalInstanceController', function ($scope, $modalInstance, $translate, api, pipelineService,
                                                         sources, targets, processors) {
    angular.extend($scope, {
      issues: [],
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
      save : function () {
        if($scope.newConfig.name) {
          api.pipelineAgent.createNewPipelineConfig($scope.newConfig.name, $scope.newConfig.description).
            then(
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
                  angular.forEach(selectedProcessors, function(stage) {
                    newPipelineObject.stages.push(pipelineService.getNewStageInstance(stage,
                      newPipelineObject));
                  });
                }

                if(selectedTargets && selectedTargets.length) {
                  angular.forEach(selectedTargets, function(stage) {
                    newPipelineObject.stages.push(pipelineService.getNewStageInstance(stage,
                      newPipelineObject));
                  });
                }

                return api.pipelineAgent.savePipelineConfig($scope.newConfig.name, newPipelineObject);
              },
              function(res) {
                $scope.issues = [res.data];
              }
            ).then(
              function(res) {
                $modalInstance.close(res.data);
              },
              function(res) {
                $scope.issues = [res.data];
              }
            );
        } else {
          $translate('home.library.nameRequiredValidation').then(function(translation) {
            $scope.issues = [translation];
          });

        }
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });
  })

  .controller('DeleteModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api) {
    angular.extend($scope, {
      issues: [],
      pipelineInfo: pipelineInfo,

      yes: function() {
        api.pipelineAgent.deletePipelineConfig(pipelineInfo.name).
          success(function() {
            $modalInstance.close(pipelineInfo);
          }).
          error(function(data) {
            $scope.issues = [data];
          });
      },
      no: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  })

  .controller('DuplicateModalInstanceController', function ($scope, $modalInstance, pipelineInfo, api, $q) {
    angular.extend($scope, {
      issues: [],
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
            $scope.issues = [data];
          });
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      }
    });
  });