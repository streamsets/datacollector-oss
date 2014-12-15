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
          backdrop: true
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
          backdrop: true,
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
        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/duplicate.tpl.html',
          controller: 'DuplicateModalInstanceController',
          size: '',
          backdrop: true,
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

        $event.stopPropagation();

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
          backdrop: true,
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

        $event.stopPropagation();

        modalInstance.result.then(function () {
          $scope.$emit('onPipelineConfigSelect', pipelineInfo);
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

  })

  .controller('CreateModalInstanceController', function ($scope, $modalInstance, api) {
    angular.extend($scope, {
      issues: [],
      newConfig : {
        name: '',
        description: ''
      },
      save : function () {
        api.pipelineAgent.createNewPipelineConfig($scope.newConfig.name, $scope.newConfig.description).
          success(function(configObject) {
            $modalInstance.close(configObject);
          }).
          error(function(data) {
            $scope.issues = [data];
          });
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