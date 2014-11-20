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
      deletePipelineConfig: function() {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/delete.tpl.html',
          controller: 'DeleteModalInstanceController',
          size: '',
          backdrop: true,
          resolve: {
            activeConfigInfo: function () {
              return $scope.activeConfigInfo;
            }
          }
        });

        modalInstance.result.then(function (configInfo) {
          var index = _.indexOf($scope.pipelines, _.find($scope.pipelines, function(pipeline){
            return pipeline.name === configInfo.name;
          }));

          $scope.pipelines.splice(index, 1);

          if($scope.pipelines.length) {
            $scope.$emit('onPipelineConfigSelect', $scope.pipelines[0]);
          } else {
            $scope.$emit('onPipelineConfigSelect');
          }


        }, function () {

        });
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function() {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/duplicate.tpl.html',
          controller: 'DuplicateModalInstanceController',
          size: '',
          backdrop: true,
          resolve: {
            originalPipelineConfig: function () {
              return $scope.pipelineConfig;
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

  .controller('DeleteModalInstanceController', function ($scope, $modalInstance, activeConfigInfo, api) {
    angular.extend($scope, {
      issues: [],
      activeConfigInfo: activeConfigInfo,

      yes: function() {
        api.pipelineAgent.deletePipelineConfig(activeConfigInfo.name).
          success(function() {
            $modalInstance.close(activeConfigInfo);
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

  .controller('DuplicateModalInstanceController', function ($scope, $modalInstance, originalPipelineConfig, api, $q) {
    angular.extend($scope, {
      issues: [],
      newConfig : {
        name: originalPipelineConfig.info.name + 'copy',
        description: originalPipelineConfig.description
      },
      save : function () {
        $q.when(api.pipelineAgent.duplicatePipelineConfig($scope.newConfig.name, $scope.newConfig.description,
          originalPipelineConfig)).
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