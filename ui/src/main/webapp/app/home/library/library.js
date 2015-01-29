/**
 * Controller for Library Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('LibraryController', function ($scope, $rootScope, $modal, _, api) {

    angular.extend($scope, {

      /**
       * Emit 'onPipelineConfigSelect' event when new configuration is selected in library panel.
       *
       * @param pipeline
       */
      onSelect : function(pipeline) {
        $rootScope.$broadcast('onPipelineConfigSelect', pipeline);
      },

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/create/create.tpl.html',
          controller: 'CreateModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            sources: function () {
              return angular.copy($scope.sources);
            },
            processors: function () {
              return angular.copy($scope.processors).concat($scope.selectorProcessors);
            },
            targets: function () {
              return angular.copy($scope.targets);
            }
          }
        });

        modalInstance.result.then(function (configObject) {
          var index = _.sortedIndex($scope.pipelines, configObject.info, function(obj) {
            return obj.name.toLowerCase();
          });

          $scope.pipelines.splice(index, 0, configObject.info);
          $rootScope.$broadcast('onPipelineConfigSelect', configObject.info);

        }, function () {

        });
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/library/delete/delete.tpl.html',
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
          templateUrl: 'app/home/library/duplicate/duplicate.tpl.html',
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

  });