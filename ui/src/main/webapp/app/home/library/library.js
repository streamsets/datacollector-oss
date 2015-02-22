/**
 * Controller for Library Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('LibraryController', function ($scope, $rootScope,  $route, $location, $modal, _, api, pipelineService) {

    angular.extend($scope, {

      /**
       * Emit 'onPipelineConfigSelect' event when new configuration is selected in library panel.
       *
       * @param pipeline
       */
      onSelect : function(pipeline) {
        //$rootScope.$broadcast('onPipelineConfigSelect', pipeline);
        $location.path('/collector/pipeline/' + pipeline.name);
      },

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        pipelineService.addPipelineConfigCommand();
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
          pipelineService.removePipeline(configInfo);

          if(pipelineInfo.name === $scope.activeConfigInfo.name) {
            if($scope.pipelines.length) {
              $location.path('/collector/pipeline/' + $scope.pipelines[0].name);
            } else {
              $location.path('/');
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
          pipelineService.addPipeline(configObject);
          $location.path('/collector/pipeline/' + configObject.info.name);
        }, function () {

        });
      },

      /**
       * Import link command handler
       */
      importPipelineConfig: function(pipelineInfo, $event) {
        pipelineService.importPipelineConfigCommand(pipelineInfo, $event);
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