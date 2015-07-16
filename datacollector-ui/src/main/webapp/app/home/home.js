/**
 * Home module for displaying home page content.
 */

angular
  .module('dataCollectorApp.home')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'app/home/home.tpl.html',
        controller: 'HomeController',
        resolve: {
          myVar: function(authService) {
            return authService.init();
          }
        },
        data: {
          authorizedRoles: ['admin', 'creator', 'manager', 'guest']
        }
      });
  }])
  .controller('HomeController', function ($scope, $rootScope, $q, $modal, $location, pipelineService, api) {
    $location.search('auth_token', null);
    $location.search('auth_user', null);

    angular.extend($scope, {
      loaded: false,
      pipelines: [],
      sortColumn: 'lastModified',
      sortReverse: true,

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        pipelineService.addPipelineConfigCommand();
      },

      /**
       * Import Pipeline Configuration
       */
      importPipelineConfig: function(pipelineInfo, $event) {
        pipelineService.importPipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.deletePipelineConfigCommand(pipelineInfo, $event)
          .then(function(pipelines) {
            $scope.pipelines = pipelines;
          });
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.duplicatePipelineConfigCommand(pipelineInfo, $event)
          .then(function(pipelines) {
            $scope.pipelines = pipelineService.getPipelines();
          });
      },

      /**
       * Export link command handler
       */
      exportPipelineConfig: function(pipelineInfo, $event) {
        $event.stopPropagation();
        api.pipelineAgent.exportPipelineConfig(pipelineInfo.name);
      },

      /**
       * Open pipeline
       * @param pipeline
       */
      openPipeline: function(pipeline) {
        $location.path('/collector/pipeline/' + pipeline.name);
      }
    });

    $q.all([
      api.pipelineAgent.getAllPipelineStatus(),
      pipelineService.init()
    ])
    .then(
      function (results) {
        $scope.loaded = true;
        $rootScope.common.pipelineStatusMap = results[0].data;
        $scope.pipelines = pipelineService.getPipelines();
      },
      function (results) {
        $scope.loaded = true;
      }
    );



  });