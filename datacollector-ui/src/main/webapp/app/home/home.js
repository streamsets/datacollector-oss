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
      importPipelineConfig: function() {
        pipelineService.importPipelineConfigCommand();
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.deletePipelineConfigCommand(pipelineInfo, $event).then(function(res) {
          $scope.pipelines = res;
        });
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.duplicatePipelineConfigCommand(pipelineInfo, $event);
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
      api.pipelineAgent.getPipelineStatus(),
      pipelineService.init()
    ])
    .then(
      function (results) {
        var pipelineStatus = results[0].data,
          pipelines = pipelineService.getPipelines(),
          activeConfigInfo;

        /*if($rootScope.$storage.activeConfigInfo && $rootScope.$storage.activeConfigInfo.name) {
          var localStorageConfigInfoName = $rootScope.$storage.activeConfigInfo.name;
          activeConfigInfo = _.find(pipelines, function(pipelineConfigInfo) {
            return pipelineConfigInfo.name === localStorageConfigInfoName;
          });
        } else if(pipelineStatus && pipelineStatus.name) {
          activeConfigInfo = _.find(pipelines, function(pipelineConfigInfo) {
            return pipelineConfigInfo.name === pipelineStatus.name;
          });
        }

        if(!activeConfigInfo && pipelines && pipelines.length) {
          activeConfigInfo =   pipelines[0];
        }

        if(activeConfigInfo) {
          //$location.path('/collector/pipeline/' + activeConfigInfo.name);
          //$location.replace();
        } else {
          $scope.loaded = true;
        }*/

        $scope.loaded = true;
        $rootScope.common.pipelineStatus = pipelineStatus;
        $scope.pipelines = pipelineService.getPipelines();
      },
      function (results) {
        $scope.loaded = true;
      }
    );



  });