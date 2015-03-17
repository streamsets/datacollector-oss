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
    angular.extend($scope, {
      loaded: false,

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

        if($rootScope.$storage.activeConfigInfo && $rootScope.$storage.activeConfigInfo.name) {
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
          $location.path('/collector/pipeline/' + activeConfigInfo.name);
          $location.replace();
        } else {
          $scope.loaded = true;
        }
      },
      function (results) {
        $scope.loaded = true;
      }
    );



  });