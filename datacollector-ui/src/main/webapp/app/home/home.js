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
  .controller('HomeController', function ($scope, $rootScope, $routeParams, $q, $modal, $location, pipelineService, api,
                                          pipelineConstant) {

    $location.search('auth_token', null);
    $location.search('auth_user', null);

    if($routeParams.errors) {
      $rootScope.common.errors = [$routeParams.errors];
      //$location.search('errors', null);
    } else {
      $rootScope.common.errors = [];
    }


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
      },

      /**
       * On Start Pipeline button click.
       *
       */
      startPipeline: function(pipelineInfo, $event) {
        if($event) {
          $event.stopPropagation();
        }

        $rootScope.common.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Start Pipeline', 1);
        if($rootScope.common.pipelineStatusMap[pipelineInfo.name].state !== 'RUNNING') {
          api.pipelineAgent.startPipeline(pipelineInfo.name, 0).
            then(
            function (res) {
              $rootScope.common.pipelineStatusMap[pipelineInfo.name] = res.data;
            },
            function (res) {
              $rootScope.common.errors = [res.data];
            }
          );
        } else {
          $translate('home.graphPane.startErrorMessage', {
            name: pipelineInfo.name
          }).then(function(translation) {
            $rootScope.common.errors = [translation];
          });
        }
      },

      /**
       * On Stop Pipeline button click.
       *
       */
      stopPipeline: function(pipelineInfo, $event) {
        if($event) {
          $event.stopPropagation();
        }

        $rootScope.common.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Stop Pipeline', 1);
        var modalInstance = $modal.open({
          templateUrl: 'app/home/header/stop/stopConfirmation.tpl.html',
          controller: 'StopConfirmationModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return pipelineInfo;
            }
          }
        });

        modalInstance.result.then(function(status) {
          $rootScope.common.pipelineStatusMap[pipelineInfo.name] = status;

          var alerts = $rootScope.common.alertsMap[pipelineInfo.name];

          if(alerts) {
            delete $rootScope.common.alertsMap[pipelineInfo.name];
            $rootScope.common.alertsTotalCount -= alerts.length;
          }
        }, function () {

        });
      },

      /**
       * Return pipeline alerts for tooltip
       *
       * @param pipelineAlerts
       */
      getPipelineAlerts: function(pipelineAlerts) {
        var alertMsg ='<span class="stage-errors-tooltip">';
        if(pipelineAlerts) {
          angular.forEach(pipelineAlerts, function(alert) {
            alertMsg += alert.ruleDefinition.alertText + '<br>';
          });
        }
        alertMsg += '</span>';
        return alertMsg;
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