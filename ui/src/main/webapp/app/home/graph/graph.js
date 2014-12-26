/**
 * Controller for Graph Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('GraphController', function ($scope, $rootScope, _, api, $translate, pipelineService) {

    angular.extend($scope, {
      /**
       * Add Stage Instance to the Pipeline Graph.
       * @param stage
       */
      addStageInstance: function (stage) {
        var stageInstance = pipelineService.getNewStageInstance(stage, $scope.pipelineConfig);
        $scope.updateDetailPaneObject(stageInstance, stage);
        $scope.$broadcast('addNode', stageInstance);
      },

      /**
       * Returns label of the Stage Instance.
       *
       * @param stageInstanceName
       * @returns {*|string}
       */
      getStageInstanceLabel: function (stageInstanceName) {
        var instance;
        angular.forEach($scope.pipelineConfig.stages, function (stageInstance) {
          if (stageInstance.instanceName === stageInstanceName) {
            instance = stageInstance;
          }
        });
        return (instance && instance.uiInfo) ? instance.uiInfo.label : undefined;
      },

      /**
       * Returns message string of the issue.
       *
       * @param stageInstanceName
       * @param issue
       * @returns {*}
       */
      getIssuesMessage: function (stageInstanceName, issue) {
        var msg = issue.message;

        if (issue.level === 'STAGE_CONFIG') {
          var stageInstance = _.find($scope.pipelineConfig.stages, function (stage) {
            return stage.instanceName === stageInstanceName;
          });

          if (stageInstance) {
            msg += ' : ' + getConfigurationLabel(stageInstance, issue.configName);
          }
        }

        return msg;
      },

      /**
       * On clicking issue in Issues dropdown selects the stage and if issue level is STAGE_CONFIG
       * Configuration is
       * @param issue
       * @param instanceName
       */
      onIssueClick: function(issue, instanceName) {
        var pipelineConfig = $scope.pipelineConfig,
          stageInstance;
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;
        if(instanceName) {
          //Select stage instance
          stageInstance = _.find(pipelineConfig.stages, function(stage) {
            return stage.instanceName === instanceName;
          });
          $scope.changeStageSelection(stageInstance);
          //$('.configuration-tabs a:last').tab('show');
        } else {
          //Select Pipeline Config
          $scope.$broadcast('selectNode');
          $scope.changeStageSelection();
        }
      },

      /**
       * On Start Pipeline button click.
       *
       */
      startPipeline: function() {
        if($rootScope.common.pipelineStatus.state !== 'RUNNING') {
          var startResponse;
          $scope.$storage.maximizeDetailPane = false;
          $scope.$storage.minimizeDetailPane = false;
          api.pipelineAgent.startPipeline($scope.activeConfigInfo.name, 0).
            then(
              function (res) {
                startResponse = res.data;
                return api.pipelineAgent.getPipelineMetrics();
              },
              function (data) {
                $rootScope.common.errors = [data];
              }
            ).
            then(
              function (res) {
                $rootScope.common.pipelineMetrics = res.data;
                $rootScope.common.pipelineStatus = startResponse;
              },
              function (data) {
                $rootScope.common.errors = [data];
              }
            );
        } else {
          $translate('home.graphPane.startErrorMessage', {
            name: $rootScope.common.pipelineStatus.name
          }).then(function(translation) {
            $rootScope.common.errors = [translation];
          });
        }
      },

      /**
       * On Stop Pipeline button click.
       *
       */
      stopPipeline: function() {
        api.pipelineAgent.stopPipeline().
          success(function(res) {
            $rootScope.common.pipelineStatus = res;
            $scope.$broadcast('updateErrorCount', {});
          }).
          error(function(data) {
            $rootScope.common.errors = [data];
          });
      }

    });

    /**
     * Returns label of Configuration for given Stage Instance object and Configuration Name.
     *
     * @param stageInstance
     * @param configName
     * @returns {*}
     */
    var getConfigurationLabel = function (stageInstance, configName) {
      var stageDefinition = _.find($scope.stageLibraries, function (stage) {
          return stageInstance.library === stage.library &&
            stageInstance.stageName === stage.name &&
            stageInstance.stageVersion === stage.version;
        }),
        configDefinition = _.find(stageDefinition.configDefinitions, function (configDefinition) {
          return configDefinition.name === configName;
        });

      return configDefinition ? configDefinition.label : configName;
    };

  });