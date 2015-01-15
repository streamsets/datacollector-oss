/**
 * Controller for Graph Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('GraphController', function ($scope, $rootScope, $timeout, _, api, $translate,
                                           pipelineService, pipelineConstant, $modal) {

    angular.extend($scope, {

      selectedSource: {},
      connectStage: {},

      /**
       * Callback function when Selecting Source from alert div.
       *
       */
      onSelectSourceChange: function() {
        var selectedStage = $scope.selectedSource.selected;
        $scope.pipelineConfig.issues = [];
        $scope.selectedSource = {};
        $scope.addStageInstance(selectedStage);
      },

      /**
       * Callback function when selecting Processor/Target from alert div.
       */
      onConnectStageChange: function() {
        var connectStage = $scope.connectStage.selected;
        $scope.addStageInstance(connectStage, $scope.firstOpenLane);
        $scope.connectStage = {};
        $scope.firstOpenLane.stageInstance = undefined;
      },

      /**
       * Add Stage Instance to the Pipeline Graph.
       * @param stage
       * @param firstOpenLane [optional]
       */
      addStageInstance: function (stage, firstOpenLane) {
        var stageInstance = pipelineService.getNewStageInstance(stage, $scope.pipelineConfig, undefined, firstOpenLane),
          edge;

        $scope.updateDetailPaneObject(stageInstance, stage);

        if(firstOpenLane && firstOpenLane.stageInstance) {
          edge = {
            source: firstOpenLane.stageInstance,
            target: stageInstance,
            outputLane: firstOpenLane.laneName
          };
        }

        $scope.$broadcast('addNode', stageInstance, edge);
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
       *
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
          $scope.changeStageSelection(stageInstance, pipelineConstant.STAGE_INSTANCE, 'configuration', issue.configName);
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
                $scope.moveGraphToCenter();
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

                $timeout(function() {
                  $scope.refreshGraph();
                });
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
        var modalInstance = $modal.open({
          templateUrl: 'app/home/graph/stop/stopConfirmation.tpl.html',
          controller: 'StopConfirmationModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.activeConfigInfo;
            }
          }
        });

        modalInstance.result.then(function(status) {
          $scope.moveGraphToCenter();
          $scope.refreshGraph();
          $rootScope.common.pipelineStatus = status;
          $scope.$broadcast('updateErrorCount', {});
        }, function () {

        });
      },


      /**
       * Reset Offset of pipeline
       *
       */
      resetOffset: function() {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/resetOffset/resetOffset.tpl.html',
          controller: 'ResetOffsetModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.activeConfigInfo;
            }
          }
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