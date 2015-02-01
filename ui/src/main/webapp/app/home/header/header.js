/**
 * Controller for Header Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('HeaderController', function ($scope, $rootScope, $timeout, _, api, $translate,
                                           pipelineService, pipelineConstant, $modal) {

    angular.extend($scope, {
      iconOnly: true,
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
        $scope.addStageInstance({
          stage: selectedStage
        });
      },

      /**
       * Callback function when selecting Processor/Target from alert div.
       */
      onConnectStageChange: function() {
        var connectStage = $scope.connectStage.selected;
        $scope.addStageInstance({
          stage: connectStage,
          firstOpenLane: $scope.firstOpenLane
        });
        $scope.connectStage = {};
        $scope.firstOpenLane.stageInstance = undefined;
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

          $scope.changeStageSelection({
            selectedObject: stageInstance,
            type: pipelineConstant.STAGE_INSTANCE,
            detailTabName: 'configuration',
            configGroup: issue.configGroup,
            configName: issue.configName
          });

        } else {
          //Select Pipeline Config
          $scope.$broadcast('selectNode');
          $scope.changeStageSelection({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE
          });
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
            function (res) {
              $rootScope.common.errors = [res.data];
            }
          ).
            then(
            function (res) {
              if(res) {
                $rootScope.common.pipelineMetrics = res.data;
                $rootScope.common.pipelineStatus = startResponse;

                $timeout(function() {
                  $scope.refreshGraph();
                });
              }
            },
            function (res) {
              $rootScope.common.errors = [res.data];
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
          $rootScope.common.pipelineStatus = status;
          $scope.refreshGraph();
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
      },

      /**
       * Callback function when Notification is clicked.
       *
       * @param alert
       */
      onNotificationClick: function(alert) {
        var edges = $scope.edges,
          edge;
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;

        if(alert.type === 'METRIC_ALERT') {
          //Select Pipeline Config
          $scope.$broadcast('selectNode');
          $scope.changeStageSelection({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE
          });
        } else {
          //Select edge
          edge = _.find(edges, function(ed) {
            return ed.outputLane === alert.rule.lane;
          });

          $scope.changeStageSelection({
            selectedObject: edge,
            type: pipelineConstant.LINK
          });
        }
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