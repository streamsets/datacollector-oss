/**
 * Controller for Graph Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('GraphController', function ($scope, $rootScope, _, api, $timeout) {
    var stageCounter = 0,
      SOURCE_STAGE_TYPE = 'SOURCE',
      PROCESSOR_STAGE_TYPE = 'PROCESSOR',
      TARGET_STAGE_TYPE = 'TARGET';

    angular.extend($scope, {
      /**
       * Add Stage Instance to the Pipeline Graph.
       * @param stage
       */
      addStageInstance: function (stage) {
        var xPos = ($scope.pipelineConfig.stages && $scope.pipelineConfig.stages.length) ?
          $scope.pipelineConfig.stages[$scope.pipelineConfig.stages.length - 1].uiInfo.xPos + 300 : 200,
          yPos = 70,
          inputConnectors = (stage.type !== SOURCE_STAGE_TYPE) ? ['i1'] : [],
          outputConnectors = (stage.type !== TARGET_STAGE_TYPE) ? ['01'] : [],
          stageInstance = {
            instanceName: stage.name + (new Date()).getTime(),
            library: stage.library,
            stageName: stage.name,
            stageVersion: stage.version,
            configuration: [],
            uiInfo: {
              label: stage.label + (++stageCounter),
              description: stage.description,
              xPos: xPos,
              yPos: yPos,
              inputConnectors: inputConnectors,
              outputConnectors: outputConnectors,
              stageType: stage.type
            },
            inputLanes: [],
            outputLanes: []
          };

        if (stage.type !== TARGET_STAGE_TYPE) {
          stageInstance.outputLanes = [stageInstance.instanceName + 'OutputLane'];
        }

        angular.forEach(stage.configDefinitions, function (configDefinition) {
          var config = {
            name: configDefinition.name
          };

          if(configDefinition.type === 'MODEL' && configDefinition.model.modelType === 'FIELD_SELECTOR') {
            config.value = [];
          }

          stageInstance.configuration.push(config);
        });

        switch(stage.type) {
          case SOURCE_STAGE_TYPE:
            stageInstance.uiInfo.icon = 'assets/stage/ic_insert_drive_file_48px.svg';
            break;
          case PROCESSOR_STAGE_TYPE:
            stageInstance.uiInfo.icon = 'assets/stage/ic_settings_48px.svg';
            break;
          case TARGET_STAGE_TYPE:
            stageInstance.uiInfo.icon = 'assets/stage/ic_storage_48px.svg';
            break;
        }

        $scope.$broadcast('addNode', stageInstance);

        $scope.detailPaneConfig = stageInstance;
        $scope.detailPaneConfigDefn = stage;
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
       * Checks if configuration has any issue.
       *
       * @param {Object} configObject - The Pipeline Configuration/Stage Configuration Object.
       * @returns {Boolean} - Returns true if configuration has any issue otherwise false.
       */
      hasConfigurationIssues: function(configObject) {
        var config = $scope.pipelineConfig,
          issues;

        if(config && config.issues) {
          if(configObject.instanceName && config.issues.stageIssues &&
            config.issues.stageIssues && config.issues.stageIssues[configObject.instanceName]) {
            issues = config.issues.stageIssues[configObject.instanceName];
          } else if(config.issues.pipelineIssues){
            issues = config.issues.pipelineIssues;
          }
        }

        return _.find(issues, function(issue) {
          return issue.level === 'STAGE_CONFIG';
        });
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
        api.pipelineAgent.startPipeline($scope.activeConfigInfo.name).
          success(function(res) {
            $rootScope.common.pipelineStatus = res;
          }).
          error(function() {

          });
      },

      /**
       * On Stop Pipeline button click.
       *
       */
      stopPipeline: function() {
        api.pipelineAgent.stopPipeline().
          success(function(res) {
            $rootScope.common.pipelineStatus = res;
          }).
          error(function() {

          });
      },

      /**
       * Capture the snapshot of running pipeline.
       *
       */
      captureSnapshot: function() {

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