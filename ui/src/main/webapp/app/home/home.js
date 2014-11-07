/**
 * Home module for displaying home page content
 */

angular
  .module('pipelineAgentApp.home',[
    'ngRoute',
    'ngTagsInput',
    'jsonFormatter',
    'splitterDirectives',
    'tabDirectives',
    'pipelineGraphDirectives',
    'showErrorsDirectives',
    'underscore'
  ])
  .config(['$routeProvider', function($routeProvider) {
    $routeProvider.when('/',
      {
        templateUrl: 'app/home/home.tpl.html',
        controller: 'HomeController'
      }
    );
  }])
  .controller('HomeController', function($scope, $timeout, api, _, $q) {
    var stageCounter = 0,
      timeout,
      saveUpdates,
      updateGraph,
      getConfigurationLabel,
      ignoreUpdate = false,
      edges = [],
      pipelineGeneralConfigDefinitions = [
        {
          name: 'label',
          label: 'Label'
        },
        {
          name: 'description',
          label: 'Description'
        }
      ],
      stageGeneralConfigDefinitions = [
        {
          name: 'label',
          label: 'Label'
        },
        {
          name: 'description',
          label: 'Description'
        }
      ];

    $scope.isPipelineRunning = false;
    $scope.stageLibraries = [];
    $scope.pipelineConfigInfo={};
    $scope.pipelineGraphData = {};
    $scope.issuesLength = 0;
    $scope.previewMode = false;
    $scope.previewData = {};
    $scope.stagePreviewData = {
      input: {},
      output: {}
    };

    $q.all([api.pipelineAgent.getDefinitions(),
      api.pipelineAgent.getPipelineConfig(),
      api.pipelineAgent.getPipelineConfigInfo()])
      .then(function(results){

        //Definitions
        var definitions = results[0].data;
        $scope.pipelineConfigDefinition = definitions.pipeline[0];
        $scope.stageLibraries = definitions.stages;

        $scope.sources = _.filter($scope.stageLibraries, function(stageLibrary) {
          return (stageLibrary.type === 'SOURCE');
        });

        $scope.processors = _.filter($scope.stageLibraries, function(stageLibrary) {
          return (stageLibrary.type === 'PROCESSOR');
        });

        $scope.targets = _.filter($scope.stageLibraries, function(stageLibrary) {
          return (stageLibrary.type === 'TARGET');
        });

        //Pipeline Configuration Info
        $scope.pipelineConfigInfo = results[2].data;

        //Pipeline Configuration
        updateGraph(results[1].data);

        stageCounter = ($scope.pipelineConfig && $scope.pipelineConfig.stages) ? $scope.pipelineConfig.stages.length : 0;
    });

    saveUpdates = function() {
      var pipelineConfigClone = _.clone($scope.pipelineConfig);

      delete pipelineConfigClone.info;
      api.pipelineAgent.savePipelineConfig(pipelineConfigClone).success(function(res) {
        updateGraph(res);
      });
    };

    updateGraph = function(pipelineConfig) {
      var selectedStageInstance,
        issueCount = 0;
      ignoreUpdate = true;

      //Force Validity Check - showErrors directive
      $scope.$broadcast('show-errors-check-validity');

      if(!pipelineConfig.uiInfo) {
        pipelineConfig.uiInfo = {
          label : 'Pipeline',
          description: 'Default Pipeline'
        };
      }

      $scope.pipelineConfig = pipelineConfig || {};

      _.each(pipelineConfig.issues, function(value, key) {
        if(_.isArray(value)) {
          issueCount += value.length;
        } else if(_.isObject(value)) {
          _.each(value, function(stageInstanceIssues, stageInstanceName){
            if(_.isArray(stageInstanceIssues)) {
              issueCount += stageInstanceIssues.length;
            }
          });
        }
      });

      $scope.issuesLength = issueCount;

      //Determine edges from input lanes and output lanes
      edges = [];
      angular.forEach($scope.pipelineConfig.stages, function(sourceStageInstance) {
        if(sourceStageInstance.outputLanes && sourceStageInstance.outputLanes.length) {
          angular.forEach(sourceStageInstance.outputLanes, function(outputLane) {
            angular.forEach($scope.pipelineConfig.stages, function(targetStageInstance) {
              if(targetStageInstance.inputLanes && targetStageInstance.inputLanes.length &&
                _.contains(targetStageInstance.inputLanes, outputLane)) {
                edges.push({
                  source: sourceStageInstance,
                  target: targetStageInstance
                });
              }
            });
          });
        }
      });

      $scope.$broadcast('updateGraph', $scope.pipelineConfig.stages, edges,
        ($scope.detailPaneConfig && !$scope.detailPaneConfig.stages) ? $scope.detailPaneConfig : undefined);

      if($scope.detailPaneConfig === undefined) {
        //First time
        $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.pipelineConfig;
      } else {
        //Check
        if($scope.detailPaneConfig.stages) {
          //In case of detail pane is Pipeline Configuration
          $scope.detailPaneConfig = $scope.pipelineConfig;
        } else {
          //In case of detail pane is stage instance
          angular.forEach($scope.pipelineConfig.stages, function(stageInstance) {
            if(stageInstance.instanceName === $scope.detailPaneConfig.instanceName) {
              selectedStageInstance = stageInstance;
            }
          });

          if(selectedStageInstance) {
            $scope.detailPaneConfig = selectedStageInstance;
          } else {
            $scope.detailPaneConfig = $scope.pipelineConfig;
            $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;
            $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
          }

        }
      }
    };

    $scope.$watch('pipelineConfig', function(newValue, oldValue) {
      if(ignoreUpdate) {
        $timeout(function() { ignoreUpdate = false; });
        return;
      }
      if (!angular.equals(newValue, oldValue)) {
        if (timeout) {
          $timeout.cancel(timeout);
        }
        timeout = $timeout(saveUpdates, 1000);
      }
    }, true);

    $scope.$on('onNodeSelection', function(event, stageInstance) {
      $scope.detailPaneConfig = stageInstance;
      $scope.detailPaneConfigDefn = _.find($scope.stageLibraries, function(stageLibrary){
        return stageLibrary.name === stageInstance.stageName &&
          stageLibrary.version === stageInstance.stageVersion;
      });
      $scope.detailPaneGeneralConfigDefn = stageGeneralConfigDefinitions;

      if($scope.previewMode) {
        $scope.stagePreviewData = getPreviewDataForStage($scope.previewData, $scope.detailPaneConfig);
      }
    });

    $scope.$on('onRemoveNodeSelection', function() {
      $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
      $scope.detailPaneConfig = $scope.pipelineConfig;
      $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;

      if($scope.previewMode) {
        $scope.stagePreviewData = {
          input: {},
          output: {}
        };
      }

    });

    $scope.addStage = function(stage) {
      var xPos = ($scope.pipelineConfig.stages && $scope.pipelineConfig.stages.length) ?
                    $scope.pipelineConfig.stages[$scope.pipelineConfig.stages.length - 1].uiInfo.xPos + 300 : 200,
        yPos = 70,
        inputConnectors = (stage.type !== 'SOURCE') ? ['i1'] : [],
        outputConnectors = (stage.type !== 'TARGET') ? ['01'] : [],
        stageInstance = {
          instanceName : stage.name + (new Date()).getTime(),
          library : stage.library,
          stageName : stage.name,
          stageVersion : stage.version,
          configuration : [],
          uiInfo : {
            label: stage.label + (++stageCounter),
            description: stage.description,
            xPos : xPos,
            yPos : yPos,
            inputConnectors: inputConnectors,
            outputConnectors: outputConnectors
          },
          inputLanes : [],
          outputLanes : []
        };

      if(stage.type !== 'TARGET') {
        stageInstance.outputLanes = [stageInstance.instanceName + 'OutputLane1'];
      }

      angular.forEach(stage.configDefinitions, function(configDefinition) {
        stageInstance.configuration.push({
          name: configDefinition.name,
          value: null
        });
      });

      $scope.$broadcast('addNode', stageInstance);

      $scope.detailPaneConfig = stageInstance;
      $scope.detailPaneConfigDefn = stage;
      $scope.detailPaneGeneralConfigDefn = stageGeneralConfigDefinitions;

    };

    $scope.getStageInstanceLabel = function(stageInstanceName) {
      var instance;
      angular.forEach($scope.pipelineConfig.stages, function(stageInstance) {
        if(stageInstance.instanceName === stageInstanceName) {
          instance = stageInstance;
        }
      });
      return (instance && instance.uiInfo) ? instance.uiInfo.label : undefined;
    };

    $scope.getIssuesMessage = function(stageInstanceName, issue) {
      var msg = issue.message;

      if(issue.level === 'STAGE_CONFIG') {
        var stageInstance = _.find($scope.pipelineConfig.stages, function(stage) {
          return stage.instanceName === stageInstanceName;
        });

        if(stageInstance) {
          msg += ' : ' + getConfigurationLabel(stageInstance, issue.configName);
        }
      }

      return msg;
    };

    getConfigurationLabel = function (stageInstance, configName) {
      var stageDefinition = _.find($scope.stageLibraries, function(stage) {
          return stageInstance.library === stage.library &&
                  stageInstance.stageName === stage.name &&
                  stageInstance.stageVersion === stage.version;
        }),
        configDefinition =  _.find(stageDefinition.configDefinitions, function(configDefinition) {
          return configDefinition.name === configName;
        });

      return configDefinition ? configDefinition.label : configName;
    };

    $scope.previewPipeline = function() {
      $scope.previewMode = true;

      api.pipelineAgent.previewPipeline().success(function(previewData) {
        $scope.previewData = previewData;

        if(!$scope.detailPaneConfig.stages) {
          $scope.stagePreviewData = getPreviewDataForStage(previewData, $scope.detailPaneConfig);
        }
      });
    };

    $scope.closePreview = function() {
      $scope.previewMode = false;
    };

    getPreviewDataForStage = function(previewData, stageInstance) {
      var inputLane = (stageInstance.inputLanes && stageInstance.inputLanes.length) ? stageInstance.inputLanes[0] : undefined,
        outputLane = (stageInstance.outputLanes && stageInstance.outputLanes.length) ? stageInstance.outputLanes[0] : undefined,
        stagePreviewData = {
          input: {},
          output: {}
        };

      angular.forEach(previewData.stagesOutput, function(stageOutput) {
        if(inputLane && stageOutput.output[inputLane] && stageOutput.output) {
          stagePreviewData.input = stageOutput.output[inputLane];
        } else if(outputLane && stageOutput.output[outputLane] && stageOutput.output) {
          stagePreviewData.output = stageOutput.output[outputLane];
        }
      });

      return stagePreviewData;
    };

  });