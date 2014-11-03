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
      ignoreUpdate = false,
      edges = [],
      pipelineGeneralConfigDefinitions = [
        {
          name: 'name',
          label: 'Name'
        },
        {
          name: 'description',
          label: 'Description'
        }
      ],
      stageGeneralConfigDefinitions = [
        {
          name: 'instanceName',
          label: 'Name'
        },
        {
          name: 'description',
          label: 'Description'
        }
      ],
      pipelineConfigDefinition = {
        label : 'Pipeline Configuration',
        configDefinitions: [
          {
            name: 'onError',
            type: 'STRING',
            label: 'On Error',
            description: 'Action on error during pipeline execution.',
            required: true,
            group: '',
            defaultValue: '',
            listValues: [{
              value: 'DROP_BATCH',
              label: 'Drop Batch'
            },{
              value: 'DROP_RECORD',
              label: 'Drop Record'
            },{
              value: 'STOP_PIPELINE',
              label: 'Stop Pipeline'
            }]
          },
          {
            name: 'deliveryGuarantee',
            type: 'STRING',
            label: 'Delivery Guarantee',
            description: '',
            required: true,
            group: '',
            defaultValue: '',
            listValues: [{
              value: 'AT_LEAST_ONCE',
              label: 'At Least Once (There may be duplicates, no data lost)'
            },{
              value: 'AT_MOST_ONCE',
              label: 'At Most Once (No duplicates, potential data lost)'
            }]
          }
        ]
      };

    $scope.isPipelineRunning = false;
    $scope.stageLibraries = [];
    $scope.pipelineConfigInfo={};
    $scope.pipelineGraphData = {};

    $q.all([api.pipelineAgent.getStageLibrary(),
      api.pipelineAgent.getPipelineConfig(),
      api.pipelineAgent.getPipelineConfigInfo()])
      .then(function(results){

        //Stage Libraries
        $scope.stageLibraries = results[0].data;

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

        stageCounter = $scope.pipelineConfig.stages.length;
    });

    saveUpdates = function() {
      console.log('Saving Pipeline Config');

      var pipelineConfigClone = _.clone($scope.pipelineConfig);

      //TODO: Remove this once backend is updated.
      delete pipelineConfigClone.name;
      delete pipelineConfigClone.description;

      pipelineConfigClone.onError = $scope.pipelineConfig.configuration[0].value;
      delete pipelineConfigClone.configuration;
      //pipelineConfigClone.stages = [];

      api.pipelineAgent.savePipelineConfig(pipelineConfigClone).success(function(res) {
        updateGraph(res);
      });

    };

    updateGraph = function(pipelineConfig) {
      var selectedStageInstance;
      ignoreUpdate = true;
      pipelineConfig.name = $scope.pipelineConfigInfo.name;
      pipelineConfig.description = $scope.pipelineConfigInfo.description;

      pipelineConfig.configuration = [{
        name: 'onError',
        value: pipelineConfig.onError
      },{
        name: 'deliveryGuarantee',
        value: pipelineConfig.deliveryGuarantee
      }];

      $scope.pipelineConfig = pipelineConfig;

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
        $scope.detailPaneConfig = $scope.pipelineConfig;
        $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;
        $scope.detailPaneConfigDefn = pipelineConfigDefinition;
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
            $scope.detailPaneConfigDefn = pipelineConfigDefinition;
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
    });

    $scope.$on('onRemoveNodeSelection', function() {
      $scope.detailPaneConfigDefn = pipelineConfigDefinition;
      $scope.detailPaneConfig = $scope.pipelineConfig;
      $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;
    });

    $scope.addStage = function(stage) {
      var xPos = ($scope.pipelineConfig.stages && $scope.pipelineConfig.stages.length) ?
                    $scope.pipelineConfig.stages[$scope.pipelineConfig.stages.length - 1].uiInfo.xPos + 300 : 200,
        yPos = 100;

      var stageInstance = {
        instanceName : stage.label + (++stageCounter),
        library : stage.library,
        stageName : stage.name,
        stageVersion : stage.version,
        configuration : [],
        uiInfo : {
          xPos : xPos,
          yPos : yPos
        },
        inputLanes : [],
        outputLanes : []
      };

      angular.forEach(stage.configDefinitions, function(configDefinition) {
        stageInstance.configuration.push({
          name: configDefinition.name,
          value: undefined
        });
      });

      $scope.$broadcast('addNode', stageInstance);

      $scope.detailPaneConfig = stageInstance;
      $scope.detailPaneConfigDefn = stage;
      $scope.detailPaneGeneralConfigDefn = stageGeneralConfigDefinitions;

    };

  });