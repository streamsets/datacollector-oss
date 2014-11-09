/**
 * Home module for displaying home page content
 */

angular
  .module('pipelineAgentApp.home', [
    'ngRoute',
    'ngTagsInput',
    'jsonFormatter',
    'splitterDirectives',
    'tabDirectives',
    'pipelineGraphDirectives',
    'showErrorsDirectives',
    'underscore'
  ])
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/',
      {
        templateUrl: 'app/home/home.tpl.html',
        controller: 'HomeController'
      }
    );
  }])
  .controller('HomeController', function ($scope, $timeout, api, _, $q) {
    var stageCounter = 0,
      timeout,
      dirty = false,
      saveOperationRunning,
      saveUpdates,
      updateGraph,
      updateDetailPane,
      getConfigurationLabel,
      getPreviewDataForStage,
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

    angular.extend($scope, {
      isPipelineRunning: false,
      stageLibraries: [],
      pipelineConfigInfo: {},
      pipelineGraphData: {},
      previewMode: false,
      previewData: {},
      stagePreviewData: {
        input: [],
        output: []
      },
      previewSourceOffset: 0,
      previewBatchSize: 10,

      /**
       * Add Stage Instance to the Pipeline Graph.
       * @param stage
       */
      addStageInstance: function (stage) {
        var xPos = ($scope.pipelineConfig.stages && $scope.pipelineConfig.stages.length) ?
            $scope.pipelineConfig.stages[$scope.pipelineConfig.stages.length - 1].uiInfo.xPos + 300 : 200,
          yPos = 70,
          inputConnectors = (stage.type !== 'SOURCE') ? ['i1'] : [],
          outputConnectors = (stage.type !== 'TARGET') ? ['01'] : [],
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
              outputConnectors: outputConnectors
            },
            inputLanes: [],
            outputLanes: []
          };

        if (stage.type !== 'TARGET') {
          stageInstance.outputLanes = [stageInstance.instanceName + 'OutputLane1'];
        }

        angular.forEach(stage.configDefinitions, function (configDefinition) {
          stageInstance.configuration.push({
            name: configDefinition.name
          });
        });

        $scope.$broadcast('addNode', stageInstance);

        $scope.detailPaneConfig = stageInstance;
        $scope.detailPaneConfigDefn = stage;
        $scope.detailPaneGeneralConfigDefn = stageGeneralConfigDefinitions;

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
       * Fetches preview data for the pipeline and sets previewMode flag to true.
       *
       * @param nextBatch - By default it starts fetching from sourceOffset=0, if nextBatch is true sourceOffset is
       * updated to fetch next batch.
       */
      previewPipeline: function (nextBatch) {
        $scope.previewMode = true;

        if (nextBatch) {
          $scope.previewSourceOffset += $scope.previewBatchSize;
        } else {
          $scope.previewSourceOffset = 0;
        }

        api.pipelineAgent.previewPipeline('xyz', $scope.previewSourceOffset, $scope.previewBatchSize).success(function (previewData) {
          $scope.previewData = previewData;

          if (!$scope.detailPaneConfig.stages) {
            $scope.stagePreviewData = getPreviewDataForStage(previewData, $scope.detailPaneConfig);
          }
        });
      },

      /**
       * Sets previewMode flag to false.
       */
      closePreview: function () {
        $scope.previewMode = false;
      },

      /**
       * Returns length of the preview collection.
       *
       * @returns {Array}
       */
      getPreviewRange: function () {
        var range = 0;

        if ($scope.stagePreviewData && $scope.stagePreviewData.input &&
          $scope.stagePreviewData.input.length) {
          range = $scope.stagePreviewData.input.length;
        } else if ($scope.stagePreviewData && $scope.stagePreviewData.output &&
          $scope.stagePreviewData.output.length) {
          range = $scope.stagePreviewData.output.length;
        }

        return new Array(range);
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
       * Returns message for the give Configuration Object and Definition.
       *
       * @param configObject
       * @param configDefinition
       */
      getConfigurationIssueMessage: function(configObject, configDefinition) {
        var config = $scope.pipelineConfig,
          issues,
          issue;

        if(config && config.issues) {
          if(configObject.instanceName && config.issues.stageIssues &&
            config.issues.stageIssues && config.issues.stageIssues[configObject.instanceName]) {
            issues = config.issues.stageIssues[configObject.instanceName];
          } else if(config.issues.pipelineIssues){
            issues = config.issues.pipelineIssues;
          }
        }

        issue = _.find(issues, function(issue) {
           return (issue.level === 'STAGE_CONFIG' && issue.configName === configDefinition.name);
        });

        return issue ? issue.message : '';
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
          $scope.$broadcast('selectNode', stageInstance);
          updateDetailPane(stageInstance);
          $('.configuration-tabs a:last').tab('show');
        } else {
          //Select Pipeline Config
          $scope.$broadcast('selectNode');
          updateDetailPane();
        }
      }
    });


    $q.all([api.pipelineAgent.getDefinitions(),
      api.pipelineAgent.getPipelineConfig('xyz'),
      api.pipelineAgent.getPipelineConfigInfo('xyz')])
      .then(function (results) {

        //Definitions
        var definitions = results[0].data;
        $scope.pipelineConfigDefinition = definitions.pipeline[0];
        $scope.stageLibraries = definitions.stages;

        $scope.sources = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === 'SOURCE');
        });

        $scope.processors = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === 'PROCESSOR');
        });

        $scope.targets = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === 'TARGET');
        });

        //Pipeline Configuration Info
        $scope.pipelineConfigInfo = results[2].data;

        //Pipeline Configuration
        updateGraph(results[1].data);

        stageCounter = ($scope.pipelineConfig && $scope.pipelineConfig.stages) ?
          $scope.pipelineConfig.stages.length : 0;
      });

    /**
     * Save Updates
     * @param config
     */
    saveUpdates = function (config) {
      if (saveOperationRunning) {
        return;
      }

      if (!config) {
        config = _.clone($scope.pipelineConfig);
      }

      delete config.info;

      dirty = false;
      saveOperationRunning = true;
      api.pipelineAgent.savePipelineConfig('xyz', config).success(function (res) {
        saveOperationRunning = false;
        if (dirty) {
          config = _.clone($scope.pipelineConfig);
          config.uuid = res.uuid;
          //saveUpdates(config);
        }
        updateGraph(res);
      });
    };

    /**
     * Update Pipeline Graph
     *
     * @param pipelineConfig
     */
    updateGraph = function (pipelineConfig) {
      var selectedStageInstance;

      ignoreUpdate = true;

      //Force Validity Check - showErrors directive
      $scope.$broadcast('show-errors-check-validity');

      if (!pipelineConfig.uiInfo) {
        pipelineConfig.uiInfo = {
          label: 'Pipeline',
          description: 'Default Pipeline'
        };
      }

      $scope.pipelineConfig = pipelineConfig || {};


      //Determine edges from input lanes and output lanes
      edges = [];
      angular.forEach($scope.pipelineConfig.stages, function (sourceStageInstance) {
        if (sourceStageInstance.outputLanes && sourceStageInstance.outputLanes.length) {
          angular.forEach(sourceStageInstance.outputLanes, function (outputLane) {
            angular.forEach($scope.pipelineConfig.stages, function (targetStageInstance) {
              if (targetStageInstance.inputLanes && targetStageInstance.inputLanes.length &&
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

      if ($scope.detailPaneConfig === undefined) {
        //First time
        $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.pipelineConfig;
      } else {
        //Check
        if ($scope.detailPaneConfig.stages) {
          //In case of detail pane is Pipeline Configuration
          $scope.detailPaneConfig = $scope.pipelineConfig;
        } else {
          //In case of detail pane is stage instance
          angular.forEach($scope.pipelineConfig.stages, function (stageInstance) {
            if (stageInstance.instanceName === $scope.detailPaneConfig.instanceName) {
              selectedStageInstance = stageInstance;
            }
          });

          if (selectedStageInstance) {
            $scope.detailPaneConfig = selectedStageInstance;
          } else {
            $scope.detailPaneConfig = $scope.pipelineConfig;
            $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;
            $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
          }

        }
      }
    };

    /**
     * Update Detail Pane
     *
     * @param stageInstance
     */
    updateDetailPane = function(stageInstance) {
      if(stageInstance) {
        //Stage Instance Configuration
        $scope.detailPaneConfig = stageInstance;
        $scope.detailPaneConfigDefn = _.find($scope.stageLibraries, function (stageLibrary) {
          return stageLibrary.name === stageInstance.stageName &&
            stageLibrary.version === stageInstance.stageVersion;
        });
        $scope.detailPaneGeneralConfigDefn = stageGeneralConfigDefinitions;

        if ($scope.previewMode) {
          $scope.stagePreviewData = getPreviewDataForStage($scope.previewData, $scope.detailPaneConfig);
        }
      } else {
        //Pipeline Configuration
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.pipelineConfig;
        $scope.detailPaneGeneralConfigDefn = pipelineGeneralConfigDefinitions;

        if ($scope.previewMode) {
          $scope.stagePreviewData = {
            input: {},
            output: {}
          };
        }
      }
    };

    /**
     * Returns label of Configuration for given Stage Instance object and Configuration Name.
     *
     * @param stageInstance
     * @param configName
     * @returns {*}
     */
    getConfigurationLabel = function (stageInstance, configName) {
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


    /**
     *
     * @param previewData
     * @param stageInstance
     * @returns {{input: Array, output: Array}}
     */
    getPreviewDataForStage = function (previewData, stageInstance) {
      var inputLane = (stageInstance.inputLanes && stageInstance.inputLanes.length) ?
          stageInstance.inputLanes[0] : undefined,
        outputLane = (stageInstance.outputLanes && stageInstance.outputLanes.length) ?
          stageInstance.outputLanes[0] : undefined,
        stagePreviewData = {
          input: [],
          output: []
        };

      angular.forEach(previewData.stagesOutput, function (stageOutput) {
        if (inputLane && stageOutput.output[inputLane] && stageOutput.output) {
          stagePreviewData.input = stageOutput.output[inputLane];
        } else if (outputLane && stageOutput.output[outputLane] && stageOutput.output) {
          stagePreviewData.output = stageOutput.output[outputLane];
        }
      });

      return stagePreviewData;
    };

    //Event Handling

    $scope.$watch('pipelineConfig', function (newValue, oldValue) {
      if (ignoreUpdate) {
        $timeout(function () {
          ignoreUpdate = false;
        });
        return;
      }
      if (!angular.equals(newValue, oldValue)) {
        dirty = true;
        if (timeout) {
          $timeout.cancel(timeout);
        }
        timeout = $timeout(saveUpdates, 1000);
      }
    }, true);

    $scope.$on('onNodeSelection', function (event, stageInstance) {
      updateDetailPane(stageInstance);
    });

    $scope.$on('onRemoveNodeSelection', function () {
      updateDetailPane();
    });

  });