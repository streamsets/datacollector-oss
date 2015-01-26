/**
 * Home module for displaying home page content.
 */

angular
  .module('pipelineAgentApp.home')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/',
      {
        templateUrl: 'app/home/home.tpl.html',
        controller: 'HomeController'
      }
    );
  }])
  .controller('HomeController', function ($scope, $rootScope, $timeout, api, configuration, _, $q, $modal,
                                          $localStorage, pipelineService, pipelineConstant, visibilityBroadcaster) {
    var stageCounter = 0,
      timeout,
      dirty = false,
      rulesDirty = false,
      ignoreUpdate = false,
      pipelineStatusTimer,
      pipelineMetricsTimer,
      edges = [],
      destroyed = false,
      pageHidden = false;

    angular.extend($scope, {
      selectedType: pipelineConstant.PIPELINE,
      loaded: false,
      isPipelineRunning: false,
      pipelines: [],
      sourceExists: false,
      stageLibraries: [],
      pipelineGraphData: {},
      previewMode: false,
      snapshotMode: false,
      hideLibraryPanel: true,
      activeConfigInfo: undefined,
      activeConfigStatus:{
        state: 'STOPPED'
      },
      minimizeDetailPane: false,
      maximizeDetailPane: false,

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        $scope.$broadcast('addPipelineConfig');
      },

      /**
       * Add New Pipeline Configuration
       */
      importPipelineConfig: function() {
        $scope.$broadcast('importPipelineConfig');
      },

      /**
       * Add Stage Instance to the Pipeline Graph.
       * @param stage
       * @param firstOpenLane [optional]
       * @param relativeXPos [optional]
       * @param relativeYPos [optional]
       */
      addStageInstance: function (stage, firstOpenLane, relativeXPos, relativeYPos) {
        if($scope.sourceExists && stage.type === pipelineConstant.SOURCE_STAGE_TYPE) {
          $rootScope.common.errors = ['Origin already exists.'];
          return;
        } else {
          $rootScope.common.errors = [];
        }


        var stageInstance = pipelineService.getNewStageInstance(stage, $scope.pipelineConfig, undefined,
            firstOpenLane, relativeXPos, relativeYPos),
          edge;

        $scope.changeStageSelection({
          selectedObject: stageInstance,
          type: pipelineConstant.STAGE_INSTANCE,
          ignoreBroadCast: true
        });

        if(firstOpenLane && firstOpenLane.stageInstance) {
          edge = {
            source: firstOpenLane.stageInstance,
            target: stageInstance,
            outputLane: firstOpenLane.laneName
          };
        }

        $scope.$broadcast('addNode', stageInstance, edge, relativeXPos, relativeYPos);
      },

      /**
       * Utility function for checking object is empty.
       *
       * @param obj
       * @returns {*|boolean}
       */
      isEmptyObject : function (obj) {
        return angular.equals({},obj);
      },

      /**
       * Value format function for D3 NVD3 charts.
       *
       * @returns {Function}
       */
      valueFormatFunction: function() {
        return function(d){
          return d3.format(',d')(d);
        };
      },

      /**
       * Fetches preview data for the pipeline and sets previewMode flag to true.
       *
       * @param nextBatch - By default it starts fetching from sourceOffset=0, if nextBatch is true sourceOffset is
       * updated to fetch next batch.
       */
      previewPipeline: function (nextBatch) {
        $scope.previewMode = true;
        $rootScope.$storage.maximizeDetailPane = false;
        $rootScope.$storage.minimizeDetailPane = false;
        $scope.setGraphReadOnly(true);
        $scope.setGraphPreviewMode(true);
        $scope.$broadcast('previewPipeline', nextBatch);
      },

      /**
       * Sets previewMode flag to false.
       */
      closePreview: function () {
        $scope.previewMode = false;
        $scope.setGraphReadOnly(false);
        $scope.setGraphPreviewMode(false);
        $scope.moveGraphToCenter();
      },

      /**
       * Capture the snapshot of running pipeline.
       *
       */
      captureSnapshot: function() {
        $scope.snapshotMode = true;
        $rootScope.$storage.maximizeDetailPane = false;
        $rootScope.$storage.minimizeDetailPane = false;
        $scope.setGraphPreviewMode(true);
        $scope.$broadcast('snapshotPipeline');
      },


      /**
       * Sets previewMode flag to false.
       */
      closeSnapshot: function () {
        $scope.snapshotMode = false;
        $scope.setGraphPreviewMode(false);
        $scope.moveGraphToCenter();
      },

      /**
       * Update Selection Object.
       *
       * @param options
       *  selectedObject
       *  type
       *  detailTabName
       *  configGroup
       *  configName
       *  ignoreBroadCast - Boolean flag for telling not to update Graph
       *  moveToCenter
       */
      changeStageSelection: function(options) {
        if(!options.type) {
          if(options.selectedObject) {
            options.type = pipelineConstant.STAGE_INSTANCE;
          } else {
            options.type = pipelineConstant.PIPELINE;
          }
        }

        if(!options.ignoreBroadCast) {
          if(options.type !== pipelineConstant.LINK) {
            $scope.$broadcast('selectNode', options.selectedObject, options.moveToCenter);
          } else {
            $scope.$broadcast('selectEdge', options.selectedObject, options.moveToCenter);
          }
        }


        updateDetailPane(options);
      },

      /**
       * Toggle Library Panel
       */
      toggleLibraryPanel: function() {
        $scope.hideLibraryPanel = ! $scope.hideLibraryPanel;
      },

      /**
       * On Detail Pane Minimize button is clicked.
       */
      onMinimizeDetailPane: function() {
        $rootScope.$storage.maximizeDetailPane = false;
        $rootScope.$storage.minimizeDetailPane = !$rootScope.$storage.minimizeDetailPane;
      },

      /**
       * On Detail Pane Maximize button is clicked.
       */
      onMaximizeDetailPane: function() {
        $rootScope.$storage.minimizeDetailPane = false;
        $rootScope.$storage.maximizeDetailPane = !$rootScope.$storage.maximizeDetailPane;
      },

      /**
       * Update Pipeline Graph to move Graph to center.
       *
       */
      moveGraphToCenter: function() {
        $scope.$broadcast('moveGraphToCenter');
        updateDetailPane({
          selectedObject: undefined,
          type: pipelineConstant.PIPELINE
        });
      },

      /**
       * Update Pipeline Graph by highlighting Start and End node.
       */
      updateStartAndEndStageInstance: function(startStage, endStage) {
        $scope.$broadcast('updateStartAndEndNode', startStage, endStage);
      },

      /**
       * Update Pipeline Graph by clearing highlighting of Start and End Stage Instance.
       */
      clearStartAndEndStageInstance: function() {
        $scope.$broadcast('clearStartAndEndNode');
      },

      /**
       * Clear the variable firstOpenLaneStage
       */
      clearFirstOpenLaneStage: function() {
        $scope.firstOpenLaneStage = undefined;
      },

      /**
       * Refresh the Pipeline Graph.
       */
      refreshGraph : function() {
        updateGraph($scope.pipelineConfig, $scope.pipelineRules);
      },

      /**
       * Set Pipeline Graph Read Only.
       *
       * @param flag
       */
      setGraphReadOnly: function(flag) {
        $scope.$broadcast('setGraphReadOnly', flag);
      },

      /**
       * Set Pipeline Graph Preview Mode.
       *
       * @param flag
       */
      setGraphPreviewMode: function(flag) {
        $scope.$broadcast('setGraphPreviewMode', flag);
      },

      /**
       * Pause Updating Monitoring Data
       */
      pauseMonitoring: function() {
        $scope.monitoringPaused = true;
      },


      /**
       * Continue Updating Monitoring Data
       */
      continueMonitoring: function() {
        $scope.monitoringPaused = false;
      }
    });


    if(!$rootScope.$storage.displayDensity) {
      $rootScope.$storage.displayDensity = pipelineConstant.DENSITY_COMFORTABLE;
    }

    /**
     * Fetch definitions for Pipeline and Stages, fetch all pipeline configuration info, status and metric.
     */
    $q.all([
      api.pipelineAgent.getDefinitions(),
      api.pipelineAgent.getPipelines(),
      api.pipelineAgent.getPipelineStatus(),
      api.pipelineAgent.getPipelineMetrics(),
      configuration.init()
    ])
      .then(function (results) {
        var definitions = results[0].data,
          pipelines = results[1].data,
          pipelineStatus = results[2].data,
          pipelineMetrics= results[3].data;

        //Definitions
        $scope.pipelineConfigDefinition = definitions.pipeline[0];
        $scope.stageLibraries = definitions.stages;

        $scope.sources = _.filter($scope.stageLibraries, function (stageLibrary) {
          return stageLibrary.type === pipelineConstant.SOURCE_STAGE_TYPE;
        });

        $scope.processors = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === pipelineConstant.PROCESSOR_STAGE_TYPE &&
            stageLibrary.name !== 'com_streamsets_pipeline_lib_stage_processor_selector_SelectorProcessor');
        });

        //TODO: Remove hard coding once backend supports modeling selector type
        $scope.selectorProcessors = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === pipelineConstant.PROCESSOR_STAGE_TYPE &&
            stageLibrary.name === 'com_streamsets_pipeline_lib_stage_processor_selector_SelectorProcessor');
        });

        $scope.targets = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === pipelineConstant.TARGET_STAGE_TYPE);
        });

        //Pipelines
        $scope.pipelines = pipelines;

        $rootScope.common.pipelineStatus = pipelineStatus;

        //Determine Active Config based on localStorage or based on last status updated config.
        if($rootScope.$storage.activeConfigInfo && $rootScope.$storage.activeConfigInfo.name) {
          var localStorageConfigInfoName = $rootScope.$storage.activeConfigInfo.name;
          $scope.activeConfigInfo = $rootScope.$storage.activeConfigInfo = _.find($scope.pipelines, function(pipelineDefn) {
            return pipelineDefn.name === localStorageConfigInfoName;
          });
        } else if(pipelineStatus && pipelineStatus.name) {
          $scope.activeConfigInfo = _.find($scope.pipelines, function(pipelineDefn) {
            return pipelineDefn.name === pipelineStatus.name;
          });
        }

        if(!$scope.activeConfigInfo && $scope.pipelines && $scope.pipelines.length) {
          $scope.activeConfigInfo =   $scope.pipelines[0];
        }

        $rootScope.common.pipelineMetrics = pipelineMetrics;

        refreshPipelineStatus();
        refreshPipelineMetrics();

        if($scope.activeConfigInfo) {
          return $q.all([api.pipelineAgent.getPipelineConfig($scope.activeConfigInfo.name),
            api.pipelineAgent.getPipelineRules($scope.activeConfigInfo.name)]);
        }

      },function(data, status, headers, config) {
          $rootScope.common.errors = [data];
      })
      .then(function(results) {
        //Pipeline Configuration
        if(results && results.length > 1) {
          var config = results[0].data,
            rules = results[1].data;
          updateGraph(config, rules);
          updateDetailPane({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE
          });
        }
        $scope.loaded = true;
      },function(resp) {
        $scope.pipelineConfig = undefined;
        $rootScope.common.errors = [resp.data];
      });

    /**
     * Load Pipeline Configuration by fetching it from server for the given Pipeline Configuration name.
     * @param configName
     */
    var loadPipelineConfig = function(configName) {
      $q.all([api.pipelineAgent.getPipelineConfig(configName),
        api.pipelineAgent.getPipelineRules(configName)]).
        then(function(results) {
          var config = results[0].data,
            rules = results[1].data;
          $rootScope.common.errors = [];
          updateGraph(config, rules);
          updateDetailPane({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE
          });
        },function(resp) {
          $scope.pipelineConfig = undefined;
          $rootScope.common.errors = [resp.data];
        });
    };

    /**
     * Save Updates
     * @param config
     */
    var saveUpdates = function (config) {
      if ($rootScope.common.saveOperationInProgress) {
        return;
      }

      if (!config) {
        config = _.clone($scope.pipelineConfig);
      }

      dirty = false;
      $rootScope.common.saveOperationInProgress = true;
      api.pipelineAgent.savePipelineConfig($scope.activeConfigInfo.name, config).
        success(function (res) {
          $rootScope.common.saveOperationInProgress = false;

          if (dirty) {
            config = _.clone($scope.pipelineConfig);
            config.uuid = res.uuid;

            //Updated new changes in return config
            res.configuration = config.configuration;
            res.uiInfo = config.uiInfo;
            res.stages = config.stages;

            saveUpdates(config);
          }
          updateGraph(res, $scope.pipelineRules);
        }).
        error(function(data, status, headers, config) {
          $rootScope.common.errors = [data];
        });
    };

    /**
     * Update Pipeline Graph
     *
     * @param pipelineConfig
     */
    var updateGraph = function (pipelineConfig, pipelineRules) {
      var selectedStageInstance,
        stageErrorCounts,
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        pipelineStatus = $rootScope.common.pipelineStatus;

      ignoreUpdate = true;

      //Force Validity Check - showErrors directive
      $scope.$broadcast('show-errors-check-validity');

      $scope.pipelineConfig = pipelineConfig || {};
      $scope.activeConfigInfo = $rootScope.$storage.activeConfigInfo = pipelineConfig.info;
      $scope.pipelineRules = pipelineRules;

      //Update Pipeline Info list
      var index = _.indexOf($scope.pipelines, _.find($scope.pipelines, function(pipeline){
        return pipeline.name === pipelineConfig.info.name;
      }));
      $scope.pipelines[index] = pipelineConfig.info;

      stageCounter = ($scope.pipelineConfig && $scope.pipelineConfig.stages) ?
        $scope.pipelineConfig.stages.length : 0;

      //Determine edges from input lanes and output lanes
      //And also set flag sourceExists if pipeline Config contains source
      edges = [];
      $scope.sourceExists = false;
      angular.forEach($scope.pipelineConfig.stages, function (sourceStageInstance) {
        if(sourceStageInstance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE) {
          $scope.sourceExists = true;
        }

        if (sourceStageInstance.outputLanes && sourceStageInstance.outputLanes.length) {
          angular.forEach(sourceStageInstance.outputLanes, function (outputLane) {
            angular.forEach($scope.pipelineConfig.stages, function (targetStageInstance) {
              if (targetStageInstance.inputLanes && targetStageInstance.inputLanes.length &&
                _.contains(targetStageInstance.inputLanes, outputLane)) {
                edges.push({
                  source: sourceStageInstance,
                  target: targetStageInstance,
                  outputLane: outputLane
                });
              }
            });
          });
        }
      });

      $scope.edges = edges;

      $scope.firstOpenLane = $rootScope.$storage.dontShowHelpAlert ? {} : getFirstOpenLane();

      if(pipelineStatus && pipelineStatus.name === pipelineConfig.info.name &&
        pipelineMetrics && pipelineMetrics.meters) {
        stageErrorCounts = getStageErrorCounts();
      }

      $scope.stageSelected = false;

      if ($scope.detailPaneConfig === undefined) {
        //First time
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
      } else {
        //Check

        if ($scope.selectedType === pipelineConstant.PIPELINE) {
          //In case of detail pane is Pipeline Configuration
          $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
          $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        } else if($scope.selectedType === pipelineConstant.STAGE_INSTANCE) {
          //In case of detail pane is stage instance
          angular.forEach($scope.pipelineConfig.stages, function (stageInstance) {
            if (stageInstance.instanceName === $scope.detailPaneConfig.instanceName) {
              selectedStageInstance = stageInstance;
            }
          });

          if (selectedStageInstance) {
            $scope.detailPaneConfig = $scope.selectedObject = selectedStageInstance;
            $scope.stageSelected = true;
          } else {
            $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
            $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
          }
        }
      }

      $timeout(function() {
        $scope.$broadcast('updateGraph', {
          nodes: $scope.pipelineConfig.stages,
          edges: edges,
          issues: $scope.pipelineConfig.issues,
          selectNode: ($scope.selectedType && $scope.selectedType === pipelineConstant.STAGE_INSTANCE) ? $scope.selectedObject : undefined,
          stageErrorCounts: stageErrorCounts,
          showEdgePreviewIcon: $scope.isPipelineRunning,
          isReadOnly: $scope.isPipelineRunning
        });
      });

    };

    /**
     * Update Detail Pane when selection changes in Pipeline Graph.
     *
     * @param options
     */
    var updateDetailPane = function(options) {
      var selectedObject = options.selectedObject,
        type = options.type;

      $scope.selectedType = type;

      if(type === pipelineConstant.STAGE_INSTANCE) {
        $scope.stageSelected = true;
        //Stage Instance Configuration
        $scope.detailPaneConfig = $scope.selectedObject = selectedObject;
        $scope.detailPaneConfigDefn = _.find($scope.stageLibraries, function (stageLibrary) {
          return stageLibrary.name === selectedObject.stageName &&
            stageLibrary.version === selectedObject.stageVersion;
        });
      } else if(type === pipelineConstant.PIPELINE){
        //Pipeline Configuration
        $scope.stageSelected = false;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
      } else if(type === pipelineConstant.LINK) {
        $scope.detailPaneConfig = $scope.selectedObject = selectedObject;
      }

      $scope.$broadcast('onSelectionChange', options);

      $timeout(function () {
        $scope.$broadcast('show-errors-check-validity');
      }, 1000);
    };


    /**
     * Fetch the Pipeline Status every configured refresh interval.
     *
     */
    var refreshPipelineStatus = function() {
      if(destroyed) {
        return;
      }

      pipelineStatusTimer = $timeout(
        function() {
          //console.log( "Pipeline Status Timeout executed", Date.now() );
        },
        configuration.getRefreshInterval()
      );

      pipelineStatusTimer.then(
        function() {
          api.pipelineAgent.getPipelineStatus()
            .success(function(data) {
              $rootScope.common.pipelineStatus = data;
              refreshPipelineStatus();
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        },
        function() {
          //console.log( "Timer rejected!" );
        }
      );
    };


    /**
     * Fetch the Pipeline Status for every configured refresh interval.
     *
     */
    var refreshPipelineMetrics = function() {
      if(destroyed) {
        return;
      }

      pipelineMetricsTimer = $timeout(
        function() {
          //console.log( "Pipeline Metrics Timeout executed", Date.now() );
        },
        configuration.getRefreshInterval()
      );

      pipelineMetricsTimer.then(
        function() {
          api.pipelineAgent.getPipelineMetrics()
            .success(function(data) {
              if(!$scope.monitoringPaused) {
                $rootScope.common.pipelineMetrics = data;
              }
              refreshPipelineMetrics();
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        },
        function() {
          //console.log( "Timer rejected!" );
        }
      );
    };


    var getStageErrorCounts = function() {
      var stageInstanceErrorCounts = {};

      angular.forEach($scope.pipelineConfig.stages, function(stageInstance) {
        stageInstanceErrorCounts[stageInstance.instanceName] = Math.round(
          $rootScope.common.pipelineMetrics.histograms['stage.' + stageInstance.instanceName + '.errorRecords.histogramM5'].mean +
          $rootScope.common.pipelineMetrics.histograms['stage.' + stageInstance.instanceName + '.stageErrors.histogramM5'].mean
        );
      });

      return stageInstanceErrorCounts;
    };

    var getFirstOpenLane = function() {
      var pipelineConfig = $scope.pipelineConfig,
        firstOpenLane = {},
        issueMessage,
        firstOpenLaneStageInstanceName;

      if(pipelineConfig && pipelineConfig.issues && pipelineConfig.issues.stageIssues) {
        angular.forEach(pipelineConfig.issues.stageIssues, function(issues, instanceName) {
          if(!firstOpenLaneStageInstanceName) {
            angular.forEach(issues, function(issue) {
              if(issue.message.indexOf('VALIDATION_0011') !== -1) {
                issueMessage = issue.message;
                firstOpenLaneStageInstanceName = instanceName;
              }
            });
          }
        });

        if(firstOpenLaneStageInstanceName) {
          var stageInstance = _.find(pipelineConfig.stages, function(stage) {
              return stage.instanceName === firstOpenLaneStageInstanceName;
            }),
            laneName = _.find(stageInstance.outputLanes, function(outputLane) {
              return issueMessage.indexOf(outputLane) !== -1;
            }),
            laneIndex = _.indexOf(stageInstance.outputLanes, laneName);

          firstOpenLane = {
            stageInstance: stageInstance,
            laneName: laneName,
            laneIndex: laneIndex
          };
        }
      }



      return firstOpenLane;
    };

    var derivePipelineRunning = function() {
      var pipelineStatus = $rootScope.common.pipelineStatus,
        config = $scope.pipelineConfig;
      return (pipelineStatus && config && pipelineStatus.name === config.info.name &&
      pipelineStatus.state === 'RUNNING');
    };

    var derivePipelineStatus = function() {
      var pipelineStatus = $rootScope.common.pipelineStatus,
        config = $scope.pipelineConfig;

      if(pipelineStatus && config && pipelineStatus.name === config.info.name) {
        return pipelineStatus;
      } else {
        return {
          state: 'STOPPED'
        };
      }
    };


    /**
     * Save Rules Update
     * @param rules
     */
    var saveRulesUpdate = function (rules) {
      if ($rootScope.common.saveOperationInProgress) {
        return;
      }

      if (!rules) {
        rules = _.clone($scope.pipelineRules);
      }

      rulesDirty = false;

      $rootScope.common.saveOperationInProgress = true;
      api.pipelineAgent.savePipelineRules($scope.activeConfigInfo.name, rules).
        success(function (res) {
          $rootScope.common.saveOperationInProgress = false;
          if (rulesDirty) {
            rules = _.clone($scope.pipelineRules);
            saveRulesUpdate(rules);
          }
        }).
        error(function(data, status, headers, config) {
          $rootScope.common.errors = [data];
        });
    };

    //Event Handling

    $scope.$watch('pipelineConfig', function (newValue, oldValue) {
      if(newValue === undefined) {
        return;
      }

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

    $scope.$watch('pipelineRules', function (newValue, oldValue) {
      if (ignoreUpdate) {
        $timeout(function () {
          ignoreUpdate = false;
        });
        return;
      }
      if (!angular.equals(newValue, oldValue)) {
        rulesDirty = true;
        if (timeout) {
          $timeout.cancel(timeout);
        }
        timeout = $timeout(saveRulesUpdate, 1000);
      }
    }, true);

    $scope.$on('onNodeSelection', function (event, stageInstance) {
      updateDetailPane({
        selectedObject: stageInstance,
        type: pipelineConstant.STAGE_INSTANCE
      });
    });

    $scope.$on('onEdgeSelection', function (event, edge) {
      updateDetailPane({
        selectedObject: edge,
        type: pipelineConstant.LINK
      });
    });

    $scope.$on('onRemoveNodeSelection', function () {
      updateDetailPane({
        selectedObject: undefined,
        type: pipelineConstant.PIPELINE
      });
    });

    $scope.$on('onPipelineConfigSelect', function(event, configInfo) {
      if(configInfo) {
        $scope.activeConfigInfo = configInfo;
        $scope.closePreview();
        loadPipelineConfig($scope.activeConfigInfo.name);
      } else {
        //No Pipieline config exists
        ignoreUpdate = true;
        $scope.pipelineConfig = undefined;
        $scope.hideLibraryPanel = true;
      }
    });

    //Preview Panel Events
    $scope.$on('changeStateInstance', function (event, stageInstance) {
      updateDetailPane({
        selectedObject: stageInstance,
        type: pipelineConstant.STAGE_INSTANCE
      });
    });

    $scope.$watch('pipelineConfig.info.name', function() {
      $scope.isPipelineRunning = derivePipelineRunning();
      $scope.activeConfigStatus = derivePipelineStatus();
    });

    $rootScope.$watch('common.pipelineStatus', function() {
      $scope.isPipelineRunning = derivePipelineRunning();
      $scope.activeConfigStatus = derivePipelineStatus();
    });

    $rootScope.$watch('common.pipelineMetrics', function() {
      var pipelineStatus = $rootScope.common.pipelineStatus,
        config = $scope.pipelineConfig;
      if(pipelineStatus && config && pipelineStatus.name === config.info.name &&
        $scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {
        $scope.$broadcast('updateErrorCount', getStageErrorCounts());
        $scope.triggeredAlerts = pipelineService.getTriggeredAlerts($scope.pipelineRules,
          $rootScope.common.pipelineMetrics);
      } else {
        $scope.triggeredAlerts = [];
      }
    });

    $scope.$on('$destroy', function() {
      $timeout.cancel(pipelineStatusTimer);
      $timeout.cancel(pipelineMetricsTimer);
      destroyed = true;
    });

    $scope.$on('visibilityChange', function(event, isHidden) {
      if (isHidden) {
        $timeout.cancel(pipelineStatusTimer);
        $timeout.cancel(pipelineMetricsTimer);
        pageHidden = true;
      } else {
        refreshPipelineMetrics();
        refreshPipelineStatus();
        pageHidden = false;
      }
    });

  });