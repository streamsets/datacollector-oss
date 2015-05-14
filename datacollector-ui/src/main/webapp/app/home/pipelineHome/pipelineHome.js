/**
 * Pipeline Home module for displaying Pipeline home page content.
 */

angular
  .module('dataCollectorApp.home')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider
      .when('/collector/pipeline/:pipelineName', {
        templateUrl: 'app/home/pipelineHome/pipelineHome.tpl.html',
        controller: 'PipelineHomeController',
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
  .controller('PipelineHomeController', function ($scope, $rootScope, $routeParams, $timeout, api, configuration, _, $q, $modal,
                                          $localStorage, pipelineService, pipelineConstant, visibilityBroadcaster,
                                          $translate, contextHelpService, $location, authService, userRoles, Analytics) {
    var routeParamPipelineName = $routeParams.pipelineName,
      configTimeout,
      configDirty = false,
      configSaveInProgress = false,
      rulesTimeout,
      rulesDirty = false,
      rulesSaveInProgress = false,
      ignoreUpdate = false,
      pipelineMetricsTimer,
      edges = [],
      destroyed = false,
      pageHidden = false,
      isWebSocketSupported = (typeof(WebSocket) === "function"),
      loc = window.location,
      webSocketBaseURL = ((loc.protocol === "https:") ?
          "wss://" : "ws://") + loc.hostname + (((loc.port != 80) && (loc.port != 443)) ? ":" + loc.port : ""),
      webSocketMetricsURL = webSocketBaseURL + '/rest/v1/webSocket?type=metrics',
      metricsWebSocket,
      webSocketAlertsURL = webSocketBaseURL + '/rest/v1/webSocket?type=alerts',
      alertsWebSocket;

    configuration.init().then(function() {
      if(configuration.isAnalyticsEnabled()) {
        Analytics.trackPage('/collector/pipeline/' + routeParamPipelineName);
      }
    });

    angular.extend($scope, {
      _: _,
      isPipelineReadOnly: !authService.isAuthorized([userRoles.admin, userRoles.creator]),
      isPipelineRulesReadOnly: !authService.isAuthorized([userRoles.admin, userRoles.creator, userRoles.manager]),
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
       * @param options
       *  stage
       *  firstOpenLane [optional]
       *  relativeXPos [optional]
       *  relativeYPos [optional]
       *  configuration [optional]
       *  insertBetweenEdge [optional]
       *
       * @param event
       */
      addStageInstance: function (options, event) {
        var stage = options.stage,
          firstOpenLane = options.firstOpenLane,
          relativeXPos = options.relativeXPos,
          relativeYPos = options.relativeYPos,
          configuration = options.configuration,
          insertBetweenEdge = options.insertBetweenEdge,
          stageInstance,
          edges = $scope.edges,
          edge;

        if(event) {
          event.preventDefault();
        }

        if($scope.isPipelineReadOnly) {
          return;
        }

        if(stage.type === pipelineConstant.SOURCE_STAGE_TYPE) {
          var sourceExists = false;
          angular.forEach($scope.pipelineConfig.stages, function (sourceStageInstance) {
            if (sourceStageInstance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE) {
              sourceExists = true;
            }
          });

          if(sourceExists) {
            $translate('global.messages.info.originExists').then(function(translation) {
              $rootScope.common.errors = [translation];
            });
            return;
          } else {
            $rootScope.common.errors = [];
          }
        } else {
          $rootScope.common.errors = [];
        }

        $scope.previewMode = false;

        stageInstance = pipelineService.getNewStageInstance({
          stage: stage,
          pipelineConfig: $scope.pipelineConfig,
          firstOpenLane: firstOpenLane,
          relativeXPos: relativeXPos,
          relativeYPos: relativeYPos,
          configuration: configuration,
          insertBetweenEdge: insertBetweenEdge
        });

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
          edges.push(edge);
        }

        if(insertBetweenEdge) {
          edges = _.filter(edges, function(e) {
            return (e.outputLane !== insertBetweenEdge.outputLane &&
              e.source.instanceName !== insertBetweenEdge.source.instanceName &&
              e.target.instanceName !== insertBetweenEdge.target.instanceName);
          });

          edges.push({
            source: insertBetweenEdge.source,
            target: stageInstance,
            outputLane: insertBetweenEdge.outputLane
          });

          edges.push({
            source: stageInstance,
            target: insertBetweenEdge.target,
            outputLane: stageInstance.outputLanes[0]
          });
        }

        $scope.$broadcast('addNode', stageInstance, edges, relativeXPos, relativeYPos);
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
       */
      previewPipeline: function () {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/preview/configuration/previewConfigModal.tpl.html',
          controller: 'PreviewConfigModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineConfig: function () {
              return $scope.pipelineConfig;
            }
          }
        });

        modalInstance.result.then(function () {
          $scope.previewMode = true;
          $rootScope.$storage.maximizeDetailPane = false;
          $rootScope.$storage.minimizeDetailPane = false;
          $scope.setGraphReadOnly(true);
          $scope.setGraphPreviewMode(true);
          $scope.$broadcast('previewPipeline');
        }, function () {

        });
      },

      /**
       * Sets previewMode flag to false.
       */
      closePreview: function () {
        $scope.previewMode = false;
        $scope.setGraphReadOnly(false);
        $scope.setGraphPreviewMode(false);
        //$scope.moveGraphToCenter();
      },

      /**
       * Capture the snapshot of running pipeline.
       *
       */
      viewSnapshot: function(snapshotName) {
        $scope.snapshotMode = true;
        $scope.snapshotName = snapshotName;
        $rootScope.$storage.maximizeDetailPane = false;
        $rootScope.$storage.minimizeDetailPane = false;
        $scope.setGraphPreviewMode(true);
        $scope.$broadcast('snapshotPipeline', snapshotName);
      },

      setSnapshotName: function(snapshotName) {
        $scope.snapshotName = snapshotName;
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
          if(options.selectedObject && options.selectedObject.instanceName) {
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
       *
       */
      refreshGraph : function() {
        updateGraph($scope.pipelineConfig, $scope.pipelineRules, true);
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
      },

      /**
       * Returns label of the Stage Instance.
       *
       * @param stageInstanceName
       * @returns {*|string}
       */
      getStageInstanceLabel: function (stageInstanceName) {
        var instance,
          errorStage = $scope.pipelineConfig.errorStage;

        angular.forEach($scope.pipelineConfig.stages, function (stageInstance) {
          if (stageInstance.instanceName === stageInstanceName) {
            instance = stageInstance;
          }
        });

        if(!instance && errorStage && errorStage.instanceName === stageInstanceName) {
          instance = errorStage;
        }

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

        if (issue.configName) {
          var stageInstance = _.find($scope.pipelineConfig.stages, function (stage) {
            return stage.instanceName === stageInstanceName;
          });

          if (stageInstance) {
            msg += ' : ' + pipelineService.getConfigurationLabel(stageInstance, issue.configName);
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

        if(instanceName) {
          //Select stage instance
          stageInstance = _.find(pipelineConfig.stages, function(stage) {
            return stage.instanceName === instanceName;
          });

          if(stageInstance) {
            $scope.changeStageSelection({
              selectedObject: stageInstance,
              type: pipelineConstant.STAGE_INSTANCE,
              detailTabName: 'configuration',
              configGroup: issue.configGroup,
              configName: issue.configName
            });
          } else if(pipelineConfig.errorStage && pipelineConfig.errorStage.instanceName === instanceName){
            //Error Stage Configuration Issue
            $scope.$broadcast('selectNode');
            $scope.changeStageSelection({
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE,
              detailTabName: 'configuration',
              configGroup: issue.configGroup,
              configName: issue.configName,
              errorStage: true
            });
          }

        } else {
          //Select Pipeline Config
          $scope.$broadcast('selectNode');
          $scope.changeStageSelection({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE,
            detailTabName: 'configuration',
            configGroup: issue.configGroup,
            configName: issue.configName
          });
        }
      },

      /**
       * Launch Contextual Help
       */
      launchHelp: function(helpId) {
        contextHelpService.launchHelp(helpId);
      },

      /**
       * Delete Triggered Alert
       */
      deleteTriggeredAlert: function(triggeredAlert, event) {

        if(event) {
          event.preventDefault();
          event.stopPropagation();
        }

        $scope.triggeredAlerts = _.filter($scope.triggeredAlerts, function(alert) {
          return alert.rule.id !== triggeredAlert.rule.id;
        });

        api.pipelineAgent.deleteAlert($scope.pipelineConfig.info.name, triggeredAlert.rule.id)
          .success(function() {
            //Alert deleted successfully
            /*$rootScope.$storage.readNotifications = _.filter($rootScope.$storage.readNotifications, function(alertId) {
              return alertId !== triggeredAlert.rule.id;
            });*/
          })
          .error(function(data, status, headers, config) {
            $rootScope.common.errors = [data];
          });
      }
    });

    $rootScope.common.errors = [];

    /**
     * Fetch definitions for Pipeline and Stages, fetch all pipeline configuration info, status and metric.
     */
    $q.all([
      api.pipelineAgent.getPipelineStatus(),
      //api.pipelineAgent.getPipelineMetrics(),
      pipelineService.init(),
      configuration.init()
    ])
      .then(function (results) {
        var pipelineStatus = results[0].data;

        //Definitions
        $scope.pipelineConfigDefinition = pipelineService.getPipelineConfigDefinition();
        $scope.stageLibraries = pipelineService.getStageDefinitions();

        $scope.sources = _.filter($scope.stageLibraries, function (stageLibrary) {
          return stageLibrary.type === pipelineConstant.SOURCE_STAGE_TYPE;
        });

        $scope.processors = _.filter($scope.stageLibraries, function (stageLibrary) {
          return stageLibrary.type === pipelineConstant.PROCESSOR_STAGE_TYPE;
        });

        $scope.targets = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === pipelineConstant.TARGET_STAGE_TYPE && !stageLibrary.errorStage);
        });

        //Pipelines
        $scope.pipelines = pipelineService.getPipelines();

        $rootScope.common.pipelineStatus = pipelineStatus;
        //$rootScope.common.pipelineMetrics = pipelineMetrics;

        $scope.activeConfigInfo = _.find($scope.pipelines, function(pipelineDefn) {
          return pipelineDefn.name === routeParamPipelineName;
        });

        refreshPipelineMetrics();

        if($rootScope.common.sdcExecutionMode !== pipelineConstant.CLUSTER) {
          initializeAlertWebSocket();
        }

        if($scope.activeConfigInfo) {
          return $q.all([api.pipelineAgent.getPipelineConfig($scope.activeConfigInfo.name),
            api.pipelineAgent.getPipelineRules($scope.activeConfigInfo.name)]);
        } else {
          $translate('global.messages.info.noPipelineExists', {name: routeParamPipelineName}).then(function(translation) {
            $rootScope.common.errors = [translation];
          });

          $location.path('/');
          $location.replace();
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
      if (configSaveInProgress || $scope.isPipelineReadOnly ||
        $rootScope.common.sdcExecutionMode === pipelineConstant.SLAVE) {
        return;
      }

      if (!config) {
        config = _.clone($scope.pipelineConfig);
      }

      configDirty = false;
      configSaveInProgress = true;
      $rootScope.common.saveOperationInProgress++;

      api.pipelineAgent.savePipelineConfig($scope.activeConfigInfo.name, config).
        success(function (res) {

          //Clear Previous errors
          $rootScope.common.errors = [];

          configSaveInProgress = false;
          $rootScope.common.saveOperationInProgress--;
          if (configDirty) {
            config = _.clone($scope.pipelineConfig);
            config.uuid = res.uuid;

            //Updated new changes in return config
            res.configuration = config.configuration;
            res.uiInfo = config.uiInfo;
            res.stages = config.stages;
            res.errorStage = config.errorStage;

            saveUpdates(config);
          }

          updateGraph(res, $scope.pipelineRules);
        }).
        error(function(data, status, headers, config) {
          configSaveInProgress = false;
          $rootScope.common.saveOperationInProgress--;
          $rootScope.common.errors = [data];
        });
    };

    /**
     * Update Pipeline Graph
     *
     * @param pipelineConfig
     * @param pipelineRules
     * @param manualUpdate
     */
    var updateGraph = function (pipelineConfig, pipelineRules, manualUpdate) {
      var selectedStageInstance,
        stageErrorCounts = {},
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        pipelineStatus = $rootScope.common.pipelineStatus;

      if(!manualUpdate) {
        ignoreUpdate = true;
      }

      //Force Validity Check - showErrors directive
      $scope.$broadcast('show-errors-check-validity');

      $scope.pipelineConfig = pipelineConfig || {};
      $scope.activeConfigInfo = $rootScope.$storage.activeConfigInfo = pipelineConfig.info;
      $scope.pipelineRules = pipelineRules;

      //Initialize the pipeline config
      if(!$scope.pipelineConfig.uiInfo) {
        $scope.pipelineConfig.uiInfo = {
          previewConfig : {
            previewSource: pipelineConstant.CONFIGURED_SOURCE,
            batchSize: 10,
            writeToDestinations: false
          }
        };

        //Load Metric Rules

        if(!pipelineRules.metricsRuleDefinitions || pipelineRules.metricsRuleDefinitions.length === 0) {
          pipelineRules.metricsRuleDefinitions = pipelineService.getPredefinedMetricAlertRules($scope.pipelineConfig.info.name);
          saveRulesUpdate(pipelineRules);
        }
      }

      //Update Pipeline Info list
      var index = _.indexOf($scope.pipelines, _.find($scope.pipelines, function(pipeline){
        return pipeline.name === pipelineConfig.info.name;
      }));
      $scope.pipelines[index] = pipelineConfig.info;

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
        pipelineStatus.state === 'RUNNING' && pipelineMetrics && pipelineMetrics.meters) {
        stageErrorCounts = getStageErrorCounts();
      }

      $scope.stageSelected = false;

      if ($scope.detailPaneConfig === undefined) {
        //First time
        $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
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

      if ($scope.selectedType === pipelineConstant.PIPELINE) {
        var errorStage = $scope.pipelineConfig.errorStage;
        if(errorStage && errorStage.configuration && errorStage.configuration.length) {
          $scope.errorStageConfig = errorStage;
          $scope.errorStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === errorStage.library &&
              stageLibrary.name === errorStage.stageName &&
              stageLibrary.version === errorStage.stageVersion;
          });
        } else {
          $scope.errorStageConfig = undefined;
          $scope.errorStageConfigDefn = undefined;
        }
      }

      $timeout(function() {
        $scope.$broadcast('updateGraph', {
          nodes: $scope.pipelineConfig.stages,
          edges: edges,
          issues: $scope.pipelineConfig.issues,
          selectNode: ($scope.selectedType && $scope.selectedType === pipelineConstant.STAGE_INSTANCE) ? $scope.selectedObject : undefined,
          selectEdge: ($scope.selectedType && $scope.selectedType === pipelineConstant.LINK) ? $scope.selectedObject : undefined,
          stageErrorCounts: stageErrorCounts,
          showEdgePreviewIcon: true,
          isReadOnly: $scope.isPipelineReadOnly || $scope.isPipelineRunning || $scope.previewMode,
          pipelineRules: $scope.pipelineRules,
          triggeredAlerts: $scope.triggeredAlerts,
          errorStage: $scope.pipelineConfig.errorStage
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
        type = options.type,
        errorStage = $scope.pipelineConfig.errorStage,
        stageLibraryList = [],
        optionsLength = Object.keys(options).length;


      if(!$scope.previewMode && !$scope.snapshotMode && $scope.selectedType === type && $scope.selectedObject && selectedObject && optionsLength <= 2 &&
        ((type === pipelineConstant.PIPELINE && $scope.selectedObject.info.name === selectedObject.info.name) ||
          (type === pipelineConstant.STAGE_INSTANCE && $scope.selectedObject.instanceName === selectedObject.instanceName))) {
        //Previous selection remain same
        return;
      }

      $scope.selectedType = type;
      $scope.errorStageConfig = undefined;
      $scope.errorStageConfigDefn = undefined;


      if(options.configName) {
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;
      }

      if(type === pipelineConstant.STAGE_INSTANCE) {
        $scope.stageSelected = true;
        //Stage Instance Configuration
        $scope.detailPaneConfig = $scope.selectedObject = selectedObject;

        _.each($scope.stageLibraries, function (stageLibrary) {
          if(stageLibrary.name === selectedObject.stageName &&
            stageLibrary.version === selectedObject.stageVersion) {

            if(stageLibrary.library === selectedObject.library) {
              $scope.detailPaneConfigDefn = stageLibrary;
            }

            stageLibraryList.push({
              library: stageLibrary.library,
              libraryLabel: stageLibrary.libraryLabel
            });
          }
        });

        $scope.stageLibraryList = stageLibraryList;

        if(!options.detailTabName) {
          if($scope.isPipelineRunning) {
            options.detailTabName = 'summary';
          } else {
            options.detailTabName = 'configuration';
          }
        }

      } else if(type === pipelineConstant.PIPELINE){
        //Pipeline Configuration
        $scope.stageSelected = false;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;

        if(errorStage && errorStage.configuration && errorStage.configuration.length) {
          $scope.errorStageConfig = errorStage;
          $scope.errorStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === errorStage.library &&
            stageLibrary.name === errorStage.stageName &&
            stageLibrary.version === errorStage.stageVersion;
          });
        }

        if(!options.detailTabName) {
          if($scope.isPipelineRunning) {
            options.detailTabName = 'summary';
          } else {
            options.detailTabName = 'configuration';
          }
        }

      } else if(type === pipelineConstant.LINK) {
        $scope.detailPaneConfig = $scope.selectedObject = selectedObject;

        if(!options.detailTabName) {
          if($scope.isPipelineRunning) {
            options.detailTabName = 'summary';
          } else {
            options.detailTabName = 'dataRules';
          }
        }
      }

      $scope.$broadcast('onSelectionChange', options);

      $timeout(function () {
        $scope.$broadcast('show-errors-check-validity');
      }, 1000);
    };

    /**
     * Fetch the Pipeline Status for every configured refresh interval.
     *
     */
    var refreshPipelineMetrics = function() {
      if(destroyed) {
        return;
      }

      if(isWebSocketSupported) {

        //WebSocket to get Pipeline Metrics
        metricsWebSocket = new WebSocket(webSocketMetricsURL);

        metricsWebSocket.onmessage = function (evt) {
          var received_msg = evt.data;

          if(!$scope.monitoringPaused) {
            $rootScope.$apply(function() {
              $rootScope.common.pipelineMetrics = JSON.parse(received_msg);
            });

          }
        };

        metricsWebSocket.onerror = function (evt) {
          isWebSocketSupported = false;
          refreshPipelineMetrics();
        };

      } else {

        //WebSocket is not support use polling to get Pipeline Metrics
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
                if(!_.isObject(data) && _.isString(data) && data.indexOf('<!doctype html>') !== -1) {
                  //Session invalidated
                  window.location.reload();
                  return;
                }

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

      }
    };

    var initializeAlertWebSocket = function() {
      if(isWebSocketSupported && 'Notification' in window) {
        Notification.requestPermission(function(permission) {
          alertsWebSocket = new WebSocket(webSocketAlertsURL);
          alertsWebSocket.onmessage = function (evt) {
            var received_msg = evt.data;
            if(received_msg) {
              var alertDefn = JSON.parse(received_msg),
                pipelineStatus = $rootScope.common.pipelineStatus;
              var notification = new Notification(pipelineStatus.name, {
                body: alertDefn.alertText,
                icon: '/assets/favicon.png'
              });
            }
          };
        });
      }
    };

    var getStageErrorCounts = function() {
      var stageInstanceErrorCounts = {},
        pipelineMetrics = $rootScope.common.pipelineMetrics;

      if(pipelineMetrics && pipelineMetrics.meters) {
        angular.forEach($scope.pipelineConfig.stages, function(stageInstance) {
          var errorRecordsMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + '.errorRecords.meter'],
            stageErrorsMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + '.stageErrors.meter'];

          if(errorRecordsMeter && stageErrorsMeter) {
            stageInstanceErrorCounts[stageInstance.instanceName] = Math.round(
              errorRecordsMeter.count +
              stageErrorsMeter.count
            );
          } else {
            // Failed to fetch metrics
            // $rootScope.common.errors = ['Failed to fetch pipeline metrics'];
            console.log('Failed to fetch stage meter metrics');
          }

        });
      }

      return stageInstanceErrorCounts;
    };

    var getFirstOpenLane = function() {
      var pipelineConfig = $scope.pipelineConfig,
        firstOpenLane = {},
        issueObj,
        firstOpenLaneStageInstanceName;

      if(pipelineConfig && pipelineConfig.issues && pipelineConfig.issues.stageIssues) {
        angular.forEach(pipelineConfig.issues.stageIssues, function(issues, instanceName) {
          if(!firstOpenLaneStageInstanceName) {
            angular.forEach(issues, function(issue) {
              if(issue.message.indexOf('VALIDATION_0011') !== -1) {
                issueObj = issue;
                firstOpenLaneStageInstanceName = instanceName;
              }
            });
          }
        });

        if(firstOpenLaneStageInstanceName && issueObj &&
          issueObj.additionalInfo && issueObj.additionalInfo.openStreams) {
          var stageInstance = _.find(pipelineConfig.stages, function(stage) {
              return stage.instanceName === firstOpenLaneStageInstanceName;
            }),
            laneName = issueObj.additionalInfo.openStreams[0],
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
      if (rulesSaveInProgress || $scope.isPipelineRulesReadOnly) {
        return;
      }

      rulesDirty = false;

      if (!rules) {
        rules = _.clone($scope.pipelineRules);
      }

      rulesSaveInProgress = true;
      $rootScope.common.saveOperationInProgress++;

      api.pipelineAgent.savePipelineRules($scope.activeConfigInfo.name, rules).
        success(function (res) {
          rulesSaveInProgress = false;
          $rootScope.common.saveOperationInProgress--;
          ignoreUpdate = true;
          rules = $scope.pipelineRules;
          rules.ruleIssues = res.ruleIssues;
          rules.uuid = res.uuid;

          angular.forEach(rules.metricsRuleDefinitions, function(rule, index) {
            var savedRule = _.find(res.metricsRuleDefinitions, function(savedRule) {
              return savedRule.id === rule.id;
            });

            if(savedRule) {
              rule.valid = savedRule.valid;
            }
          });

          angular.forEach(rules.dataRuleDefinitions, function(rule, index) {
            var savedRule = _.find(res.dataRuleDefinitions, function(savedRule) {
              return savedRule.id === rule.id;
            });

            if(savedRule) {
              rule.valid = savedRule.valid;
            }
          });

          if (rulesDirty) {
            saveRulesUpdate(rules);
          }

          $scope.$broadcast('updateEdgePreviewIconColor', $scope.pipelineRules, []);
        }).
        error(function(data, status, headers, config) {
          $rootScope.common.saveOperationInProgress--;
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
        configDirty = true;

        //badRecordsHandling check
        var badRecordHandlingConfig = _.find(newValue.configuration, function(c) {
              return c.name === 'badRecordsHandling';
            }),
            badRecordHandlingConfigArr = (badRecordHandlingConfig && badRecordHandlingConfig.value) ?
              (badRecordHandlingConfig.value).split('::') : undefined,
            errorStageInst = newValue.errorStage;

        if((badRecordHandlingConfigArr && badRecordHandlingConfigArr.length === 3) &&
            (!errorStageInst || errorStageInst.library !== badRecordHandlingConfigArr[0] ||
            errorStageInst.stageName !== badRecordHandlingConfigArr[1] ||
            errorStageInst.stageVersion !== badRecordHandlingConfigArr[2])) {

          var badRecordsStage = _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === badRecordHandlingConfigArr[0] &&
              stageLibrary.name === badRecordHandlingConfigArr[1] &&
              stageLibrary.version === badRecordHandlingConfigArr[2];
          });

          newValue.errorStage = pipelineService.getNewStageInstance({
            stage: badRecordsStage,
            pipelineConfig: $scope.pipelineConfig,
            errorStage: true
          });
        }

        if (configTimeout) {
          $timeout.cancel(configTimeout);
        }
        configTimeout = $timeout(saveUpdates, 1000);
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
        if (rulesTimeout) {
          $timeout.cancel(rulesTimeout);
        }
        rulesTimeout = $timeout(saveRulesUpdate, 1000);
      }
    }, true);

    $scope.$on('onNodeSelection', function (event, options) {
      updateDetailPane(options);
    });


    $scope.$on('onPasteNode', function (event, stageInstance) {
      var stageLibraries = $scope.stageLibraries,
        newStage = _.find(stageLibraries, function(stage) {
          return stage.library === stageInstance.library && stage.name === stageInstance.stageName;
        });

      $scope.addStageInstance({
        stage: newStage,
        configuration: angular.copy(stageInstance.configuration)
      });
    });

    $scope.$on('onEdgeSelection', function (event, edge) {
      updateDetailPane({
        selectedObject: edge,
        type: pipelineConstant.LINK
      });
    });

    $scope.$on('onRemoveNodeSelection', function (event, options) {
      updateDetailPane(options);
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
      var oldActiveConfigStatus = $scope.activeConfigStatus;

      $scope.isPipelineRunning = derivePipelineRunning();
      $scope.activeConfigStatus = derivePipelineStatus();

      if(oldActiveConfigStatus.lastStatusChange !== $scope.activeConfigStatus.lastStatusChange &&
        $scope.activeConfigStatus.state === 'ERROR') {
        $rootScope.common.errors = [$scope.activeConfigStatus.message];
      }
    });

    $rootScope.$watch('common.pipelineMetrics', function() {
      var pipelineStatus = $rootScope.common.pipelineStatus,
        config = $scope.pipelineConfig;
      if(pipelineStatus && config && pipelineStatus.name === config.info.name &&
        $scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {
        $scope.$broadcast('updateErrorCount', getStageErrorCounts());
        $scope.triggeredAlerts = pipelineService.getTriggeredAlerts($scope.pipelineRules,
          $rootScope.common.pipelineMetrics);
        $scope.$broadcast('updateEdgePreviewIconColor', $scope.pipelineRules, $scope.triggeredAlerts);
      } else {
        $scope.triggeredAlerts = [];
      }
    });

    $scope.$on('$destroy', function() {
      if(isWebSocketSupported) {
        if(metricsWebSocket) {
          metricsWebSocket.close();
        }
        if(alertsWebSocket) {
          alertsWebSocket.close();
        }
      } else {
        $timeout.cancel(pipelineMetricsTimer);
      }

      destroyed = true;
    });

    $scope.$on('visibilityChange', function(event, isHidden) {
      if (isHidden) {
        if(isWebSocketSupported && metricsWebSocket) {
          metricsWebSocket.close();
        } else if(!isWebSocketSupported){
          $timeout.cancel(pipelineMetricsTimer);
        }
        pageHidden = true;
      } else {
        refreshPipelineMetrics();
        pageHidden = false;
      }
    });

  });