/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  .controller('PipelineHomeController', function (
    $scope, $rootScope, $routeParams, $timeout, api, configuration, _, $q, $modal, $localStorage, pipelineService,
    pipelineConstant, visibilityBroadcaster, $translate, contextHelpService, $location, authService, userRoles,
    Analytics
  ) {
    var routeParamPipelineName = $routeParams.pipelineName;
    var configTimeout;
    var configDirty = false;
    var configSaveInProgress = false;
    var rulesTimeout;
    var rulesDirty = false;
    var rulesSaveInProgress = false;
    var ignoreUpdate = false;
    var pipelineMetricsTimer;
    var edges = [];
    var destroyed = false;
    var pageHidden = false;
    var isWebSocketSupported;
    var webSocketMetricsURL = $rootScope.common.webSocketBaseURL + 'rest/v1/webSocket?type=metrics&pipelineName=' +
      routeParamPipelineName;
    var metricsWebSocket;
    var undoLimit = 10;
    var archive = [];
    var currArchivePos = null;
    var archiveOp = false;
    var reloadingNew = false;
    var retryCountDownTimer;
    var infoNameWatchListener;
    var pipelineStatusWatchListener;
    var metricsWatchListener;
    var errorsWatchListener;

    //Remove search parameter if any, search parameter causing canvas arrow issue
    $location.search({});

    //Clear potentially remaining notifications from previous page views
    $rootScope.common.errors = [];
    $rootScope.common.successList = [];

    angular.extend($scope, {
      _: _,
      showLoading: true,
      monitorMemoryEnabled: false,
      isPipelineReadOnly: !authService.isAuthorized([userRoles.admin, userRoles.creator]),
      isPipelineRulesReadOnly: !authService.isAuthorized([userRoles.admin, userRoles.creator, userRoles.manager]),
      isRemotePipeline: false,
      selectedType: pipelineConstant.PIPELINE,
      loaded: false,
      isPipelineRunning: false,
      sourceExists: false,
      stageLibraries: [],
      pipelineGraphData: {},
      previewMode: false,
      snapshotMode: false,
      hideLibraryPanel: true,
      activeConfigInfo: undefined,
      activeConfigStatus:{
        status: 'STOPPED'
      },
      minimizeDetailPane: false,
      maximizeDetailPane: false,
      selectedDetailPaneTabCache: {},
      selectedConfigGroupCache: {},
      existingPipelineLabels: [],
      canWrite: false,
      canExecute: false,
      isDPMPipelineDirty: false,

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
        var stage = options.stage;
        var firstOpenLane = options.firstOpenLane;
        var relativeXPos = options.relativeXPos;
        var relativeYPos = options.relativeYPos;
        var configuration = options.configuration;
        var insertBetweenEdge = options.insertBetweenEdge;
        var stageInstance;
        var edges = $scope.edges;
        var edge;

        if (event) {
          event.preventDefault();
        }

        if ($scope.isPipelineReadOnly) {
          return;
        }

        $scope.trackEvent(pipelineConstant.STAGE_CATEGORY, pipelineConstant.ADD_ACTION, stage.label, 1);

        if (stage.type === pipelineConstant.SOURCE_STAGE_TYPE) {
          var sourceExists = false;
          angular.forEach($scope.pipelineConfig.stages, function (sourceStageInstance) {
            if (sourceStageInstance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE) {
              sourceExists = true;
            }
          });

          if (sourceExists) {
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

        if (firstOpenLane && firstOpenLane.stageInstance) {
          if (firstOpenLane.isEventLane) {
            edge = {
              source: firstOpenLane.stageInstance,
              target: stageInstance,
              eventLane: firstOpenLane.laneName
            };
          } else {
            edge = {
              source: firstOpenLane.stageInstance,
              target: stageInstance,
              outputLane: firstOpenLane.laneName
            };
          }
          edges.push(edge);
        }

        if (insertBetweenEdge) {
          if (insertBetweenEdge.outputLane) {
            edges = _.filter(edges, function(e) {
              return e.eventLane || (e.outputLane !== insertBetweenEdge.outputLane &&
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
          } else if (insertBetweenEdge.eventLane) {
            edges = _.filter(edges, function(e) {
              return e.outputLane || (e.eventLane !== insertBetweenEdge.eventLane &&
              e.source.instanceName !== insertBetweenEdge.source.instanceName &&
              e.target.instanceName !== insertBetweenEdge.target.instanceName);
            });

            edges.push({
              source: insertBetweenEdge.source,
              target: stageInstance,
              eventLane: insertBetweenEdge.eventLane
            });

            edges.push({
              source: stageInstance,
              target: insertBetweenEdge.target,
              outputLane: stageInstance.outputLanes[0]
            });
          }
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
      previewPipeline: function (showPreviewConfig) {
        //Clear Previous errors
        $rootScope.common.errors = [];
        if (!$scope.pipelineConfig.uiInfo.previewConfig.rememberMe || showPreviewConfig) {
          $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Preview Pipeline Configuration', 1);
          var modalInstance = $modal.open({
            templateUrl: 'app/home/preview/configuration/previewConfigModal.tpl.html',
            controller: 'PreviewConfigModalInstanceController',
            size: '',
            backdrop: 'static',
            resolve: {
              pipelineConfig: function () {
                return $scope.pipelineConfig;
              },
              pipelineStatus: function () {
                return $scope.activeConfigStatus;
              }
            }
          });

          modalInstance.result.then(function () {
            $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Run Preview', 1);
            $scope.previewMode = true;
            $rootScope.$storage.maximizeDetailPane = false;
            $rootScope.$storage.minimizeDetailPane = false;
            $scope.setGraphReadOnly(true);
            $scope.setGraphPreviewMode(true);
            $scope.$broadcast('previewPipeline');
          }, function () {

          });
        } else {
          $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Run Preview', 1);
          $scope.previewMode = true;
          $rootScope.$storage.maximizeDetailPane = false;
          $rootScope.$storage.minimizeDetailPane = false;
          $scope.setGraphReadOnly(true);
          $scope.setGraphPreviewMode(true);
          $scope.$broadcast('previewPipeline');
        }
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
      viewSnapshot: function(snapshotInfo) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'View Snapshot', 1);
        $scope.snapshotMode = true;
        $scope.activeSnapshotInfo = snapshotInfo;
        $rootScope.$storage.maximizeDetailPane = false;
        $rootScope.$storage.minimizeDetailPane = false;
        $scope.setGraphPreviewMode(true);
        $scope.$broadcast('snapshotPipeline', snapshotInfo.id);
      },

      setActiveSnapshotInfo: function(snapshotInfo) {
        $scope.activeSnapshotInfo = snapshotInfo;
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
        if (!options.type) {
          if (options.selectedObject && options.selectedObject.instanceName) {
            options.type = pipelineConstant.STAGE_INSTANCE;
          } else {
            options.type = pipelineConstant.PIPELINE;
          }
        }

        if (!options.ignoreBroadCast) {
          if (options.type !== pipelineConstant.LINK) {
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
       * Start Metrics and Alerts WebSocket connection
       */
      startMonitoring: function() {
        refreshPipelineMetrics();
        //initializeAlertWebSocket();
      },

      /**
       * Close Metrics and Alerts WebSocket connection
       */
      stopMonitoring: function() {
        if (isWebSocketSupported) {
          if (metricsWebSocket) {
            metricsWebSocket.close();
          }
        } else {
          $timeout.cancel(pipelineMetricsTimer);
        }
      },

      /**
       * Returns label of the Stage Instance.
       *
       * @param stageInstanceName
       * @returns {*|string}
       */
      getStageInstanceLabel: function (stageInstanceName) {
        var instance;
        var errorStage = $scope.pipelineConfig.errorStage;
        var statsAggregatorStage = $scope.pipelineConfig.statsAggregatorStage;
        var startEventStage = $scope.pipelineConfig.startEventStages[0];
        var stopEventStage = $scope.pipelineConfig.stopEventStages[0];

        angular.forEach($scope.pipelineConfig.stages, function (stageInstance) {
          if (stageInstance.instanceName === stageInstanceName) {
            instance = stageInstance;
          }
        });

        if (!instance && errorStage && errorStage.instanceName === stageInstanceName) {
          instance = errorStage;
        }

        if (!instance && statsAggregatorStage && statsAggregatorStage.instanceName === stageInstanceName) {
          instance = statsAggregatorStage;
        }

        if (!instance && startEventStage && startEventStage.instanceName === stageInstanceName) {
          instance = startEventStage;
        }

        if (!instance && stopEventStage && stopEventStage.instanceName === stageInstanceName) {
          instance = stopEventStage;
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
        var pipelineConfig = $scope.pipelineConfig;
        var stageInstance;

        if (instanceName) {
          //Select stage instance
          stageInstance = _.find(pipelineConfig.stages, function(stage) {
            return stage.instanceName === instanceName;
          });

          if (stageInstance) {
            $scope.changeStageSelection({
              selectedObject: stageInstance,
              type: pipelineConstant.STAGE_INSTANCE,
              detailTabName: 'configuration',
              configGroup: issue.configGroup,
              configName: issue.configName
            });
          } else if (pipelineConfig.errorStage && pipelineConfig.errorStage.instanceName === instanceName){
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
          } else if (pipelineConfig.statsAggregatorStage &&
            pipelineConfig.statsAggregatorStage.instanceName === instanceName){
            //StatsAggregator Stage Configuration Issue
            $scope.$broadcast('selectNode');
            $scope.changeStageSelection({
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE,
              detailTabName: 'configuration',
              configGroup: issue.configGroup,
              configName: issue.configName,
              statsAggregatorStage: true
            });
          } else if (pipelineConfig.startEventStages[0] &&
            pipelineConfig.startEventStages[0].instanceName === instanceName){
            //StartEvent Stage Configuration Issue
            $scope.$broadcast('selectNode');
            $scope.changeStageSelection({
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE,
              detailTabName: 'configuration',
              configGroup: issue.configGroup,
              configName: issue.configName,
              startEventStage: true
            });
          } else if (pipelineConfig.stopEventStages[0] &&
            pipelineConfig.stopEventStages[0].instanceName === instanceName){
            //StopEvent Stage Configuration Issue
            $scope.$broadcast('selectNode');
            $scope.changeStageSelection({
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE,
              detailTabName: 'configuration',
              configGroup: issue.configGroup,
              configName: issue.configName,
              stopEventStage: true
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

        if (event) {
          event.preventDefault();
          event.stopPropagation();
        }

        $scope.triggeredAlerts = _.filter($scope.triggeredAlerts, function(alert) {
          return alert.ruleDefinition.id !== triggeredAlert.ruleDefinition.id;
        });

        $rootScope.common.deleteTriggeredAlert(triggeredAlert);
      },

      /**
       * Google Analytics Track Event
       *
       * @param category Typically the object that was interacted with (e.g. button)
       * @param action The type of interaction (e.g. click)
       * @param label Useful for categorizing events (e.g. nav buttons)
       * @param value Values must be non-negative. Useful to pass counts (e.g. 4 times)
       */
      trackEvent: function(category, action, label, value) {
        $rootScope.common.trackEvent(category, action, label, value);
      },

      /**
       * Undo Changes
       * @returns {boolean}
       */
      undo: function() {
        if ($scope.canUndo()){
          currArchivePos -= 1;
          revert(currArchivePos);
          return true;
        }
        return false;
      },

      /**
       * Redo changes
       * @returns {boolean}
       */
      redo: function() {
        if ($scope.canRedo()){
          currArchivePos += 1;
          revert(currArchivePos);
          return true;
        }
        return false;
      },

      /**
       * Returns true if undo operation is supported
       * @returns {boolean}
       */
      canUndo: function() {
        return currArchivePos > 0;
      },

      /**
       * Returns true if redo operation is supported
       * @returns {boolean}
       */
      canRedo: function() {
        return currArchivePos < archive.length-1;
      },

      /**
       * Clear undo redo archive
       */
      clearUndoRedoArchive: function () {
        archive = [];
        currArchivePos = null;
      },

      reloadingNewPipeline: function() {
        reloadingNew = true;
      },

      /**
       * Select Pipeline Config in detail pane
       */
      selectPipelineConfig: function() {
        $scope.$broadcast('selectNode');
        $scope.changeStageSelection({
          selectedObject: undefined,
          type: pipelineConstant.PIPELINE
        });
      },

      /**
       * Clear detail tab selection cache
       */
      clearTabSelectionCache: function() {
        $scope.selectedDetailPaneTabCache = {};
        $scope.selectedConfigGroupCache = {};
      }
    });

    /**
     * Fetch definitions for Pipeline and Stages, fetch all pipeline configuration info, status and metric.
     */
    $q.all([
      api.pipelineAgent.getPipelineStatus(routeParamPipelineName, '0'),
      api.acl.getPipelinePermissions(routeParamPipelineName),
      pipelineService.init(),
      configuration.init()
    ])
      .then(function (results) {
        var pipelineStatusMap = {};
        pipelineStatusMap[routeParamPipelineName] = results[0].data;

        var permissions = results[1].data;
        var isACLEnabled = configuration.isACLEnabled();
        if (authService.isAuthorized([userRoles.admin]) || !isACLEnabled) {
          // for admin user or if ACL is disabled
          $scope.canWrite = true;
          $scope.canExecute = true;
        } else if (permissions) {
          angular.forEach(permissions, function (permission) {
            if (permission.actions.indexOf('WRITE') !== -1) {
              $scope.canWrite = true;
            }

            if (permission.actions.indexOf('EXECUTE') !== -1) {
              $scope.canExecute = true;
            }
          });

          if (!$scope.canWrite) {
            $scope.isPipelineReadOnly = true;
          }
        }

        isWebSocketSupported = (typeof(WebSocket) === 'function') && configuration.isWebSocketUseEnabled();
        undoLimit = configuration.getUndoLimit();

        if (configuration.isAnalyticsEnabled()) {
          Analytics.trackPage('/collector/pipeline/pipelineName');
        }

        $scope.monitorMemoryEnabled = configuration.isMonitorMemoryEnabled();

        //Definitions
        $scope.pipelineConfigDefinition = pipelineService.getPipelineConfigDefinition();
        $scope.stageLibraries = pipelineService.getStageDefinitions();

        //Pipelines

        $scope.existingPipelineLabels = pipelineService.existingPipelineLabels || [];

        $rootScope.common.pipelineStatusMap = pipelineStatusMap;

        return $q.all([api.pipelineAgent.getPipelineConfig(routeParamPipelineName),
          api.pipelineAgent.getPipelineRules(routeParamPipelineName),
          api.pipelineAgent.getPipelineMetrics(routeParamPipelineName, 0)]);
      },function(resp) {
        $scope.showLoading = false;
        $rootScope.common.errors = [resp.data];
      })
      .then(function(results) {
        $scope.showLoading = false;
        //Pipeline Configuration, Rules & Metrics
        if (results && results.length > 1) {
          var config = results[0].data;
          var rules = results[1].data;
          var clickedAlert = $rootScope.common.clickedAlert;

          $rootScope.common.pipelineMetrics = results[2].data;
          if (_.contains(['RUNNING', 'STARTING'], $rootScope.common.pipelineStatusMap[routeParamPipelineName].status)) {
            refreshPipelineMetrics();
          }

          if ($rootScope.common.pipelineStatusMap[routeParamPipelineName].status === 'RETRY') {
            updateRetryCountdown($rootScope.common.pipelineStatusMap[routeParamPipelineName].nextRetryTimeStamp);
          }

          updateGraph(config, rules, undefined, undefined, true);

          if (clickedAlert && clickedAlert.pipelineName === $scope.activeConfigInfo.pipelineId) {
            var edges = $scope.edges,
                edge;
            $rootScope.common.clickedAlert = undefined;

            if (clickedAlert.ruleDefinition.metricId) {
              //Select Pipeline Config
              updateDetailPane({
                selectedObject: undefined,
                type: pipelineConstant.PIPELINE,
                detailTabName: 'summary'
              });
            } else {
              //Select edge
              edge = _.find(edges, function(ed) {
                return ed.outputLane === clickedAlert.ruleDefinition.lane;
              });
              updateDetailPane({
                selectedObject: edge,
                type: pipelineConstant.LINK,
                detailTabName: 'summary'
              });
            }
          } else {
            updateDetailPane({
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE
            });
          }
        }
        $scope.loaded = true;
      },function(resp) {
        $scope.pipelineConfig = undefined;
        $scope.showLoading = false;
        $rootScope.common.errors = [resp.data];
      });

    /**
     * Load Pipeline Configuration by fetching it from server for the given Pipeline Configuration name.
     * @param configName
     */
    var loadPipelineConfig = function(configName) {
      $q.all([api.pipelineAgent.getPipelineConfig(configName),
        api.pipelineAgent.getPipelineRules(configName)]
      ).then(function(results) {
        var config = results[0].data;
        var rules = results[1].data;
        var clickedAlert = $rootScope.common.clickedAlert;

        $rootScope.common.errors = [];

        archive = [];
        currArchivePos = null;

        updateGraph(config, rules);

        if (clickedAlert && clickedAlert.pipelineName === $scope.activeConfigInfo.pipelineId) {
          var edges = $scope.edges,
            edge;
          $rootScope.common.clickedAlert = undefined;

          if (clickedAlert.ruleDefinition.metricId) {
            //Select Pipeline Config
            updateDetailPane({
              selectedObject: undefined,
              type: pipelineConstant.PIPELINE,
              detailTabName: 'summary'
            });
          } else {
            //Select edge
            edge = _.find(edges, function(ed) {
              return ed.outputLane === clickedAlert.ruleDefinition.lane;
            });
            updateDetailPane({
              selectedObject: edge,
              type: pipelineConstant.LINK,
              detailTabName: 'summary'
            });
          }
        } else {
          updateDetailPane({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE
          });
        }
      },function(resp) {
        $scope.pipelineConfig = undefined;
        $rootScope.common.errors = [resp.data];
      });
    };

    /**
     * Group config definitions together based on the display group
     *
     * @param stageDefinition Definition of the stage itself
     */
    var getGroupConfigDefinitions = function(stageDefinition) {
      if(!stageDefinition) {
        return undefined;
      }

      return stageDefinition.configGroupDefinition.groupNameToLabelMapList.map(function(group) {
        return {
          'group' : group,
          'configDefinitions': _.filter(stageDefinition.configDefinitions, function(configDefinition) {
            return configDefinition.group == group.name;
          })
        };
      });
    };

    /**
     * Revert the changes.
     *
     * @param revertToPos
     */
    var revert = function(revertToPos) {
      var cuuid = $scope.pipelineConfig.uuid,
        ruuid = $scope.pipelineRules.uuid,
        ar = archive[revertToPos];

      $scope.pipelineConfig = angular.copy(ar.c);
      $scope.pipelineConfig.uuid = cuuid;

      $scope.pipelineRules = angular.copy(ar.r);
      $scope.pipelineRules.uuid = ruuid;

      archiveOp = true;

      updateGraph($scope.pipelineConfig, $scope.pipelineRules, true, true);
    };


    /**
     * Add to archive
     *
     * @param config
     * @param rules
     */
    var addToArchive = function(config, rules) {
      if (archiveOp) {
        archiveOp = false;
        return;
      }

      //Archiving the current state of the variables
      if (archive.length - 1 > currArchivePos){
        //Cutting off the end of the archive if you were in the middle of your archive and made a change
        var diff = archive.length - currArchivePos - 1;
        archive.splice(currArchivePos+1, diff);
      }

      archive.push({
        c: angular.copy(config),
        r: angular.copy(rules)
      });

      if (archive.length > undoLimit) {
        //Store only last 10 changes
        archive.shift();
      }

      currArchivePos = archive.length -1;
    };


    /**
     * Save Updates
     * @param config
     */
    var saveUpdates = function (config) {
      if (configSaveInProgress || $scope.isPipelineReadOnly ||
        $rootScope.common.isSlaveNode) {
        return;
      }

      if (!config) {
        config = _.clone($scope.pipelineConfig);
      }

      configDirty = false;
      configSaveInProgress = true;
      $rootScope.common.saveOperationInProgress++;


      if (!$scope.isPipelineRunning) {
        api.pipelineAgent.savePipelineConfig($scope.activeConfigInfo.pipelineId, config)
          .then(function (response) {
            var res = response.data;

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
              res.statsAggregatorStage = config.statsAggregatorStage;
              res.startEventStages = config.startEventStages;
              res.stopEventStages = config.stopEventStages;

              saveUpdates(config);
            }

            updateGraph(res, $scope.pipelineRules);
          })
          .catch(function(res) {
            configSaveInProgress = false;
            $rootScope.common.saveOperationInProgress--;
            $rootScope.common.errors = [res.data];
          });
      } else {

        //If Pipeline is running save only UI Info

        var uiInfoMap = {
          ':pipeline:': config.uiInfo
        };

        angular.forEach(config.stages, function(stageInstance) {
          uiInfoMap[stageInstance.instanceName] = stageInstance.uiInfo;
        });

        api.pipelineAgent.savePipelineUIInfo($scope.activeConfigInfo.pipelineId, uiInfoMap)
          .then(function (res) {

            //Clear Previous errors
            $rootScope.common.errors = [];

            configSaveInProgress = false;

            if (configDirty) {
              saveUpdates();
            }

            $rootScope.common.saveOperationInProgress--;

            //updateGraph(res, $scope.pipelineRules);
          })
          .catch(function(res) {
            configSaveInProgress = false;
            $rootScope.common.saveOperationInProgress--;
            $rootScope.common.errors = [res.data];
          });
      }
    };

    /**
     * Update Pipeline Graph
     *
     * @param pipelineConfig
     * @param pipelineRules
     * @param manualUpdate
     * @param ignoreArchive
     * @param fitToBounds
     */
    var updateGraph = function (pipelineConfig, pipelineRules, manualUpdate, ignoreArchive, fitToBounds) {
      var selectedStageInstance;
      var stageErrorCounts = {};
      var pipelineMetrics = $rootScope.common.pipelineMetrics;
      var pipelineStatus = $rootScope.common.pipelineStatusMap[routeParamPipelineName];

      if (!manualUpdate) {
        ignoreUpdate = true;
      }

      if ($rootScope.common.isDPMEnabled && pipelineConfig.metadata && pipelineConfig.metadata.lastConfigId &&
        (pipelineConfig.metadata.lastConfigId !== pipelineConfig.uuid ||
        pipelineConfig.metadata.lastRulesId !== pipelineRules.uuid)) {
        $scope.isDPMPipelineDirty = true;
      }

      //Force Validity Check - showErrors directive
      $scope.$broadcast('show-errors-check-validity');

      $scope.pipelineConfig = pipelineConfig || {};
      $scope.activeConfigInfo = $rootScope.$storage.activeConfigInfo = pipelineConfig.info;
      $scope.pipelineRules = pipelineRules;
      var executionModeConfig = _.find($scope.pipelineConfig.configuration, function (c) {
        return c.name === 'executionMode';
      });
      $scope.executionMode = executionModeConfig.value;

      $scope.sources = _.filter($scope.stageLibraries, function (stageLibrary) {
        return stageLibrary.type === pipelineConstant.SOURCE_STAGE_TYPE &&
          stageLibrary.library !== 'streamsets-datacollector-stats-lib' &&
          stageLibrary.executionModes.indexOf($scope.executionMode) !== -1;
      });

      $scope.processors = _.filter($scope.stageLibraries, function (stageLibrary) {
        return stageLibrary.type === pipelineConstant.PROCESSOR_STAGE_TYPE &&
          stageLibrary.library !== 'streamsets-datacollector-stats-lib' &&
          stageLibrary.executionModes.indexOf($scope.executionMode) !== -1;
      });

      $scope.executors = _.filter($scope.stageLibraries, function (stageLibrary) {
        return (stageLibrary.type === pipelineConstant.EXECUTOR_STAGE_TYPE &&
          stageLibrary.library !== 'streamsets-datacollector-stats-lib') &&
          stageLibrary.executionModes.indexOf($scope.executionMode) !== -1;
      });

      $scope.targets = _.filter($scope.stageLibraries, function (stageLibrary) {
        return (stageLibrary.type === pipelineConstant.TARGET_STAGE_TYPE &&
          !stageLibrary.errorStage && !stageLibrary.statsAggregatorStage  &&
          stageLibrary.library !== 'streamsets-datacollector-stats-lib') &&
          stageLibrary.executionModes.indexOf($scope.executionMode) !== -1;
      });

      //Initialize the pipeline config
      if (!$scope.pipelineConfig.uiInfo || _.isEmpty($scope.pipelineConfig.uiInfo)) {
        $scope.pipelineConfig.uiInfo = {
          previewConfig : {
            previewSource: pipelineConstant.CONFIGURED_SOURCE,
            batchSize: 10,
            timeout: 10000,
            writeToDestinations: false,
            executeLifecycleEvents: false,
            showHeader: false,
            showFieldType: true,
            rememberMe: false
          }
        };
      }

      // Initialize metadata for label support
      if (!$scope.pipelineConfig.metadata || _.isEmpty($scope.pipelineConfig.metadata)) {
        $scope.pipelineConfig.metadata = {
          labels : []
        };
      }

      if (!$scope.pipelineConfig.statsAggregatorStage) {
        var statsAggregatorStageConfig = _.find($scope.pipelineConfig.configuration, function (c) {
          return c.name === 'statsAggregatorStage';
        });
        if (statsAggregatorStage) {
          statsAggregatorStageConfig.value = "streamsets-datacollector-basic-lib::" +
            "com_streamsets_pipeline_stage_destination_devnull_StatsNullDTarget::1";
        }
      }

      //Determine edges from input lanes and output lanes
      //And also set flag sourceExists if pipeline Config contains source
      edges = [];
      $scope.sourceExists = false;
      angular.forEach($scope.pipelineConfig.stages, function (sourceStageInstance) {
        if (sourceStageInstance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE) {
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

        if (sourceStageInstance.eventLanes && sourceStageInstance.eventLanes.length) {
          angular.forEach(sourceStageInstance.eventLanes, function (eventLane) {
            angular.forEach($scope.pipelineConfig.stages, function (targetStageInstance) {
              if (targetStageInstance.inputLanes && targetStageInstance.inputLanes.length &&
                _.contains(targetStageInstance.inputLanes, eventLane)) {
                edges.push({
                  source: sourceStageInstance,
                  target: targetStageInstance,
                  eventLane: eventLane
                });
              }
            });
          });
        }


      });

      $scope.edges = edges;

      $scope.firstOpenLane = $rootScope.$storage.dontShowHelpAlert ? {} : getFirstOpenLane();

      if (pipelineStatus && pipelineStatus.pipelineId === pipelineConfig.info.pipelineId &&
        pipelineStatus.status === 'RUNNING' && pipelineMetrics && pipelineMetrics.meters) {
        stageErrorCounts = getStageErrorCounts();
      }

      $scope.stageSelected = false;

      if ($scope.detailPaneConfig === undefined) {
        //First time
        $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.selectedType = pipelineConstant.PIPELINE;
      } else {
        //Check

        if ($scope.selectedType === pipelineConstant.PIPELINE) {
          //In case of detail pane is Pipeline Configuration
          $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
          $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
          $scope.selectedType = pipelineConstant.PIPELINE;
        } else if ($scope.selectedType === pipelineConstant.STAGE_INSTANCE) {
          //In case of detail pane is stage instance
          angular.forEach($scope.pipelineConfig.stages, function (stageInstance) {
            if (stageInstance.instanceName === $scope.detailPaneConfig.instanceName) {
              selectedStageInstance = stageInstance;
            }
          });

          if (selectedStageInstance) {
            $scope.detailPaneConfig = $scope.selectedObject = selectedStageInstance;
            $scope.stageSelected = true;
            $scope.selectedType = pipelineConstant.STAGE_INSTANCE;
          } else {
            $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;
            $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
            $scope.selectedType = pipelineConstant.PIPELINE;
          }
        }
      }

      if ($scope.selectedType === pipelineConstant.PIPELINE) {
        var errorStage = $scope.pipelineConfig.errorStage;
        if (errorStage && errorStage.configuration && errorStage.configuration.length) {
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

        var statsAggregatorStage = $scope.pipelineConfig.statsAggregatorStage;
        if (statsAggregatorStage && statsAggregatorStage.configuration && statsAggregatorStage.configuration.length) {
          $scope.statsAggregatorStageConfig = statsAggregatorStage;
          $scope.statsAggregatorStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === statsAggregatorStage.library &&
              stageLibrary.name === statsAggregatorStage.stageName &&
              stageLibrary.version === statsAggregatorStage.stageVersion;
          });
        } else {
          $scope.statsAggregatorStageConfig = undefined;
          $scope.statsAggregatorStageConfigDefn = undefined;
        }


        var startEventStage = $scope.pipelineConfig.startEventStages[0];
        if (startEventStage && startEventStage.configuration && startEventStage.configuration.length) {
          $scope.startEventStageConfig = startEventStage;
          $scope.startEventStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === startEventStage.library &&
              stageLibrary.name === startEventStage.stageName &&
              stageLibrary.version === startEventStage.stageVersion;
          });
          $scope.startEventStageConfigDefnGroups = getGroupConfigDefinitions($scope.startEventStageConfigDefn);
        } else {
          $scope.startEventStageConfig = undefined;
          $scope.startEventStageConfigDefn = undefined;
          $scope.startEventStageConfigDefnGroups = undefined;
        }

        var stopEventStage = $scope.pipelineConfig.stopEventStages[0];
        if (stopEventStage && stopEventStage.configuration && stopEventStage.configuration.length) {
          $scope.stopEventStageConfig = stopEventStage;
          $scope.stopEventStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === stopEventStage.library &&
              stageLibrary.name === stopEventStage.stageName &&
              stageLibrary.version === stopEventStage.stageVersion;
          });
          $scope.stopEventStageConfigDefnGroups = getGroupConfigDefinitions($scope.stopEventStageConfigDefn);
        } else {
          $scope.stopEventStageConfig = undefined;
          $scope.stopEventStageConfigDefn = undefined;
          $scope.stopEventStageConfigDefnGroups = undefined;
        }
      }

      if (!ignoreArchive) {
        addToArchive($scope.pipelineConfig, $scope.pipelineRules);
      }

      $timeout(function() {
        var config = $scope.pipelineConfig;
        var commonErrors = $rootScope.common.errors;
        var issuesMap;

        if (commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) {
          issuesMap = commonErrors[0];
        } else if (config && config.issues){
          issuesMap = config.issues;
        }

        $scope.$broadcast('updateGraph', {
          nodes: $scope.pipelineConfig.stages,
          edges: edges,
          issues: issuesMap,
          selectNode: ($scope.selectedType && $scope.selectedType === pipelineConstant.STAGE_INSTANCE) ? $scope.selectedObject : undefined,
          selectEdge: ($scope.selectedType && $scope.selectedType === pipelineConstant.LINK) ? $scope.selectedObject : undefined,
          stageErrorCounts: stageErrorCounts,
          showEdgePreviewIcon: true,
          isReadOnly: $scope.isPipelineReadOnly || $scope.isPipelineRunning || $scope.previewMode,
          pipelineRules: $scope.pipelineRules,
          triggeredAlerts: $scope.isPipelineRunning ? $scope.triggeredAlerts : [],
          errorStage: $scope.pipelineConfig.errorStage,
          statsAggregatorStage: $scope.pipelineConfig.statsAggregatorStage,
          startEventStage: $scope.pipelineConfig.startEventStages[0],
          stopEventStage: $scope.pipelineConfig.stopEventStages[0],
          fitToBounds: fitToBounds
        });
      });

    };

    /**
     * Update Detail Pane when selection changes in Pipeline Graph.
     *
     * @param options
     */
    var updateDetailPane = function(options) {
      var selectedObject = options.selectedObject;
      var type = options.type;
      var errorStage = $scope.pipelineConfig.errorStage;
      var statsAggregatorStage = $scope.pipelineConfig.statsAggregatorStage;
      var startEventStage = $scope.pipelineConfig.startEventStages[0];
      var stopEventStage = $scope.pipelineConfig.stopEventStages[0];
      var stageLibraryList = [];
      var optionsLength = Object.keys(options).length;

      if (!$scope.previewMode && !$scope.snapshotMode && $scope.selectedType === type && $scope.selectedObject && selectedObject && optionsLength <= 2 &&
        ((type === pipelineConstant.PIPELINE && $scope.selectedObject.info.pipelineId === selectedObject.info.pipelineId) ||
          (type === pipelineConstant.STAGE_INSTANCE && $scope.selectedObject.instanceName === selectedObject.instanceName))) {
        // Previous selection remain same
        return;
      }

      $scope.selectedType = type;
      $scope.errorStageConfig = undefined;
      $scope.errorStageConfigDefn = undefined;
      $scope.statsAggregatorStageConfig = undefined;
      $scope.statsAggregatorStageConfigDefn = undefined;
      $scope.startEventStageConfig = undefined;
      $scope.startEventStageConfigDefn = undefined;
      $scope.stopEventStageConfig = undefined;
      $scope.stopEventStageConfigDefn = undefined;
      $scope.stopEventStageConfigDefnGroups = undefined;
      $scope.startEventStageConfigDefnGroups = undefined;

      if (options.configName) {
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;
      }

      if (type === pipelineConstant.STAGE_INSTANCE) {
        $scope.stageSelected = true;
        //Stage Instance Configuration
        $scope.detailPaneConfig = $scope.selectedObject = selectedObject;

        $scope.detailPaneConfigDefn = undefined;

        _.each($scope.stageLibraries, function (stageLibrary) {
          if (stageLibrary.name === selectedObject.stageName &&
            stageLibrary.version === selectedObject.stageVersion) {

            if (stageLibrary.library === selectedObject.library) {
              $scope.detailPaneConfigDefn = stageLibrary;
            }

            stageLibraryList.push({
              library: stageLibrary.library,
              libraryLabel: stageLibrary.libraryLabel
            });
          }
        });

        // Crate list of services where each item have both the definition and actual configuration values
        $scope.detailPaneServices = [];
        _.each($scope.detailPaneConfig.services, function(serviceConfig) {
          var service = {};
          service.definition = pipelineService.getServiceDefinition(serviceConfig.service);
          service.config = serviceConfig;
          $scope.detailPaneServices.push(service);
        });

        $scope.stageLibraryList = _.sortBy(stageLibraryList, 'libraryLabel');

        if (!options.detailTabName) {
          if ($scope.selectedDetailPaneTabCache[selectedObject.instanceName]) {
            options.detailTabName = $scope.selectedDetailPaneTabCache[selectedObject.instanceName];
          } else {
            if ($scope.isPipelineRunning) {
              options.detailTabName = 'summary';
            } else {
              options.detailTabName = 'configuration';
            }
          }

          if ($scope.selectedConfigGroupCache[selectedObject.instanceName]) {
            options.configGroup = $scope.selectedConfigGroupCache[selectedObject.instanceName];
          }
        }

      } else if (type === pipelineConstant.PIPELINE){
        // Pipeline Configuration
        $scope.stageSelected = false;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.selectedObject = $scope.pipelineConfig;

        if (errorStage && errorStage.configuration && errorStage.configuration.length) {
          $scope.errorStageConfig = errorStage;
          $scope.errorStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === errorStage.library &&
            stageLibrary.name === errorStage.stageName &&
            stageLibrary.version === errorStage.stageVersion;
          });
        }

        if (statsAggregatorStage && statsAggregatorStage.configuration && statsAggregatorStage.configuration.length) {
          $scope.statsAggregatorStageConfig = statsAggregatorStage;
          $scope.statsAggregatorStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === statsAggregatorStage.library &&
              stageLibrary.name === statsAggregatorStage.stageName &&
              stageLibrary.version === statsAggregatorStage.stageVersion;
          });
        }

        if (startEventStage && startEventStage.configuration && startEventStage.configuration.length) {
          $scope.startEventStageConfig = startEventStage;
          $scope.startEventStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === startEventStage.library &&
              stageLibrary.name === startEventStage.stageName &&
              stageLibrary.version === startEventStage.stageVersion;
          });
          $scope.startEventStageConfigDefnGroups = getGroupConfigDefinitions($scope.startEventStageConfigDefn);
        }

        if (stopEventStage && stopEventStage.configuration && stopEventStage.configuration.length) {
          $scope.stopEventStageConfig = stopEventStage;
          $scope.stopEventStageConfigDefn =  _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === stopEventStage.library &&
              stageLibrary.name === stopEventStage.stageName &&
              stageLibrary.version === stopEventStage.stageVersion;
          });
          $scope.stopEventStageConfigDefnGroups = getGroupConfigDefinitions($scope.stopEventStageConfigDefn);
        }

        if (!options.detailTabName) {
          if ($scope.selectedDetailPaneTabCache[$scope.pipelineConfig.info.pipelineId]) {
            options.detailTabName = $scope.selectedDetailPaneTabCache[$scope.pipelineConfig.info.pipelineId];
          } else {
            if ($scope.isPipelineRunning) {
              options.detailTabName = 'summary';
            } else {
              options.detailTabName = 'configuration';
            }
          }

          if ($scope.selectedConfigGroupCache[$scope.pipelineConfig.info.pipelineId]) {
            options.configGroup = $scope.selectedConfigGroupCache[$scope.pipelineConfig.info.pipelineId];
          }
        }

      } else if (type === pipelineConstant.LINK) {
        $scope.detailPaneConfig = $scope.selectedObject = selectedObject;

        if (!options.detailTabName) {
          if ($scope.selectedDetailPaneTabCache[$scope.selectedObject.outputLane]) {
            options.detailTabName = $scope.selectedDetailPaneTabCache[$scope.selectedObject.outputLane];
          } else {
            if ($scope.isPipelineRunning) {
              options.detailTabName = 'summary';
            } else {
              options.detailTabName = 'dataRules';
            }
          }
        }
      }

      $scope.firstOpenLane = $rootScope.$storage.dontShowHelpAlert ? {} : getFirstOpenLane();
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
      if (destroyed) {
        return;
      }

      if (isWebSocketSupported) {

        if (metricsWebSocket) {
          metricsWebSocket.close();
        }

        //WebSocket to get Pipeline Metrics
        metricsWebSocket = new WebSocket(webSocketMetricsURL);

        metricsWebSocket.onmessage = function (evt) {
          var received_msg = evt.data;

          if (!$scope.monitoringPaused) {
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
            //console.log( 'Pipeline Metrics Timeout executed', Date.now() );
          },
          configuration.getRefreshInterval()
        );

        pipelineMetricsTimer.then(
          function() {
            api.pipelineAgent.getPipelineMetrics($scope.activeConfigInfo.pipelineId, 0)
              .then(function(res) {
                var data = res.data;
                if (!_.isObject(data) && _.isString(data) && data.indexOf('<!doctype html>') !== -1) {
                  //Session invalidated
                  window.location.reload();
                  return;
                }

                if (!$scope.monitoringPaused) {
                  $rootScope.common.pipelineMetrics = data;
                }
                refreshPipelineMetrics();
              })
              .catch(function(res) {
                $rootScope.common.errors = [res.data];
              });
          },
          function() {
            //console.log( 'Timer rejected!' );
          }
        );

      }
    };

    var getStageErrorCounts = function() {
      var stageInstanceErrorCounts = {},
        pipelineMetrics = $rootScope.common.pipelineMetrics;

      if (pipelineMetrics && pipelineMetrics.meters) {
        angular.forEach($scope.pipelineConfig.stages, function(stageInstance) {
          var errorRecordsMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + '.errorRecords.meter'],
            stageErrorsMeter = pipelineMetrics.meters['stage.' + stageInstance.instanceName + '.stageErrors.meter'];

          if (errorRecordsMeter && stageErrorsMeter) {
            stageInstanceErrorCounts[stageInstance.instanceName] = Math.round(
              errorRecordsMeter.count +
              stageErrorsMeter.count
            );
          } else {
            // Failed to fetch metrics
            // $rootScope.common.errors = ['Failed to fetch pipeline metrics'];
            // console.log('Failed to fetch stage meter metrics');
          }

        });
      }

      return stageInstanceErrorCounts;
    };

    var getFirstOpenLane = function() {
      var pipelineConfig = $scope.pipelineConfig;
      var selectedType = $scope.selectedType;
      var selectedObject = $scope.selectedObject;
      var firstOpenLane = {};
      var issueObj;
      var firstOpenLaneStageInstanceName;
      var isEventLane = false;

      if (pipelineConfig && pipelineConfig.issues && pipelineConfig.issues.stageIssues) {
        angular.forEach(pipelineConfig.issues.stageIssues, function(issues, instanceName) {
          if (!firstOpenLaneStageInstanceName ||
            (selectedType === pipelineConstant.STAGE_INSTANCE && selectedObject.instanceName === instanceName)) {
            angular.forEach(issues, function(issue) {
              if (issue.message.indexOf('VALIDATION_0011') !== -1) {
                issueObj = issue;
                firstOpenLaneStageInstanceName = instanceName;
              } else if (issue.message.indexOf('VALIDATION_0104') !== -1) {
                issueObj = issue;
                firstOpenLaneStageInstanceName = instanceName;
                isEventLane = true;
              }
            });
          }
        });

        if (firstOpenLaneStageInstanceName && issueObj &&
          issueObj.additionalInfo && issueObj.additionalInfo.openStreams) {
          var stageInstance = _.find(pipelineConfig.stages, function(stage) {
              return stage.instanceName === firstOpenLaneStageInstanceName;
            });

          if (stageInstance) {
            if (!isEventLane) {
              firstOpenLane = {
                stageInstance: stageInstance,
                laneName: issueObj.additionalInfo.openStreams[0],
                laneIndex: _.indexOf(stageInstance.outputLanes, issueObj.additionalInfo.openStreams[0])
              };
            } else {
              firstOpenLane = {
                stageInstance: stageInstance,
                laneName: issueObj.additionalInfo.openStreams[0],
                laneIndex: 1,
                isEventLane: true
              };
            }
          }
        }
      }

      return firstOpenLane;
    };

    var derivePipelineRunning = function() {
      var pipelineStatus = $rootScope.common.pipelineStatusMap[routeParamPipelineName],
        config = $scope.pipelineConfig;
      return (pipelineStatus && config && pipelineStatus.pipelineId === config.info.pipelineId &&
      _.contains(['STARTING', 'STARTING_ERROR', 'RUNNING', 'RUNNING_ERROR', 'RETRY', 'FINISHING', 'STOPPING', 'STOPPING_ERROR', 'CONNECTING', 'CONNECT_ERROR'], pipelineStatus.status));
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

      api.pipelineAgent.savePipelineRules($scope.activeConfigInfo.pipelineId, rules)
        .then(function (response) {
          var res = response.data;
          rulesSaveInProgress = false;
          $rootScope.common.saveOperationInProgress--;
          ignoreUpdate = true;
          rules = $scope.pipelineRules;
          rules.ruleIssues = res.ruleIssues;
          rules.configIssues = res.configIssues;
          rules.uuid = res.uuid;

          angular.forEach(rules.metricsRuleDefinitions, function(rule, index) {
            var savedRule = _.find(res.metricsRuleDefinitions, function(savedRule) {
              return savedRule.id === rule.id;
            });

            if (savedRule) {
              rule.valid = savedRule.valid;
            }
          });

          angular.forEach(rules.dataRuleDefinitions, function(rule, index) {
            var savedRule = _.find(res.dataRuleDefinitions, function(savedRule) {
              return savedRule.id === rule.id;
            });

            if (savedRule) {
              rule.valid = savedRule.valid;
            }
          });

          angular.forEach(rules.driftRuleDefinitions, function(rule, index) {
            var savedRule = _.find(res.driftRuleDefinitions, function(savedRule) {
              return savedRule.id === rule.id;
            });

            if (savedRule) {
              rule.valid = savedRule.valid;
            }
          });

          if (rulesDirty) {
            saveRulesUpdate(rules);
          }

          addToArchive($scope.pipelineConfig, $scope.pipelineRules);
          $scope.$broadcast('updateEdgePreviewIconColor', $scope.pipelineRules, []);
        })
        .catch(function(res) {
          $rootScope.common.saveOperationInProgress--;
          $rootScope.common.errors = [res.data];
        });
    };

    var updateRetryCountdown = function(nextRetryTimeStamp) {
      $scope.retryCountDown = (nextRetryTimeStamp - $rootScope.common.serverTimeDifference - (new Date()).getTime())/1000;

      if (retryCountDownTimer) {
        $timeout.cancel(retryCountDownTimer);
      }

      var retryCountDownCallback = function(){
        $scope.retryCountDown--;
        if ($scope.retryCountDown > 0) {
          retryCountDownTimer = $timeout(retryCountDownCallback,1000);
        } else {
          $scope.retryCountDown = 0;
        }
      };
      retryCountDownTimer = $timeout(retryCountDownCallback, 1000);

    };

    //Event Handling

    $scope.$watch('activeConfigStatus.status', function (newValue) {
      if (newValue === 'FINISHED') {
        $rootScope.common.successList = [{message:'Pipeline Job has been finished'}];
      }
    });

    $scope.$watch('pipelineConfig', function (newValue, oldValue) {
      if (newValue === undefined) {
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

        if ((badRecordHandlingConfigArr && badRecordHandlingConfigArr.length === 3) &&
            (!errorStageInst || errorStageInst.library !== badRecordHandlingConfigArr[0] ||
            errorStageInst.stageName !== badRecordHandlingConfigArr[1] ||
            errorStageInst.stageVersion !== badRecordHandlingConfigArr[2])) {

          var badRecordsStage = _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === badRecordHandlingConfigArr[0] &&
              stageLibrary.name === badRecordHandlingConfigArr[1] &&
              stageLibrary.version === badRecordHandlingConfigArr[2];
          });

          if (badRecordsStage) {
            var configuration;
            if (errorStageInst && errorStageInst.stageName === badRecordsStage.name  &&
              errorStageInst.stageVersion === badRecordsStage.version) {
              configuration = errorStageInst.configuration;
            }
            newValue.errorStage = pipelineService.getNewStageInstance({
              stage: badRecordsStage,
              pipelineConfig: $scope.pipelineConfig,
              errorStage: true,
              configuration: configuration
            });
          }
        }


        //statsAggregatorStage check
        var statsAggregatorStageConfig = _.find(newValue.configuration, function (c) {
          return c.name === 'statsAggregatorStage';
        });
        var statsAggregatorStageConfigArr = (statsAggregatorStageConfig && statsAggregatorStageConfig.value) ?
          (statsAggregatorStageConfig.value).split('::') : undefined;
        var statsAggregatorStageInst = newValue.statsAggregatorStage;

        if ((statsAggregatorStageConfigArr && statsAggregatorStageConfigArr.length === 3) &&
          (!statsAggregatorStageInst || statsAggregatorStageInst.library !== statsAggregatorStageConfigArr[0] ||
          statsAggregatorStageInst.stageName !== statsAggregatorStageConfigArr[1] ||
          statsAggregatorStageInst.stageVersion !== statsAggregatorStageConfigArr[2])) {

          var statsAggregatorStage = _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === statsAggregatorStageConfigArr[0] &&
              stageLibrary.name === statsAggregatorStageConfigArr[1] &&
              stageLibrary.version === statsAggregatorStageConfigArr[2];
          });

          if (statsAggregatorStage) {
            if (statsAggregatorStageInst && statsAggregatorStageInst.stageName === statsAggregatorStage.name  &&
              statsAggregatorStageInst.stageVersion === statsAggregatorStage.version) {
              saConfig = statsAggregatorStageInst.configuration;
            }
            newValue.statsAggregatorStage = pipelineService.getNewStageInstance({
              stage: statsAggregatorStage,
              pipelineConfig: $scope.pipelineConfig,
              statsAggregatorStage: true,
              configuration: saConfig
            });
          }
        }

        // Start Event check
        var startEventStageConfig = _.find(newValue.configuration, function (c) {
          return c.name === 'startEventStage';
        });
        var startEventStageConfigArr = (startEventStageConfig && startEventStageConfig.value) ?
          (startEventStageConfig.value).split('::') : undefined;
        var startEventStageInst = newValue.startEventStages[0];

        if ((startEventStageConfigArr && startEventStageConfigArr.length === 3) &&
          (!startEventStageInst || startEventStageInst.library !== startEventStageConfigArr[0] ||
          startEventStageInst.stageName !== startEventStageConfigArr[1] ||
          startEventStageInst.stageVersion !== startEventStageConfigArr[2])) {

          var startEventStage = _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === startEventStageConfigArr[0] &&
              stageLibrary.name === startEventStageConfigArr[1] &&
              stageLibrary.version === startEventStageConfigArr[2];
          });

          if (startEventStage) {
            if (startEventStageInst && startEventStageInst.stageName === startEventStage.name  &&
              startEventStageInst.stageVersion === startEventStage.version) {
              saConfig = startEventStageInst.configuration;
            }
            eventStage = pipelineService.getNewStageInstance({
              stage: startEventStage,
              pipelineConfig: $scope.pipelineConfig,
              startEventStage: true,
              configuration: saConfig
            });

            newValue.startEventStages = [ eventStage ]
          }
        }

        // Stop Event check
        var stopEventStageConfig = _.find(newValue.configuration, function (c) {
          return c.name === 'stopEventStage';
        });
        var stopEventStageConfigArr = (stopEventStageConfig && stopEventStageConfig.value) ?
          (stopEventStageConfig.value).split('::') : undefined;
        var stopEventStageInst = newValue.stopEventStages[0];

        if ((stopEventStageConfigArr && stopEventStageConfigArr.length === 3) &&
          (!stopEventStageInst || stopEventStageInst.library !== stopEventStageConfigArr[0] ||
          stopEventStageInst.stageName !== stopEventStageConfigArr[1] ||
          stopEventStageInst.stageVersion !== stopEventStageConfigArr[2])) {

          var stopEventStage = _.find($scope.stageLibraries, function (stageLibrary) {
            return stageLibrary.library === stopEventStageConfigArr[0] &&
              stageLibrary.name === stopEventStageConfigArr[1] &&
              stageLibrary.version === stopEventStageConfigArr[2];
          });

          if (stopEventStage) {
            var saConfig;
            if (stopEventStageInst && stopEventStageInst.stageName === stopEventStage.name  &&
              stopEventStageInst.stageVersion === stopEventStage.version) {
              saConfig = stopEventStageInst.configuration;
            }
            eventStage = pipelineService.getNewStageInstance({
              stage: stopEventStage,
              pipelineConfig: $scope.pipelineConfig,
              stopEventStage: true,
              configuration: saConfig
            });

            newValue.stopEventStages = [ eventStage ]
          }
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

    $scope.$on('onOriginStageDelete', function () {
      if ($scope.executionMode !== 'EDGE') {
        api.pipelineAgent.resetOffset($scope.activeConfigInfo.pipelineId);
      }
    });

    $scope.$on('onPipelineConfigSelect', function(event, configInfo) {
      if (configInfo) {
        $scope.activeConfigInfo = configInfo;
        $scope.closePreview();
        loadPipelineConfig($scope.activeConfigInfo.pipelineId);
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

    infoNameWatchListener = $scope.$watch('pipelineConfig.info.pipelineId', function() {
      $scope.isPipelineRunning = derivePipelineRunning();
      $scope.activeConfigStatus = $rootScope.common.pipelineStatusMap[routeParamPipelineName];
      if ($scope.activeConfigStatus && $scope.activeConfigStatus.attributes &&
        $scope.activeConfigStatus.attributes.IS_REMOTE_PIPELINE === true) {
        $scope.isRemotePipeline = true;
        $scope.isPipelineReadOnly = true;
      }
    });

    pipelineStatusWatchListener = $rootScope.$watch('common.pipelineStatusMap["' + routeParamPipelineName + '"]',
      function() {
        var oldActiveConfigStatus = $scope.activeConfigStatus || {};

        $scope.isPipelineRunning = derivePipelineRunning();
        $scope.activeConfigStatus = $rootScope.common.pipelineStatusMap[routeParamPipelineName] || {};

        if (oldActiveConfigStatus.timeStamp && oldActiveConfigStatus.timeStamp !== $scope.activeConfigStatus.timeStamp &&
          _.contains(['START_ERROR', 'STARTING_ERROR','RUNNING_ERROR', 'RUN_ERROR', 'CONNECT_ERROR', 'STOP_ERROR', 'STOPPING_ERROR'], $scope.activeConfigStatus.status)
        ) {

          var status = $scope.activeConfigStatus;

          if (status.attributes && status.attributes.issues) {
            $rootScope.common.errors = [status.attributes.issues];
          } else {
            $rootScope.common.errors = ['Pipeline Status: ' + $scope.activeConfigStatus.status + ': ' +
            $scope.activeConfigStatus.message];
          }

        }

        if ($scope.activeConfigStatus.status === 'RETRY') {
          updateRetryCountdown($scope.activeConfigStatus.nextRetryTimeStamp);
        }

        if ($scope.activeConfigStatus && $scope.activeConfigStatus.attributes &&
          $scope.activeConfigStatus.attributes.IS_REMOTE_PIPELINE === true) {
          $scope.isRemotePipeline = true;
          $scope.isPipelineReadOnly = true;
        }

      });

    metricsWatchListener = $rootScope.$watch('common.pipelineMetrics', function() {
      var pipelineStatus = $rootScope.common.pipelineStatusMap[routeParamPipelineName],
        config = $scope.pipelineConfig;
      if (pipelineStatus && config && pipelineStatus.pipelineId === config.info.pipelineId &&
        $scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {

        if (!$scope.snapshotMode) {
          $scope.$broadcast('updateErrorCount', getStageErrorCounts());
        }

        $scope.triggeredAlerts = pipelineService.getTriggeredAlerts(
          routeParamPipelineName,
          $scope.pipelineRules,
          $rootScope.common.pipelineMetrics
        );

        $scope.$broadcast('updateEdgePreviewIconColor', $scope.pipelineRules, $scope.triggeredAlerts);
      } else {
        $scope.triggeredAlerts = [];
      }
    });

    errorsWatchListener = $rootScope.$watch('common.errors', function(newValue, oldValue) {
      var commonErrors = $rootScope.common.errors;
      if ((commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) ||
        (oldValue && oldValue.length && commonErrors && commonErrors.length === 0)){
        $scope.refreshGraph();
      }
    });

    $scope.$on('$destroy', function() {
      if (!reloadingNew) {
        //In case of closing page
        setTimeout(function() {
          setTimeout(function() {
            //If user clicked cancel for reload the page
            //refreshPipelineStatus();
          }, 1000);
        },1);
      }

      reloadingNew = false;

      if (isWebSocketSupported) {
        if (metricsWebSocket) {
          metricsWebSocket.close();
        }
      } else {
        $timeout.cancel(pipelineMetricsTimer);
      }

      //Clear all $watch by calling de registration function

      if (infoNameWatchListener) {
        infoNameWatchListener();
      }

      if (pipelineStatusWatchListener) {
        pipelineStatusWatchListener();
      }

      if (metricsWatchListener) {
        metricsWatchListener();
      }

      if (errorsWatchListener) {
        errorsWatchListener();
      }

      destroyed = true;
    });

    $scope.$on('visibilityChange', function(event, isHidden) {
      if (isHidden) {
        if (isWebSocketSupported && metricsWebSocket) {
          metricsWebSocket.close();
        } else if (!isWebSocketSupported){
          $timeout.cancel(pipelineMetricsTimer);
        }
        pageHidden = true;
      } else {
        if (_.contains(['RUNNING', 'STARTING'], $rootScope.common.pipelineStatusMap[routeParamPipelineName].status)) {
          refreshPipelineMetrics();
        }
        pageHidden = false;
      }
    });

    $scope.$on('onAlertClick', function(event, alert) {
      if (alert && alert.pipelineName === $scope.activeConfigInfo.pipelineId) {
        var edges = $scope.edges,
            edge;

        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;

        if (alert.ruleDefinition.metricId) {
          //Select Pipeline Config
          $scope.$broadcast('selectNode');
          $scope.changeStageSelection({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE,
            detailTabName: 'summary'
          });
        } else {
          //Select edge
          edge = _.find(edges, function(ed) {
            return ed.outputLane === alert.ruleDefinition.lane;
          });
          $scope.changeStageSelection({
            selectedObject: edge,
            type: pipelineConstant.LINK,
            detailTabName: 'summary'
          });
        }
      } else {
        $rootScope.common.clickedAlert = alert;
        $location.path('/collector/pipeline/' + alert.pipelineName);
      }
    });

    $scope.$on('bodyUndoKeyPressed', function() {
      $scope.undo();
    });

    $scope.$on('bodyRedoKeyPressed', function() {
      $scope.redo();
    });

  });
