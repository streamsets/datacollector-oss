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

// Controller for Detail Pane.
angular
  .module('dataCollectorApp.home')
  .controller('DetailController', function (
    $scope, $rootScope, _, pipelineConstant, api, contextHelpService, $modal,
    authService, userRoles, configuration, pipelineTracking
  ) {
    var infoTab = {
      name: 'info',
      template: 'app/home/detail/info/info.tpl.html',
      iconClass: 'fa fa-info-circle'
    };
    var eventsInfoTab = {
      name: 'eventsInfo',
      template: 'app/home/detail/eventsInfo/eventsInfo.tpl.html',
      iconClass: 'fa fa-info-circle'
    };
    var historyTab = {
      name: 'history',
      template: 'app/home/detail/history/history.tpl.html',
      iconClass: 'fa fa-history'
    };
    var configurationTab = {
      name: 'configuration',
      template: 'app/home/detail/configuration/configuration.tpl.html',
      iconClass: 'fa fa-gear',
      active: true
    };
    var summaryTab = {
      name: 'summary',
      template: 'app/home/detail/summary/summary.tpl.html',
      iconClass: 'fa fa-bar-chart',
      helpId: 'pipeline-monitoring'
    };
    var errorTab = {
      name: 'errors',
      template: 'app/home/detail/badRecords/badRecords.tpl.html',
      iconClass: 'fa fa-exclamation-triangle',
      helpId: 'errors-tab'
    };
    var dataSummaryTab = {
      name: 'summary',
      template: 'app/home/detail/dataSummary/dataSummary.tpl.html',
      iconClass: 'fa fa-bar-chart',
      active: true,
      helpId: 'pipeline-monitoring'
    };
    var dataRulesTab = {
      name: 'dataRules',
      template: 'app/home/detail/rules/dataRules/dataRules.tpl.html',
      iconClass: 'fa fa-list',
      helpId: 'data-rules-tab'
    };
    var dataDriftRulesTab = {
      name: 'dataDriftRules',
      template: 'app/home/detail/rules/dataDriftRules/dataDriftRules.tpl.html',
      iconClass: 'fa fa-list',
      helpId: 'data-drift-rules-tab'
    };
    var rulesTab = {
      name: 'rules',
      template: 'app/home/detail/rules/rules.tpl.html',
      iconClass: 'fa fa-list',
      helpId: 'metric-rules-tab'
    };
    var externalLibrariesTab = {
      name: 'externalLibraries',
      template: 'app/home/detail/external_libraries/external_libraries.tpl.html',
      iconClass: 'fa fa-upload',
      helpId: 'metric-rules-tab'
    };

    var defaultTabName = {
      PIPELINE: 'configuration',
      STAGE_INSTANCE: 'configuration',
      LINK: 'dataRules'
    };

    /**
     * Returns list tabs based on type.
     *
     * @param type
     * @param isPipelineRunning
     * @returns {*}
     */
    var getDetailTabsList = function(type, isPipelineRunning) {
      var tabsList = [];
      var executionMode = $scope.activeConfigStatus.executionMode;
      var isSamplePipeline = $scope.isSamplePipeline;
      var clusterExecutionModePipeline = _.contains(pipelineConstant.CLUSTER_MODES, executionMode);
      switch(type) {
        case pipelineConstant.PIPELINE:
          if (isSamplePipeline) {
            tabsList = [infoTab, configurationTab, rulesTab];
          } else if (isPipelineRunning) {
            if (clusterExecutionModePipeline) {
              tabsList = [summaryTab, infoTab, configurationTab, historyTab];
            } else {
              tabsList =
                [summaryTab, errorTab, infoTab, configurationTab, rulesTab, historyTab];
            }
          } else {
            tabsList = [summaryTab, infoTab, configurationTab, rulesTab, historyTab];
          }

          return tabsList;
        case pipelineConstant.STAGE_INSTANCE:
          if (isSamplePipeline) {
            tabsList = [infoTab, configurationTab];
          } else if (isPipelineRunning) {
            if (clusterExecutionModePipeline) {
              tabsList = [summaryTab, infoTab, configurationTab];
            } else {
              tabsList = [summaryTab, errorTab, infoTab, configurationTab];
            }
          } else {
            tabsList = [summaryTab, errorTab, infoTab, configurationTab];
          }

          if (authService.isAuthorized([userRoles.admin])) {
            tabsList.push(externalLibrariesTab);
          }

          if ($scope.detailPaneConfigDefn && $scope.detailPaneConfigDefn.producingEvents) {
            tabsList.push(eventsInfoTab);
          }

          return tabsList;
        case pipelineConstant.LINK:
          if (isSamplePipeline) {
            return [infoTab, dataRulesTab, dataDriftRulesTab];
          } else if (isPipelineRunning) {
            if (clusterExecutionModePipeline) {
              return [dataRulesTab, dataDriftRulesTab, infoTab];
            } else {
              return [dataSummaryTab, dataRulesTab, dataDriftRulesTab, infoTab];
            }
          } else {
            return [infoTab, dataRulesTab, dataDriftRulesTab];
          }
      }
    };

    angular.extend($scope, {
      detailPaneTabs: getDetailTabsList(pipelineConstant.PIPELINE, false),
      timeRange: 'latest',
      pipelineStateHistory: [],
      runHistory: [],
      showRunHistory: false,
      selectedRunHistory: null,
      detailPaneMetrics: null,

      /**
       * Returns label for Detail Pane
       */
      getDetailPaneLabel: function() {
        var selectedType = $scope.selectedType,
          selectedObject = $scope.selectedObject;

        if (selectedObject) {
          switch(selectedType) {
            case pipelineConstant.PIPELINE:
              return selectedObject.info.title;
            case pipelineConstant.STAGE_INSTANCE:
              return selectedObject.uiInfo.label;
            case pipelineConstant.LINK:
              return 'Stream ( ' + selectedObject.source.uiInfo.label + ' - ' + selectedObject.target.uiInfo.label + ' )';
          }
        }
      },

      getStageLibraryLabel: function(stageInstance) {
        var nameLabelMap = _.find($scope.stageLibraryList, function(labelMap) {
         return stageInstance.library === labelMap.library;
        });

        return nameLabelMap.libraryLabel;
      },

      getSelectedRunLabel: function() {
        if ($scope.selectedRunHistory) {
          return $scope.selectedRunHistory.appName;
        }
        return 'Latest';
      },

      monitorRun: function(history) {
        $scope.selectedRunHistory = history;
        if (history.metrics) {
          $scope.detailPaneMetrics = history.metrics;
        } else if ($scope.selectedRunHistory === $scope.runHistory[0]) {
          $scope.detailPaneMetrics = $rootScope.common.pipelineMetrics;
        }
      },

      /**
       * Checks if configuration has any issue.
       *
       * @param {Object} stageInstance - The Pipeline Configuration/Stage Configuration Object.
       * @returns {Boolean} - Returns true if configuration has any issue otherwise false.
       */
      hasConfigurationIssues: function(stageInstance) {
        var config = $scope.pipelineConfig;
        var commonErrors = $rootScope.common.errors;
        var issuesMap;
        var issues = [];

        if (commonErrors && commonErrors.length && commonErrors[0].pipelineIssues) {
          issuesMap = commonErrors[0];
        } else if (config && config.issues){
          issuesMap = config.issues;
        }

        if (issuesMap) {
          if (stageInstance.instanceName && issuesMap.stageIssues &&
            issuesMap.stageIssues[stageInstance.instanceName]) {
            issues = issuesMap.stageIssues[stageInstance.instanceName];
          } else if (!stageInstance.instanceName && issuesMap.pipelineIssues){
            issues.push.apply(issues, issuesMap.pipelineIssues);

            if (config.errorStage && issuesMap.stageIssues && issuesMap.stageIssues[config.errorStage.instanceName]) {
              issues.push.apply(issues, issuesMap.stageIssues[config.errorStage.instanceName]);
            } if (config.testOriginStage && issuesMap.stageIssues && issuesMap.stageIssues[config.testOriginStage.instanceName]) {
              issues.push.apply(issues, issuesMap.stageIssues[config.testOriginStage.instanceName]);
            } else if (config.statsAggregatorStage && issuesMap.stageIssues &&
              issuesMap.stageIssues[config.statsAggregatorStage.instanceName]) {
              issues.push.apply(issues, issuesMap.stageIssues[config.statsAggregatorStage.instanceName]);
            } else if(config.startEventStages[0] && issuesMap.stageIssues &&
              issuesMap.stageIssues[config.startEventStages[0].instanceName]) {
              issues.push.apply(issues, issuesMap.stageIssues[config.startEventStages[0].instanceName]);
            } else if(config.stopEventStages[0] && issuesMap.stageIssues &&
              issuesMap.stageIssues[config.stopEventStages[0].instanceName]) {
              issues.push.apply(issues, issuesMap.stageIssues[config.stopEventStages[0].instanceName]);
            }
          }
        }

        return _.find(issues, function(issue) {
          return issue.configName;
        });
      },

      /**
       * Checks if configDefinitions has any configs with ADVANCED displayMode, including services
       * @param {*} configDefinitions
       * @param {[]} services
       * @returns {Boolean}
       */
      hasAdvancedConfig: function(configDefinitions, services) {
        services = services || [];
        var serviceConfigDefs = services.reduce(function(acc, service) {
          return acc.concat(service.definition.configDefinitions);
        }, []);
        return configDefinitions.concat(serviceConfigDefs).some(function (x) {
          return x.displayMode === $scope.pipelineConstant.DISPLAY_MODE_ADVANCED;
        }) || configDefinitions.some(function(configDef) {
          return configDef && configDef.model && configDef.model.configDefinitions && configDef.model.configDefinitions.some(function(x) {
            return x.displayMode === $scope.pipelineConstant.DISPLAY_MODE_ADVANCED;
          });
        });
      },

      /**
       * Returns true to display icon on tab.
       *
       * @param tab
       * @returns {*}
       */
      showWarning: function(tab) {
        if (tab.name === 'configuration') {
          return $scope.hasConfigurationIssues($scope.detailPaneConfig);
        }

        if (tab.name === 'rules') {
          var pipelineRules = $scope.pipelineRules;
          return pipelineRules && ((pipelineRules.ruleIssues && pipelineRules.ruleIssues.length) ||
              (pipelineRules.configIssues && pipelineRules.configIssues.length));
        }
        return false;
      },

      /**
       * Select the Rules Tab
       * @param triggeredAlert
       */
      selectRulesTab: function(triggeredAlert) {
        var rulesTabName = (triggeredAlert && triggeredAlert.type === 'DATA_DRIFT_ALERT' ? 'dataDriftRules' : 'dataRules');
        angular.forEach($scope.detailPaneTabs, function(tab) {
          if (tab.name === 'rules' || tab.name === rulesTabName) {
            tab.active = true;
          }
        });
      },

      /**
       * Launch Contextual Help
       */
      launchHelp: function() {
        var helpId = '';
        var selectedObject = $scope.selectedObject;
        var activeTab = _.find($scope.detailPaneTabs, function (tab) {
          return tab.active;
        });

        switch($scope.selectedType) {
          case pipelineConstant.PIPELINE:
            if (activeTab.helpId) {
              helpId = activeTab.helpId;
            } else {
              helpId = 'pipeline-configuration';
            }
            break;
          case pipelineConstant.STAGE_INSTANCE:
            helpId = selectedObject.stageName;
            break;
          case pipelineConstant.LINK:
            helpId = activeTab.helpId;
        }

        contextHelpService.launchHelp(helpId);

      },

      /**
       * Launch Settings for Summary Tab
       */
      launchSettings: function() {
        $scope.$broadcast('launchSummarySettings');
      },

    /*
       * On Tab Select
       * @param tab
       */
      onTabSelect: function(tab) {
        $scope.trackEvent(pipelineConstant.TAB_CATEGORY, pipelineConstant.SELECT_ACTION, tab.name, 1);
        $scope.activeDetailTab = tab;
        switch($scope.selectedType) {
          case pipelineConstant.PIPELINE:
            $scope.selectedDetailPaneTabCache[$scope.pipelineConfig.info.pipelineId] = tab.name;
            break;
          case pipelineConstant.STAGE_INSTANCE:
            $scope.selectedDetailPaneTabCache[$scope.selectedObject.instanceName] = tab.name;
            break;
          case pipelineConstant.LINK:
            $scope.selectedDetailPaneTabCache[$scope.selectedObject.outputLane] = tab.name;
        }
        pipelineTracking.trackTabSelected(
          tab.name,
          $scope.isPipelineRunning,
          $scope.selectedType,
          $scope.pipelineConfig
        );
      },

      /**
       * Callback function on clicking install missing library link
       */
      onInstallMissingLibraryClick: function(libraryId) {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/packageManager/install/install.tpl.html',
          controller: 'InstallModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            customRepoUrl: function () {
              return $rootScope.$storage.customPackageManagerRepoUrl;
            },
            libraryList: function () {
              return [{
                stageLibraryManifest: {
                  stageLibId: libraryId,
                  stageLibLabel: libraryId
                }
              }];
            },
            withStageLibVersion: function () {
              return false; // TODO: Change to true when we update stage instance with StageLibVersion
            }
          }
        });
        modalInstance.result.then(function() {
          angular.forEach(libraryList, function(library) {
            library.installed = true;
          });
        }, function () {
        });
      },

      updateHistory: function (pipelineId) {
        updateHistory(pipelineId);
      }

    });

    $scope.$on('onSelectionChange', function(event, options) {
      $scope.detailPaneTabs = getDetailTabsList(options.type, $scope.isPipelineRunning, options.selectedObject);
      if (options.detailTabName) {
        angular.forEach($scope.detailPaneTabs, function(tab) {
          if (tab.name === options.detailTabName) {
            tab.active = true;
          }
        });
      } else {
        // Make sure at least one of the tab is active.
        var activeTab = _.find($scope.detailPaneTabs, function (tab) {
          return tab.active;
        });
        if (!activeTab) {
          var defaultTab = defaultTabName[$scope.selectedType];
          angular.forEach($scope.detailPaneTabs, function(tab) {
            tab.active = (tab.name === defaultTab);
          });
        }
      }
    });

    $scope.$watch('isPipelineRunning', function(newValue, oldValue) {
      var tabs = $scope.detailPaneTabs = getDetailTabsList($scope.selectedType, newValue);

      if (newValue || oldValue || $scope.detailPaneTabs.length < 2 ) {
        $scope.detailPaneMetrics = $rootScope.common.pipelineMetrics;
        angular.forEach(tabs, function(tab) {
          tab.active = (tab.name === 'summary');
        });
      }
    });

    $scope.$watch('selectedObject', function() {
      if (($scope.selectedType === pipelineConstant.STAGE_INSTANCE || $scope.selectedType == pipelineConstant.PIPELINE) &&
          $scope.detailPaneConfigDefn &&
          $scope.hasAdvancedConfig($scope.detailPaneConfigDefn.configDefinitions) &&
          !$scope.detailPaneConfig.uiInfo.displayMode) {
        if(configuration.defaultShowAdvancedConfigs()) {
          $scope.detailPaneConfig.uiInfo.displayMode = pipelineConstant.DISPLAY_MODE_ADVANCED;
        } else {
          $scope.detailPaneConfig.uiInfo.displayMode = pipelineConstant.DISPLAY_MODE_BASIC;
        }
      }
    });

    $scope.$watch('activeConfigStatus.status', function () {
      updateHistory($scope.pipelineConfig.info.pipelineId);
    });

    var updateHistory = function(pipelineId) {
      if ($scope.isSamplePipeline) {
        $scope.pipelineStateHistory = [];
        return;
      }
      $scope.showLoading = true;
      api.pipelineAgent.getHistory(pipelineId)
        .then(function(res) {
          if(res.data && res.data.length) {
            $scope.pipelineStateHistory = res.data;
          } else {
            $scope.pipelineStateHistory = [];
          }
          generateRunHistory($scope.pipelineStateHistory);
          $scope.showLoading = false;
        })
        .catch(function(res) {
          $scope.showLoading = false;
          $rootScope.common.errors = [res.data];
        });
    };

    var generateRunHistory = function(pipelineStateHistory) {
      $scope.runHistory = [];
      angular.forEach(pipelineStateHistory, function (pipelineState, index) {
        if (pipelineState.metrics != null || _.contains(pipelineConstant.END_STATES, pipelineState.status) ||
          (index === 0 && _.contains(pipelineConstant.ACTIVE_STATES, pipelineState.status))) {

          var  startTimeStamp = getStartTimestamp(pipelineStateHistory, index);

          var metrics = pipelineState.metrics ? JSON.parse(pipelineState.metrics) : null;
          var inputRecords = 0;
          var outputRecords = 0;
          var errorRecordsAndMessages = 0;

          if (metrics && metrics.counters) {
            if (metrics.counters['pipeline.batchInputRecords.counter']) {
              inputRecords = metrics.counters['pipeline.batchInputRecords.counter'].count;
            }
            if (metrics.counters['pipeline.batchOutputRecords.counter']) {
              outputRecords = metrics.counters['pipeline.batchOutputRecords.counter'].count;
            }
            if (metrics.counters['pipeline.batchErrorRecords.counter']) {
              errorRecordsAndMessages += metrics.counters['pipeline.batchErrorRecords.counter'].count;
            }
            if (metrics.counters['pipeline.batchErrorMessages.counter']) {
              errorRecordsAndMessages += metrics.counters['pipeline.batchErrorMessages.counter'].count;
            }
          }

          var run = {
            started: startTimeStamp,
            completed: pipelineState.timeStamp,
            message: pipelineState.message,
            errorStackTrace: pipelineState.attributes['errorStackTrace'],
            metrics: metrics,
            status: pipelineState.status,
            user: pipelineState.user,
            pipelineState: pipelineState,
            stateIndex: index,
            isErrorState: pipelineConstant.ERROR_STATES.indexOf(pipelineState.status) !== -1,
            inputRecords: inputRecords,
            outputRecords: outputRecords,
            errorRecordsAndMessages: errorRecordsAndMessages
          };

          if (index === 0 && _.contains(pipelineConstant.ACTIVE_STATES, pipelineState.status)) {
            run.active = true;
          }

          $scope.runHistory.push(run);
        }
      });
      if ($scope.runHistory.length > 0) {

        for (var i = $scope.runHistory.length - 1, j = 1; i >=0 ; i--, j++) {
          $scope.runHistory[i].appName = 'run' + j;
        }

        $scope.showRunHistory = true;
        $scope.monitorRun($scope.runHistory[0]);
      }
    };

    var getStartTimestamp = function(pipelineStateHistory, startIndex) {
      for (var i = startIndex; i < pipelineStateHistory.length; i++) {
        if (pipelineStateHistory[i].status === 'STARTING') {
          return pipelineStateHistory[i].timeStamp;
        }
      }
      return pipelineStateHistory[startIndex].timeStamp;
    };

    $scope.$on('onPipelineConfigSelect', function(event, configInfo) {
      if (configInfo) {
        updateHistory(configInfo.pipelineId);
      }
    });

    $rootScope.$watch('common.pipelineMetrics', function() {
      if ($scope.selectedRunHistory === $scope.runHistory[0] && $rootScope.common.pipelineMetrics) {
        $scope.detailPaneMetrics = $rootScope.common.pipelineMetrics;
      }
    });
  });
