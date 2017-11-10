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
 * Controller for Detail Pane.
 */

angular
  .module('dataCollectorApp.home')
  .controller('DetailController', function ($scope, $rootScope, _, pipelineConstant, api, contextHelpService, $modal) {
    var infoTab = {
      name: 'info',
      template: 'app/home/detail/info/info.tpl.html',
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
      iconClass: 'fa fa-gear'
    };
    var rawPreviewTab = {
      name: 'rawPreview',
      template: 'app/home/detail/rawPreview/rawPreview.tpl.html',
      iconClass: 'fa fa-eye'
    };
    var summaryTab = {
      name: 'summary',
      template: 'app/home/detail/summary/summary.tpl.html',
      iconClass: 'fa fa-bar-chart',
      active: true,
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

    /**
     * Returns list tabs based on type.
     *
     * @param type
     * @param isPipelineRunning
     * @returns {*}
     */
    var getDetailTabsList = function(type, isPipelineRunning) {
      var tabsList = [],
        executionMode = $scope.activeConfigStatus.executionMode;
      switch(type) {
        case pipelineConstant.PIPELINE:
          if (isPipelineRunning) {
            if (executionMode === pipelineConstant.CLUSTER || executionMode === pipelineConstant.CLUSTER_BATCH ||
                executionMode === pipelineConstant.CLUSTER_YARN_STREAMING || executionMode === pipelineConstant.CLUSTER_MESOS_STREAMING) {
              tabsList = [summaryTab, infoTab, configurationTab, historyTab];
            } else {
              tabsList = [summaryTab, errorTab, infoTab, configurationTab, rulesTab, historyTab];
            }
          } else {
            tabsList = [infoTab, configurationTab, rulesTab, historyTab];
          }

          return tabsList;
        case pipelineConstant.STAGE_INSTANCE:
          if (isPipelineRunning) {
            if (executionMode === pipelineConstant.CLUSTER || executionMode === pipelineConstant.CLUSTER_BATCH ||
              executionMode === pipelineConstant.CLUSTER_YARN_STREAMING || executionMode === pipelineConstant.CLUSTER_MESOS_STREAMING) {
              tabsList = [summaryTab, infoTab, configurationTab];
            } else {
              tabsList = [summaryTab, errorTab, infoTab, configurationTab];
            }
          } else {
            tabsList = [infoTab, configurationTab];
          }

          if ($scope.detailPaneConfigDefn && $scope.detailPaneConfigDefn.rawSourceDefinition) {
            tabsList.push(rawPreviewTab);
          }

          return tabsList;
        case pipelineConstant.LINK:
          if (isPipelineRunning) {
            if (executionMode === pipelineConstant.CLUSTER || executionMode === pipelineConstant.CLUSTER_BATCH ||
              executionMode === pipelineConstant.CLUSTER_YARN_STREAMING || executionMode === pipelineConstant.CLUSTER_MESOS_STREAMING) {
              return [dataRulesTab, dataDriftRulesTab, infoTab];
            } else {
              return [dataSummaryTab, dataRulesTab, dataDriftRulesTab, infoTab];
            }
          } else {
            return [infoTab, dataRulesTab, dataDriftRulesTab];
          }
          break;
      }
    };

    angular.extend($scope, {
      detailPaneTabs: getDetailTabsList(pipelineConstant.PIPELINE, false),
      timeOptions: [
        'latest',
        'last5m',
        'last15m',
        'last1h',
        'last6h',
        'last12h',
        'last24h',
        'last2d',
        'last7d',
        'last30d',
        'custom'
      ],
      timeRange: 'latest',
      customFromTime: null,
      customToTime: null,


      /**
       * Returns label for Time Range
       */
      getTimeRangeLabel: function() {
        var timeRange = $scope.timeRange;
        switch(timeRange) {
          case 'latest':
            return 'Latest';
          case 'last5m':
            return '5 minutes ago to a few seconds ago';
          case 'last15m':
            return '15 minutes ago to a few seconds ago';
          case 'last1h':
            return 'an hour ago to a few seconds ago';
          case 'last6h':
            return '6 hours ago to a few seconds ago';
          case 'last12h':
            return '12 hours ago to a few seconds ago';
          case 'last24h':
            return 'a day ago to a few seconds ago';
          case 'last2d':
            return '2 days ago to a few seconds ago';
          case 'last7d':
            return '7 days ago to a few seconds ago';
          case 'last30d':
            return 'a month ago to a few seconds ago';
          case 'custom':
            return '2 days ago to a few seconds ago';
        }
      },

      /**
       * Returns label for Time Range
       */
      getTimeRangeWhereCondition: function() {
        var timeRange = $scope.timeRange;
        switch(timeRange) {
          case 'last5m':
            return '(time > now() - 5m)';
          case 'last15m':
            return '(time > now() - 15m)';
          case 'last1h':
            return '(time > now() - 1h)';
          case 'last6h':
            return '(time > now() - 6h)';
          case 'last12h':
            return '(time > now() - 12h)';
          case 'last24h':
            return '(time > now() - 24h)';
          case 'last2d':
            return '(time > now() - 1d)';
          case 'last7d':
            return '(time > now() - 7d)';
          case 'last30d':
            return '(time > now() - 30d)';
        }
      },

      changeTimeRange: function(timeRange) {
        $scope.timeRange = timeRange;
      },

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
              (pipelineRules.configIssues && pipelineRules.configIssues.length))
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

      /**
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
                id: libraryId,
                label: libraryId
              }];
            }
          }
        });
        modalInstance.result.then(function() {
          angular.forEach(libraryList, function(library) {
            library.installed = true;
          });
        }, function () {
        });
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
      }

      if (!options.detailTabName && options.type === pipelineConstant.LINK && !$scope.isPipelineRunning &&
        $scope.detailPaneTabs.length > 1) {
        $scope.detailPaneTabs[1].active = true;
      }
    });

    $scope.$watch('isPipelineRunning', function(newValue) {
      var tabs = $scope.detailPaneTabs = getDetailTabsList($scope.selectedType, newValue);

      if (newValue || $scope.detailPaneTabs.length < 2 ) {
        angular.forEach(tabs, function(tab) {
          tab.active = (tab.name === 'summary');
        });
      } else if ($scope.detailPaneTabs.length > 1) {
        angular.forEach(tabs, function(tab) {
          tab.active = (tab.name === 'configuration' || tab.name === 'dataRules');
        });
      }
    });

  });
