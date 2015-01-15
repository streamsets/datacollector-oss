/**
 * Controller for Detail Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('DetailController', function ($scope, $rootScope, _, pipelineConstant) {
    var generalTab =  {
        name:'general',
        label: 'General',
        template:'app/home/detail/general/general.tpl.html',
        iconClass: 'fa fa-info-circle fa-12x'
      },
      historyTab = {
        name:'history',
        label: 'History',
        template:'app/home/detail/history/history.tpl.html',
        iconClass: 'fa fa-history fa-12x'
      },
      configurationTab = {
        name:'configuration',
        label: 'Configuration',
        template:'app/home/detail/configuration/configuration.tpl.html',
        iconClass: 'fa fa-gear fa-12x'
      },
      rawPreviewTab = {
        name:'rawPreview',
        label: 'Raw Preview',
        template:'app/home/detail/rawPreview/rawPreview.tpl.html',
        iconClass: 'fa fa-eye fa-12x'
      },
      summaryTab = {
        name:'summary',
        label: 'Summary',
        template:'app/home/detail/summary/summary.tpl.html',
        iconClass: 'fa fa-bar-chart fa-12x'
      },
      errorTab = {
        name:'errors',
        label: 'Errors',
        template:'app/home/detail/badRecords/badRecords.tpl.html',
        iconClass: 'fa fa-exclamation-triangle fa-12x'
      },
      dataSummaryTab = {
        name:'summary',
        label: 'Summary',
        template:'app/home/detail/dataSummary/dataSummary.tpl.html',
        iconClass: 'fa fa-bar-chart fa-12x'
      },
      alertsTab = {
        name:'alerts',
        label: 'Alerts',
        template:'app/home/detail/alerts/alerts.tpl.html',
        iconClass: 'glyphicon glyphicon-exclamation-sign fa-12x'
      },
      rulesTab = {
        name:'rules',
        label: 'Rules',
        template:'app/home/detail/rules/rules.tpl.html',
        iconClass: 'fa fa-list fa-12x'
      },
      pipelineTabsEdit = [
        {
          name:'general',
          label: 'General',
          template:'app/home/detail/general/general.tpl.html',
          iconClass: 'fa fa-info-circle fa-12x'
        },
        {
          name:'configuration',
          template:'app/home/detail/configuration/configuration.tpl.html',
          iconClass: 'fa fa-gear fa-12x'
        },
        {
          name:'history',
          label: 'History',
          template:'app/home/detail/history/history.tpl.html',
          iconClass: 'fa fa-history fa-12x'
        }
      ];

    angular.extend($scope, {
      detailPaneTabs: pipelineTabsEdit,

      /**
       * Returns label for Detail Pane
       */
      getDetailPaneLabel: function() {
        var selectedType = $scope.selectedType,
          selectedObject = $scope.selectedObject;

        if(selectedObject) {
          switch(selectedType) {
            case pipelineConstant.PIPELINE:
              return selectedObject.info.name;
            case pipelineConstant.STAGE_INSTANCE:
              return selectedObject.uiInfo.label;
            case pipelineConstant.LINK:
              return 'Link ( ' + selectedObject.source.uiInfo.label + ' - ' + selectedObject.target.uiInfo.label + ' )';
          }
        }
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

      showWarning: function(tab) {
        if(tab.name === 'configuration') {
          return $scope.hasConfigurationIssues($scope.detailPaneConfig);
        }
        return false;
      }
    });

    var getDetailTabsList = function(type, isPipelineRunning, selectedObject) {
      var tabsList = [];
      switch(type) {
        case pipelineConstant.PIPELINE:
          if(isPipelineRunning) {
            tabsList = [summaryTab, errorTab, configurationTab, historyTab];
          } else {
            tabsList = [configurationTab, historyTab];
          }

          return tabsList;
        case pipelineConstant.STAGE_INSTANCE:
          if(isPipelineRunning) {
            tabsList = [summaryTab, errorTab, configurationTab];
          } else {
            tabsList = [configurationTab];
          }

          if($scope.detailPaneConfigDefn.rawSourceDefinition) {
            tabsList.push(rawPreviewTab);
          }

          return tabsList;
        case pipelineConstant.LINK:
          if(isPipelineRunning) {
            return [dataSummaryTab, alertsTab, rulesTab, configurationTab];
          } else {
            return [configurationTab];
          }
          break;
      }
    };

    $scope.$on('onSelectionChange', function(event, selectedObject, type, detailTabName, configName) {
      $scope.detailPaneTabs = getDetailTabsList(type, $scope.isPipelineRunning, selectedObject);

      if(detailTabName) {
        angular.forEach($scope.detailPaneTabs, function(tab) {
          if(tab.name === detailTabName) {
            tab.active = true;
          }
        });
      }

      $scope.autoFocusConfigName = configName;

    });

    $scope.$watch('isPipelineRunning', function(newValue) {
      $scope.detailPaneTabs = getDetailTabsList($scope.selectedType, newValue);
      $scope.detailPaneTabs[0].active = true;
    });

    $scope.$on('showBadRecordsSelected', function() {
      angular.forEach($scope.detailPaneTabs, function(tab) {
        if(tab.name === 'errors') {
          tab.active = true;
        }
      });
    });


  });