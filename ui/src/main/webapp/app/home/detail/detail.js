/**
 * Controller for Detail Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('DetailController', function ($scope, $rootScope, _) {
    var detailTabsInEditMode = [{
        name:'configuration',
        template:'app/home/detail/configuration/configuration.tpl.html',
        iconClass: 'fa fa-gear fa-12x'
      }
      ],
      detailTabsInRunningMode = [
        {
          name:'summary',
          template:'app/home/detail/summary/summary.tpl.html',
          iconClass: 'fa fa-bar-chart fa-12x'
        },
        {
          name:'errors',
          template:'app/home/detail/badRecords/badRecords.tpl.html',
          iconClass: 'fa fa-exclamation-triangle fa-12x'
        },
        {
          name:'alerts',
          template:'app/home/detail/alerts/alerts.tpl.html',
          iconClass: 'glyphicon glyphicon-exclamation-sign fa-12x'
        },
        {
          name:'rules',
          template:'app/home/detail/rules/rules.tpl.html',
          iconClass: 'fa fa-list fa-12x'
        },
        {
          name:'configuration',
          template:'app/home/detail/configuration/configuration.tpl.html',
          iconClass: 'fa fa-gear fa-12x'
        }
      ];

    angular.extend($scope, {
      detailPaneTabs: detailTabsInEditMode,

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

    $scope.$watch('isPipelineRunning', function(newValue) {
      if(newValue) {
        $scope.detailPaneTabs = detailTabsInRunningMode;
      } else {
        $scope.detailPaneTabs = detailTabsInEditMode;
      }
    });

    $scope.$on('showBadRecordsSelected', function() {
      angular.forEach($scope.detailPaneTabs, function(tab) {
        if(tab.name === 'errors') {
          tab.active = true;
        }
      });
    });


  });