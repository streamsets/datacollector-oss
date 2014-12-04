/**
 * Controller for Configuration.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('ConfigurationController', function ($scope, $rootScope, _, api) {
    angular.extend($scope, {

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
       * Toggles selection of value in given Array.
       *
       * @param arr
       * @param value
       */
      toggleSelector: function(arr, value) {
        var index = _.indexOf(arr, value);
        if(index !== -1) {
          arr.splice(index, 1);
        } else {
          arr.push(value);
        }
      },

      /**
       * Remove the field from uiInfo.inputFields and passed array.
       * @param fieldArr
       * @param index
       * @param configValueArr
       */
      removeFieldSelector: function(fieldArr, index, configValueArr) {
        var field = fieldArr[index];
        fieldArr.splice(index, 1);

        index = _.indexOf(configValueArr, field.name);
        if(index !== -1) {
          configValueArr.splice(index, 1);
        }
      },

      /**
       * Adds new field to the array uiInfo.inputFields
       * @param fieldArr
       */
      addNewField: function(fieldArr) {
        if(this.newFieldName) {
          fieldArr.push({
            name: this.newFieldName
          });
          this.newFieldName = '';
        }
      },

      rawSourcePreview: function() {
        api.pipelineAgent.previewRawSource($scope.activeConfigInfo.name, 0, $scope.detailPaneConfig.uiInfo.rawSource.configuration)
          .success(function(data) {
            $scope.rawSourcePreviewData = data ? data.previewString : '';
          })
          .error(function(data, status, headers, config) {
            $rootScope.common.errors = [data];
          });
      }
    });
  });