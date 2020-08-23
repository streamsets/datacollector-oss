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
 * Controller for Rules Configuration tab.
 */

angular
  .module('dataCollectorApp.home')
  .controller('RulesConfigurationController', function ($rootScope, $scope, pipelineService) {
    angular.extend($scope, {
      pipelineRulesConfigDefinition: pipelineService.getPipelineRulesConfigDefinition(),
      detailPaneConfig: $scope.pipelineRules,

      verifyDependsOnMap: function(stageInstance, configDefinition) {
        var returnValue = true,
          valueMap = _.object(_.map(stageInstance.configuration, function(configuration) {
            return [configuration.name, configuration.value];
          }));

        angular.forEach(configDefinition.dependsOnMap, function(triggeredByValues, dependsOn) {
          var dependsOnConfigValue = valueMap[dependsOn];
          if (dependsOnConfigValue === undefined ||
            !_.contains(triggeredByValues, dependsOnConfigValue)) {
            returnValue = false;
          }
        });

        return returnValue;
      },

      getConfigIndex: function(stageInstance, configDefinition) {
        if (stageInstance && configDefinition) {
          var configIndex;

          angular.forEach(stageInstance.configuration, function (config, index) {
            if (configDefinition.name === config.name) {
              configIndex = index;
            }
          });

          if (configIndex === undefined) {
            //No configuration found, added the configuration with default value
            stageInstance.configuration.push({
              name: configDefinition.name,
              value: (configDefinition.defaultValue !== undefined && configDefinition.defaultValue !== null) ? configDefinition.defaultValue : undefined
            });

            configIndex = stageInstance.configuration.length - 1;
          }

          return configIndex;
        }
      },

      getCodeMirrorOptions: function(options, configDefinition) {
        var codeMirrorOptions = {};

        if (configDefinition.type !== 'TEXT') {
          codeMirrorOptions = {
            dictionary: $scope.getCodeMirrorHints(configDefinition),
            lineWrapping: $rootScope.$storage.lineWrapping
          };
        } else {
          codeMirrorOptions = {
            dictionary: $scope.getTextCodeMirrorHints(configDefinition),
            lineWrapping: $rootScope.$storage.lineWrapping
          };
        }

        return angular.extend(codeMirrorOptions, pipelineService.getDefaultELEditorOptions(), options);
      },

      getCodeMirrorHints: function(configDefinition) {
        var pipelineConfig = $scope.pipelineConfig;
        var pipelineConstants = _.find(pipelineConfig.configuration, function (config) {
          return config.name === 'constants';
        });
        var hints = pipelineService.getGeneralRulesElMetadata();
        return {
          elFunctionDefinitions: hints.elFunctionDefinitions,
          elConstantDefinitions: hints.elConstantDefinitions,
          pipelineConstants: pipelineConstants ? pipelineConstants.value : [],
          runtimeConfigs: pipelineService.getRuntimeConfigs(),
          regex: 'wordColonSlash'
        };
      },

      getTextCodeMirrorHints: function(configDefinition) {
        var hints = $scope.getCodeMirrorHints(configDefinition);

        return {
          elFunctionDefinitions: hints.elFunctionDefinitions,
          elConstantDefinitions: hints.elConstantDefinitions,
          pipelineConstants: hints.pipelineConstants,
          textMode: configDefinition.mode,
          regex: "wordColonSlashBracket"
        };
      },

      addToList: function(stageInstance, configValue) {
        configValue.push('');
      },

      removeFromList: function(stageInstance, configValue, $index) {
        configValue.splice($index, 1);
      },

      addToMap: function(stageInstance, configValue) {
        configValue.push({
          key: '',
          value: ''
        });
      },

      removeFromMap: function(stageInstance, configValue, mapObject, $index) {
        configValue.splice($index, 1);
      },

      /**
       * Add Object to Custom Field Configuration.
       *
       * @param stageInstance
       * @param config
       * @param configDefinitions
       */
      addToCustomField: function(stageInstance, config, configDefinitions) {
        var complexFieldObj = {};
        angular.forEach(configDefinitions, function (complexFiledConfigDefinition) {
          var complexFieldConfig = pipelineService.setDefaultValueForConfig(
            $scope.pipelineRulesConfigDefinition,
            complexFiledConfigDefinition,
            stageInstance
          );
          complexFieldObj[complexFieldConfig.name] = (complexFieldConfig.value !== undefined && complexFieldConfig.value !== null) ? complexFieldConfig.value : undefined;
        });
        if (config.value) {
          config.value.push(complexFieldObj);
        } else {
          config.value = [complexFieldObj];
        }
      },


      /**
       * Remove Object from Custom Field Configuration.
       *
       * @param stageInstance
       * @param config
       * @param configValue
       * @param $index
       */
      removeFromCustomField: function(stageInstance, config, configValue, $index) {
        configValue.splice($index, 1);
      },

      /**
       * Returns true if dependsOn Custom Field configuration contains value in triggeredByValues.
       *
       * @param stageInstance
       * @param customFieldConfigValue
       * @param customConfiguration
       * @returns {*}
       */
      verifyCustomFieldDependsOn: function(stageInstance, customFieldConfigValue, customConfiguration) {
        var returnValue = true;

        angular.forEach(customConfiguration.dependsOnMap, function(triggeredByValues, dependsOn) {
          var dependsOnConfigValue = customFieldConfigValue[dependsOn];
          if (dependsOnConfigValue === undefined ||
            !_.contains(triggeredByValues, dependsOnConfigValue)) {
            returnValue = false;
          }
        });

        return returnValue;
      },

      /**
       * Checks if a configuration should be shown, based on displayMode and stage settings
       * @param {String} configurationItemDisplayMode
       * @param {String} stageDisplayMode
       * @returns {Boolean}
       */
      isShownByConfigDisplayMode: function(configurationItemDisplayMode, stageDisplayMode) {
        stageDisplayMode = stageDisplayMode || $scope.pipelineConstant.DISPLAY_MODE_ADVANCED;
        return configurationItemDisplayMode === $scope.pipelineConstant.DISPLAY_MODE_BASIC ||
            stageDisplayMode === $scope.pipelineConstant.DISPLAY_MODE_ADVANCED;
      },

      /**
       * Returns issues for the given Configuration Object and Definition.
       *
       * @param configObject
       * @param configDefinition
       */
      getConfigurationIssues: function(configObject, configDefinition) {
        var config = $scope.pipelineRules;
        var issues;

        if (config && config.configIssues && config.configIssues.length) {
          issues =  _.filter((config.configIssues), function(issue) {
            return (issue.configName === configDefinition.name);
          });
        }
        return issues;
      }
    });


  });
