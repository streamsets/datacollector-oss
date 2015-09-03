/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
 * Controller for Library Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('LibraryController', function ($scope, $rootScope,  $route, $location, $modal, _, api, pipelineService) {

    angular.extend($scope, {

      /**
       * Emit 'onPipelineConfigSelect' event when new configuration is selected in library panel.
       *
       * @param pipeline
       */
      onSelect : function(pipeline) {
        //$rootScope.$broadcast('onPipelineConfigSelect', pipeline);
        $scope.reloadingNewPipeline();
        $location.path('/collector/pipeline/' + pipeline.name);
      },

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        pipelineService.addPipelineConfigCommand();
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.deletePipelineConfigCommand(pipelineInfo, $event)
          .then(function(pipelines) {
            if(pipelines.length) {
              $location.path('/collector/pipeline/' + pipelines[0].name);
            } else {
              $location.path('/');
            }
          });
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.duplicatePipelineConfigCommand(pipelineInfo, $event)
          .then(function(newPipelineConfig) {
            $location.path('/collector/pipeline/' + newPipelineConfig.info.name);
          });
      },

      /**
       * Import link command handler
       */
      importPipelineConfig: function(pipelineInfo, $event) {
        pipelineService.importPipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Export link command handler
       */
      exportPipelineConfig: function(pipelineInfo, $event) {
        $event.stopPropagation();
        api.pipelineAgent.exportPipelineConfig(pipelineInfo.name);
      }

    });
  });