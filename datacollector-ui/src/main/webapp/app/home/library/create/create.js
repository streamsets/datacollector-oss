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
 * Controller for Library Pane Create Modal.
 */

angular
  .module('dataCollectorApp.home')
  .controller('CreateModalInstanceController', function (
    $scope, $modalInstance, $translate, api, pipelineType, tracking, trackingEvent
  ) {
    api.pipelineAgent.getPipelineLabels().then(function(res) {$scope.pipelineLabels = res.data;});
    angular.extend($scope, {
      common: {
        errors: []
      },
      selectedSource: '',
      selectedProcessors: {},
      selectedTargets: {},
      newConfig : {
        name: '',
        description: '',
        pipelineType: pipelineType !== undefined ? pipelineType: 'DATA_COLLECTOR',
        pipelineLabel: '',
      },
      currentLabel: '',

      save : function () {
        if($scope.newConfig.name) {
          api.pipelineAgent.createNewPipelineConfig(
            $scope.newConfig.name,
            $scope.newConfig.description,
            $scope.newConfig.pipelineType,
            $scope.newConfig.pipelineLabel
          ).then(
            function(res) {
              $modalInstance.close(res.data);
              tracking.mixpanel.people.set({'Core Journey Stage - Pipeline Created': true});
              tracking.mixpanel.track(trackingEvent.PIPELINE_CREATED, {'Pipeline ID': res.data.pipelineId});
              tracking.FS.event(trackingEvent.PIPELINE_CREATED, {'Pipeline ID': res.data.pipelineId});
            },
            function(res) {
              $scope.common.errors = [res.data];
            }
          );
        } else {
          $translate('home.library.nameRequiredValidation').then(function(translation) {
            $scope.common.errors = [translation];
          });
        }
      },
      cancel : function () {
        $modalInstance.dismiss('cancel');
      },

      refreshResults : function ($select){
        var search = $select.search,
          list = angular.copy($scope.pipelineLabels);
        //remove last user input
        list = list.filter(function(item) {
          return item !== $scope.currentLabel;
        });

        if (!search) {
          //use the predefined list
          $scope.pipelineLabels = list;
        }
        else {
          //manually add user input and set selection
          $scope.currentLabel = search;
          $scope.pipelineLabels = [search].concat(list);
          $select.selected = search;
      }
    }
    });

  });
