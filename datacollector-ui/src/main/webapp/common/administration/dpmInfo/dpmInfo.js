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
 * Controller for Control Hub Info Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('DPMInfoModalInstanceController', function ($rootScope, $scope, $modalInstance, $modalStack, $modal,
                                                          configuration, $translate) {
    angular.extend($scope, {
      openEnableDPMModal: function () {
        if (configuration.isManagedByClouderaManager()) {
          $translate('home.enableDPM.isManagedByClouderaManager').then(function(translation) {
            $rootScope.common.errors = [translation];
          });
          $modalInstance.dismiss('cancel');
          return;
        }

        $modalStack.dismissAll();
        $modal.open({
          templateUrl: 'common/administration/enableDPM/enableDPM.tpl.html',
          controller: 'EnableDPMModalInstanceController',
          size: 'lg',
          backdrop: 'static'
        });
      },

      cancel: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });
