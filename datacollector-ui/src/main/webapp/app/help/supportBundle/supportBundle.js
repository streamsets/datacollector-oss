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
 * Controller for Settings Modal Dialog.
 */

angular
    .module('dataCollectorApp')
    .controller('SupportBundleModalInstanceController', function ($scope, $rootScope, $modalInstance, $window, api, configuration) {

  // True if we're waiting on list of generators from SDC
  $scope.showLoading = true;
  // True if user pressed "Upload" button and the bundle is still "uploading"
  $scope.uploading = false;
  // Messaging to show on the UI
  $scope.message = null;
  // Validate that bundle upload is allowed
  $scope.isSupportBundleUplodEnabled = configuration.isSupportBundleUplodEnabled();

  api.admin.getSdcId().then(function(res) {
    $scope.sdc_id = res.data.id;
  }, function(res) {
    $scope.common.errors = [res.data];
  });

  api.system.getSupportBundleGenerators().then(function(res) {
    $scope.showLoading = false;
    $scope.generators = _.map(res.data, function(generator) {
      generator.checked = generator.enabledByDefault;
      return generator;
    });
  }, function(res) {
    $scope.showLoading = false;
    $scope.common.errors = [res.data];
  });

  angular.extend($scope, {
    common: {
      errors: []
    },

    hasAnyGeneratorSelected: function() {
      return _.any($scope.generators, function(generator) {
        return generator.checked;
      });
    },

    getSelectedGenerators: function() {
      var selectedGenerators = _.filter($scope.generators, function(generator) {
        return generator.checked;
      });
      var fullyQualifiedClassNames = _.pluck(selectedGenerators, 'klass');
      var simpleClassNames = _.map(fullyQualifiedClassNames, function(className) {
        return _.last(className.split('.'));
      });
      return simpleClassNames;
    },

    toggleGenerator: function($event, generator) {
      if ($($event.target).is(':not(:checkbox)')) {
        generator.checked = !generator.checked;
      }
    },

    downloadBundle: function() {
      if (!this.hasAnyGeneratorSelected()) {
        return '';
      }

      $window.location.href = api.system.getGenerateSupportBundleUrl(this.getSelectedGenerators());
      $scope.message = {id: 'sdcSupportBundle.downloadingMessage', type: 'success'};
    },

    done: function() {
      $modalInstance.dismiss('cancel');
    },
  });
});
