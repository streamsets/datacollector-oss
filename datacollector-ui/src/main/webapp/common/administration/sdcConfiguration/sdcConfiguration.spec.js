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
describe('Controller: modules/sdcConfiguration/SDCConfigurationController', function () {

  beforeEach(module('commonUI.sdcConfiguration'));

  var $controller, $q, $scope, configuration, Analytics;

  beforeEach(inject(function(_$controller_, _$q_) {
    $controller = _$controller_;
    $q = _$q_;
    $scope = {};
    configuration = {
      init: function() {
       return $q.when({});
      }
    };
    Analytics = {};

  }));

  it('calls configuration init method', function() {
    spyOn(configuration, 'init');

    var controller = $controller('SDCConfigurationController', {
      $scope: $scope,
      $rootScope: {},
      $q: $q,
      Analytics: Analytics,
      configuration: configuration,
      _: _
    });

    expect(configuration.init).toHaveBeenCalled();

  });

  it('calls Analytics trackPage method with url /collector/configuration', function() {
    configuration = {
      init: function() {
        var deferred = $q.defer();
        deferred.resolve();
        return deferred.promise;
      },
      isAnalyticsEnabled: function() {
        return true;
      }
    };

    Analytics = {
      trackPage: function(url) {

      }
    };

    spyOn(configuration, 'init');
    spyOn(Analytics, 'trackPage');

    var controller = $controller('SDCConfigurationController', {
      $scope: $scope,
      $rootScope: {},
      $q: $q,
      Analytics: Analytics,
      configuration: configuration,
      _: _
    });

    expect($scope.initDefer).toBeDefined();

    $scope.initDefer.then(function() {
      expect(configuration.init).toHaveBeenCalled();
      expect(Analytics.trackPage).toHaveBeenCalledWith('/collector/configuration');
    });

  });

});