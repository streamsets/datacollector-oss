/*
 * Copyright 2020 StreamSets Inc.
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
 * Helper functions for information needed for activation tracking
 */

angular.module('dataCollectorApp.common')
  .factory('activationTracking', function(tracking, api, configuration, trackingEvent, $q) {
    var activationTracking = {};

    activationTracking.trackActivationEvent = function(activationInfo, previousActivationInfo) {
      // Stats opt in may be changed by activation, so update it
      var info = previousActivationInfo.info;
      var trackingData = {};
      trackingData['Previous Expiration Date'] = info.expiration;
      trackingData['Previous Valid Activation'] = info.valid;
      trackingData['Basic Stages Only'] = info.valid && info.expiration === 0;
      $q.all([
        api.system.getStats(),
        api.admin.getBuildInfo(),
        api.pipelineAgent.getPipelinesCount()
      ]).then(function (sbaResults) {
        var statsResult = sbaResults[0];
        var buildResult = sbaResults[1];
        var pipelinesCountResult = sbaResults[2];

        configuration.setAnalyticsEnabled(statsResult.data.active);
        tracking.trackExtraUserInfo(buildResult, statsResult);
        if (pipelinesCountResult.data) {
          trackingData['Pipeline Count'] = pipelinesCountResult.data.count;
        }
      }).catch(function(err) {
        // If permission error happens, we can assume 0
        trackingData['Pipeline Count'] = 0;
      }).finally(function() {
        tracking.mixpanel.track(trackingEvent.ACTIVATION, trackingData);
        tracking.mixpanel.people.set({'$email': activationInfo.info.userInfo});
        var additionalInfo = activationInfo.info.additionalInfo;
        var firstName = additionalInfo['user.firstName'];
        var lastName = additionalInfo['user.lastName'];
        tracking.mixpanel.people.set({
          '$first_name': firstName,
          '$last_name': lastName,
          '$name': firstName + ' ' + lastName,
          'Company': additionalInfo['user.company']
        });
      });
    };

    return activationTracking;
});
