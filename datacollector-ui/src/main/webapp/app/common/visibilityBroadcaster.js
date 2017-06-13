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
 * Service for broadcasting whether browser is visible or not using HTML5 Page Visibility API.
 */
angular.module('dataCollectorApp.common')
  .service('visibilityBroadcaster', function($rootScope, $document, _) {

    var document = $document[0],
      features,
      detectedFeature;

    features = {
      standard: {
        eventName: 'visibilitychange',
        propertyName: 'hidden'
      },
      moz: {
        eventName: 'mozvisibilitychange',
        propertyName: 'mozHidden'
      },
      ms: {
        eventName: 'msvisibilitychange',
        propertyName: 'msHidden'
      },
      webkit: {
        eventName: 'webkitvisibilitychange',
        propertyName: 'webkitHidden'
      }
    };

    Object.keys(features).some(function(feature) {
      if (_.isBoolean(document[features[feature].propertyName])) {
        detectedFeature = features[feature];
        return true;
      }
    });

    // Feature doesn't exist in browser.
    if (!detectedFeature) {
      return;
    }

    $document.on(detectedFeature.eventName, broadcastChangeEvent);

    function broadcastChangeEvent() {
      $rootScope.$broadcast('visibilityChange',
        document[detectedFeature.propertyName]);
    }

  });