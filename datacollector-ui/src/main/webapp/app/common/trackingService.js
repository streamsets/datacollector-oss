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
 * Service for calling tracking tools if possible
 */

angular.module('dataCollectorApp.common')
  .factory('tracking', function() {
    var tracking = {
      FS: {},
      mixpanel: {
        people: {}
      }
    };

    // Fullstory
    tracking.FS.setUserVars = function() {
      if(typeof FS === 'object') {
        return FS.setUserVars(arguments);
      } else {
        return null;
      }
    };
    tracking.FS.restart = function() {
      if(typeof FS === 'object') {
        return FS.restart(arguments);
      } else {
        return null;
      }
    };
    tracking.FS.shutdown = function() {
      if(typeof FS === 'object') {
        return FS.shutdown(arguments);
      } else {
        return null;
      }
    };

    // Mixpanel
    tracking.mixpanel.opt_in_tracking = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.opt_in_tracking(arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.opt_out_tracking = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.opt_out_tracking(arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.opt_out_tracking = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.opt_out_tracking(arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.identify = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.identify(arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.people.set = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.people.set(arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.register = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.register(arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.track = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.track(arguments);
      } else {
        return null;
      }
    };


    return tracking;
});
