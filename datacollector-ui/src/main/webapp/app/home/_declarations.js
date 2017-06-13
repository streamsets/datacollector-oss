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
 * Home module for displaying home page content.
 */

angular
  .module('dataCollectorApp.home', [
    'ngRoute',
    'jsonFormatter',
    'splitterDirectives',
    'tabDirectives',
    'pipelineGraphDirectives',
    'commonUI.commonDirectives',
    'commonUI.filters',
    'commonUI.codemirrorDirectives',
    'ui.bootstrap',
    'angularMoment',
    'nvd3',
    'ui.select',
    'showLoadingDirectives',
    'recordTreeDirectives',
    'ui.bootstrap.datetimepicker'
  ])
  .constant('amTimeAgoConfig', {
    withoutSuffix: true
  });