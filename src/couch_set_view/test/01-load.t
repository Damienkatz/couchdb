#!/usr/bin/env escript
%% -*- erlang -*-

% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

main(_) ->
    test_util:init_code_path(),
    Modules = [
        couch_set_view,
        couch_set_view_http,
        couch_set_view_group,
        couch_set_view_updater,
        couch_set_view_compactor,
        couch_set_view_util,
        couch_db_set
    ],

    etap:plan(length(Modules)),
    lists:foreach(
        fun(Module) ->
            etap_can:loaded_ok(Module, lists:concat(["Loaded: ", Module]))
        end, Modules),
    etap:end_tests().
