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

-module(ejson).
-export([encode/1, decode/1, validate/1]).
-on_load(init/0).

init() ->
    ok.


decode(IoList) ->
    Fun = mochijson2:decoder([{object_hook, fun({struct, List}) -> {List} end}]),
    Fun(IoList).


encode(EJson) ->
    iolist_to_binary(mochijson2:encode(EJson)).

validate(IoList, _) ->
    {ok, iolist_to_binary(IoList)}.
