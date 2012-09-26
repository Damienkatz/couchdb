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

-module(couch_ejson_compare).

-export([less/2, less_json/2]).

-on_load(init/0).

-type raw_json() :: binary() | iolist() | {'json', binary()} | {'json', iolist()}.


init() -> ok.


-spec less(EJsonKey1::term(), EJsonKey2::term()) -> -1 .. 1.
less(A, B) ->
    less_erl(A, B).


-spec less_json(raw_json(), raw_json()) -> -1 .. 1.
less_json(A, B) ->
    less_erl(get_ejson(A), get_ejson(B)).



less_erl(A,A)                                 -> 0;

less_erl(A,B) when is_atom(A), is_atom(B)     -> convert(atom_sort(A) - atom_sort(B));
less_erl(A,_) when is_atom(A)                 -> -1;
less_erl(_,B) when is_atom(B)                 -> 1;

less_erl(A,B) when is_number(A), is_number(B) -> convert(A - B);
less_erl(A,_) when is_number(A)               -> -1;
less_erl(_,B) when is_number(B)               -> 1;

less_erl(A,B) when is_binary(A), is_binary(B) -> collate(A,B);
less_erl(A,_) when is_binary(A)               -> -1;
less_erl(_,B) when is_binary(B)               -> 1;

less_erl(A,B) when is_list(A), is_list(B)     -> less_list(A,B);
less_erl(A,_) when is_list(A)                 -> -1;
less_erl(_,B) when is_list(B)                 -> 1;

less_erl({A},{B}) when is_list(A), is_list(B) -> less_props(A,B);
less_erl({A},_) when is_list(A)               -> -1;
less_erl(_,{B}) when is_list(B)               -> 1.

atom_sort(null) -> 1;
atom_sort(false) -> 2;
atom_sort(true) -> 3.

less_props([], [_|_]) ->
    -1;
less_props(_, []) ->
    1;
less_props([{AKey, AValue}|RestA], [{BKey, BValue}|RestB]) ->
    case collate(AKey, BKey) of
    0 ->
        case less_erl(AValue, BValue) of
        0 ->
            less_props(RestA, RestB);
        Result ->
            Result
        end;
    Result ->
        Result
    end.

less_list([], [_|_]) ->
    -1;
less_list(_, []) ->
    1;
less_list([A|RestA], [B|RestB]) ->
    case less_erl(A,B) of
    0 ->
        less_list(RestA, RestB);
    Result ->
        Result
    end.

convert(N) when N < 0 -> -1;
convert(N) when N > 0 ->  1;
convert(_)            ->  0.

collate(BinA, BinB) when BinA == BinB ->
    0;
collate(BinA, BinB) when BinA < BinB ->
    -1;
collate(_BinA, _BinB) ->
    1.


get_ejson({json, RawJson}) ->
    ejson:decode(RawJson);
get_ejson(RawJson) ->
    ejson:decode(RawJson).
