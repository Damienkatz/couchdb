%% @copyright 2012 Couchbase, Inc.
%%
%% @author Filipe Manana  <filipe@couchbase.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%  http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(couch_view_parser).

-export([start_context/0, parse_chunk/2, next_state/1]).

-on_load(init/0).

-type key_docid()    :: {Key::binary(), DocId::binary()}.
-type map_value()    :: binary() |
                        {PartId::binary(), Node::binary(), Value::binary()}.
-type reduce_value() :: binary().
-type doc_member()   :: {'doc', Doc::binary()}.
-type view_row()     :: {Key::binary(), reduce_value()} |
                        {key_docid(), map_value()} |
                        {key_docid(), map_value(), doc_member()}.

init() -> ok.


-spec start_context() -> {ok, Context::term()}.
start_context() ->
    P = self(),
    Child = spawn_link(fun() -> parse_loop(P) end),
    %io:format("start_context:~p~n", [Child]),
    receive
    {'EXIT', Child, Error} ->
        Error;
    {Child, {ok, need_more_data}} ->
        {ok, Child}
    end.


-spec parse_chunk(Context::term(), iolist()) -> 'ok' | {'error', term()}.
parse_chunk(Ctx, Chunk0) ->
    Chunk = iolist_to_binary(Chunk0),
    %io:format("parse_chunk ~p:~p~n", [Ctx, Chunk]),
    Ctx ! {chunk, Chunk},
    Prev = case get(Ctx) of
        undefined ->
            [];
        Prev0 ->
            Prev0
    end,
    receive
    {Ctx, {ok, need_more_data}} when Prev == [] ->
        %io:format("chunk receive ~p:~p~n", [Ctx, need_more_data]),
        erase(Ctx),
        ok;
    {Ctx, {ok, need_more_data}} ->
        
        %io:format("chunk receive ~p:~p~n", [Ctx, need_more_data]),
        ok;
    {Ctx, {error, Error}} ->
        %io:format("chunk receive ~p:~p~n", [Ctx, Error]),
        put(Ctx, [{error, Error}]),
        {error, Error};
    {Ctx, Resp} ->
        %io:format("chunk receive ~p:~p~n", [Ctx, Resp]),
        put(Ctx, Prev ++ Resp),
        ok
    end.


-spec next_state(Context::term()) ->
                        {'ok', 'need_more_data'} |
                        {'ok', 'debug_infos', [{From::binary(), Value::binary()}]} |
                        {'ok', 'row_count', string()} |
                        {'ok', 'rows', [view_row()]} |
                        {'ok', 'errors', [{From::binary(), Reason::binary()}]} |
                        {'ok', 'done'} |
                        {'error', term()}.
next_state(Ctx) ->
    case get(Ctx) of
    undefined ->
        {ok, need_more_data};
    List ->
        %io:format("next state: ~p~n", [List]),
        case combine_rows(List) of
        [{ok, done}] ->
            % don't erase in case we get called again
            {ok, done};
        [V] ->
            erase(Ctx),
            V;
        [V | Rest] ->
            put(Ctx, Rest),
            V
        end
    end.


combine_rows([{ok, Type, Rows1}, {ok, Type, Rows2} | Rest]) ->
    combine_rows([{ok, Type, Rows1 ++ Rows2} | Rest]);
combine_rows(List) ->
    List.

parse_loop(Parent) ->
    try
        json_stream_parse:events(fun() -> get_data_loop(Parent) end,
                             fun(Ev) -> parse_event_first(Ev) end),
        %io:format("Out of parser ~p~n", [self()]),
        ResultsList = get_responses() ++ [{ok, done}],
        Parent ! {self(), ResultsList}
    catch
        throw:{parse_error, Error} ->
            %io:format("Parse error:~p~n", [Error]),
            Parent ! {self(), {error, Error}};
        _EType:Error ->
            %io:format("other error:~p ~p~n", [{EType,Error}, erlang:get_stacktrace()]),
            Parent ! {self(), {error, Error}}
    end,        
    unlink(Parent).
        
get_data_loop(Parent)->
    case get(done) of
    true ->
        done;
    undefined ->
        Msg = get_responses(),
        if Msg == [] ->
            Parent ! {self(), {ok, need_more_data}};
        true ->
            Parent ! {self(), Msg}
        end,
        receive
        {chunk, Chunk} ->
            {Chunk, fun() -> get_data_loop(Parent) end}
        end
    end.

get_responses() ->
    case get(debug_infos) of
    undefined ->
        [];
    Infos ->
        erase(debug_infos),
        [{ok, debug_infos, Infos}]
    end
    ++
    case get(row_count) of
    undefined ->
        [];
    RowCount ->    
        %io:format("send row count~n"),
        erase(row_count),
        [{ok, row_count, RowCount}]
    end
    ++
    case get(rows) of
    undefined ->
        [];
    Rows ->
        %io:format("send rows~n"),
        erase(rows),
        [{ok, rows, lists:reverse(Rows)}]
    end
    ++
    case get(errors) of
    undefined ->
        [];
    Errors ->
        erase(errors),
        [{ok, errors, lists:reverse(Errors)}]
    end.

parse_event_first(object_start) ->
    fun(Ev) -> parse_event_root_keys(Ev) end.
    
parse_event_root_keys({key, <<"total_rows">>}) ->
    %io:format("parse_key total rows ~p~n", [self()]),
    fun(Ev) -> parse_event_total_rows(Ev) end;

parse_event_root_keys({key, <<"rows">>}) ->
    %io:format("Parse key rows ~p~n", [self()]),
    fun(Ev) -> parse_event_row_values_array_start(Ev) end;

parse_event_root_keys({key, <<"errors">>}) ->
    %io:format("Parse key rows ~p~n", [self()]),
    fun(Ev) -> parse_event_errors_array_start(Ev) end;

parse_event_root_keys({key, <<"debug_info">>}) ->
    %io:format("Parse key rows ~p~n", [self()]),
    fun(Ev) -> parse_event_debug_infos_values(Ev) end;

parse_event_root_keys(object_end) ->
    %io:format("Parse object done ~p~n", [self()]),
    put(done, true),
    fun(Ev) -> parse_event_root_keys(Ev) end.

parse_event_total_rows(RowCount) when is_integer(RowCount) ->
    %io:format("parse_event_total_rows:~p~n", [RowCount]),
    case get(did_row_count) of
    true ->
        % already did it, ignore this one
        ok;
    _ ->    
        put(row_count, integer_to_list(RowCount)),
        put(did_row_count, true)
    end,
    fun(Ev) -> parse_event_root_keys(Ev) end.


parse_event_errors_array_start(array_start) ->
    fun(Ev) -> parse_event_error_values(Ev) end.

parse_event_row_values_array_start(array_start) ->
    fun(Ev) -> parse_event_row_values(Ev) end.


parse_event_error_values(array_end) ->
    %io:format("parse_event_row_values array end ~p~n", [self()]),
    fun(Ev) -> parse_event_root_keys(Ev) end;
parse_event_error_values(object_start) ->
    %io:format("parse_event_row_values ~p~n", [self()]),
    fun(Ev) ->
        json_stream_parse:collect_object(Ev,
            fun({Props}) ->
                From = ejson:encode(proplists:get_value(<<"from">>, Props)),
                Reason = ejson:encode(proplists:get_value(<<"reason">>, Props)),
                case get(errors) of
                undefined ->
                    Errors = [];
                Errors ->
                    ok
                end,
                put(errors, [{From, Reason} | Errors]),
                fun(Ev2) -> parse_event_error_values(Ev2) end
            end)
    end.

parse_event_debug_infos_values(object_start) ->
    %io:format("parse_event_debug_infos_values ~p~n", [self()]),
    fun(Ev) ->
        json_stream_parse:collect_object(Ev,
            fun({Props}) ->
                Props2 = [{ejson:encode(K), ejson:encode(V)} || {K, V} <- Props],
                case get(debug_infos) of
                undefined ->
                    Infos = [];
                Infos ->
                    ok
                end,
                put(debug_infos, Props2 ++ Infos),
                fun(Ev2) -> parse_event_root_keys(Ev2) end
            end)
    end.

parse_event_row_values(array_end) ->
    %io:format("parse_event_row_values array end ~p~n", [self()]),
    fun(Ev) -> parse_event_root_keys(Ev) end;
parse_event_row_values(object_start) ->
    %io:format("parse_event_row_values ~p~n", [self()]),
    fun(Ev) ->
        json_stream_parse:collect_object(Ev,
            fun({Props}) ->
                Key = ejson:encode(proplists:get_value(<<"key">>, Props)),
                Id0 = proplists:get_value(<<"id">>, Props),
                if is_binary(Id0) ->
                    Id = ejson:encode(Id0);
                Id0 == undefined ->
                    Id = undefined;
                true ->
                    Id = <<>>,
                    throw(bad_id)
                end,
                Value = ejson:encode(proplists:get_value(<<"value">>, Props)),
                case get(rows) of
                undefined ->
                    Rows = [];
                Rows ->
                    ok
                end,
                case proplists:get_value(<<"doc">>, Props) of
                undefined ->
                    case proplists:get_value(<<"partition">>, Props) of
                    undefined ->
                        if Id == undefined ->
                            case proplists:get_value(<<"error">>, Props) of
                            undefined ->
                                put(rows, [{Key, Value} | Rows]);
                            Error ->
                                put(rows, [{{Key, error}, ejson:encode(Error)} | Rows])
                            end;
                        true ->
                            put(rows, [{{Key, Id}, Value} | Rows])
                        end;
                    Partition when is_integer(Partition) ->
                        Node = proplists:get_value(<<"node">>, Props),
                        true = is_binary(Node),
                        put(rows, [{{Key, Id}, {ejson:encode(Partition), ejson:encode(Node), Value}} | Rows])
                    end;
                DocVal when is_tuple(DocVal) orelse is_atom(DocVal) ->
                    put(rows, [{{Key, Id}, Value, ejson:encode(DocVal)} | Rows]);
                _ ->
                    throw(bad_doc_val)
                end,
                fun(Ev2) -> parse_event_row_values(Ev2) end
            end)
    end.


    