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

-module(couch_db).
-behaviour(gen_server).

-export([open/2,open_int/2,close/1,create/2,get_db_info/1,get_design_docs/1]).
-export([start_compact/1, cancel_compact/1,get_design_docs/2]).
-export([open_ref_counted/2,is_idle/1,monitor/1,count_changes_since/2]).
-export([update_doc/2,update_doc/3]).
-export([update_docs/2,update_docs/3]).
-export([get_doc_info/2,open_doc/2,open_doc/3]).
-export([set_revs_limit/2,get_revs_limit/1]).
-export([get_missing_revs/2,name/1,get_update_seq/1,get_committed_update_seq/1]).
-export([enum_docs/4,enum_docs_since/5]).
-export([enum_docs_since_reduce_to_count/1,enum_docs_reduce_to_count/1]).
-export([increment_update_seq/1,get_purge_seq/1,purge_docs/2,get_last_purged/1]).
-export([start_link/3,open_doc_int/3,ensure_full_commit/1]).
-export([set_security/2,get_security/1]).
-export([init/1,terminate/2,handle_call/3,handle_cast/2,code_change/3,handle_info/2]).
-export([changes_since/4,changes_since/5]).
-export([check_is_admin/1, check_is_member/1]).
-export([reopen/1,get_current_seq/1,fast_reads/2]).

-include("couch_db.hrl").

-define(FD_CLOSE_TIMEOUT_MS, 10). % Fds will close after msecs of non-use

start_link(DbName, Filepath, Options) ->
    case open_db_file(Filepath, Options) of
    {ok, Fd} ->
        StartResult = gen_server:start_link(couch_db, {DbName, Filepath, Fd, Options}, []),
        unlink(Fd),
        StartResult;
    Else ->
        Else
    end.

open_db_file(Filepath, Options) ->
    case couch_file:open(Filepath,
            [{fd_close_after, ?FD_CLOSE_TIMEOUT_MS} | Options]) of
    {ok, Fd} ->
        {ok, Fd};
    {error, enoent} ->
        % couldn't find file. is there a compact version? This can happen if
        % crashed during the file switch.
        case couch_file:open(Filepath ++ ".compact") of
        {ok, Fd} ->
            ?LOG_INFO("Found ~s~s compaction file, using as primary storage.", [Filepath, ".compact"]),
            ok = file:rename(Filepath ++ ".compact", Filepath),
            ok = couch_file:sync(Fd),
            couch_file:close(Fd),
            open_db_file(Filepath, Options);
        {error, enoent} ->
            {not_found, no_db_file}
        end;
    Error ->
        Error
    end.


create(DbName, Options) ->
    couch_server:create(DbName, Options).

% this is for opening a database for internal purposes like the replicator
% or the view indexer. it never throws a reader error.
open_int(DbName, Options) ->
    couch_server:open(DbName, Options).

% this should be called anytime an http request opens the database.
% it ensures that the http userCtx is a valid reader
open(DbName, Options) ->
    case couch_server:open(DbName, Options) of
        {ok, Db} ->
            try
                check_is_member(Db),
                {ok, Db}
            catch
                throw:Error ->
                    close(Db),
                    throw(Error)
            end;
        Else -> Else
    end.

reopen(#db{main_pid = Pid, fd_ref_counter = OldRefCntr, user_ctx = UserCtx}) ->
    {ok, #db{fd_ref_counter = NewRefCntr} = NewDb} =
        gen_server:call(Pid, get_db, infinity),
    case NewRefCntr =:= OldRefCntr of
    true ->
        ok;
    false ->
        couch_ref_counter:add(NewRefCntr),
        catch couch_ref_counter:drop(OldRefCntr)
    end,
    {ok, NewDb#db{user_ctx = UserCtx}}.

get_current_seq(#db{main_pid = Pid}) ->
    gen_server:call(Pid, get_current_seq, infinity).

ensure_full_commit(#db{update_pid=UpdatePid,instance_start_time=StartTime}) ->
    ok = gen_server:call(UpdatePid, full_commit, infinity),
    {ok, StartTime}.

close(#db{fd_ref_counter=RefCntr}) ->
    couch_ref_counter:drop(RefCntr).

open_ref_counted(MainPid, OpenedPid) ->
    gen_server:call(MainPid, {open_ref_count, OpenedPid}, infinity).

is_idle(#db{main_pid = MainPid}) ->
    is_idle(MainPid);
is_idle(MainPid) ->
    gen_server:call(MainPid, is_idle, infinity).

monitor(#db{main_pid=MainPid}) ->
    erlang:monitor(process, MainPid).

start_compact(#db{update_pid=Pid}) ->
    gen_server:call(Pid, start_compact, infinity).

cancel_compact(#db{update_pid=Pid}) ->
    gen_server:call(Pid, cancel_compact, infinity).

open_doc(Db, IdOrDocInfo) ->
    open_doc(Db, IdOrDocInfo, []).

open_doc(Db, Id, Options) ->
    increment_stat(Db, {couchdb, database_reads}),
    case open_doc_int(Db, Id, Options) of
    {ok, #doc{deleted=true}=Doc} ->
        case lists:member(deleted, Options) of
        true ->
            apply_open_options({ok, Doc},Options);
        false ->
            {not_found, deleted}
        end;
    Else ->
        apply_open_options(Else,Options)
    end.

apply_open_options({ok, Doc},Options) ->
    apply_open_options2(Doc,Options);
apply_open_options(Else,_Options) ->
    Else.

apply_open_options2(Doc,[]) ->
    {ok, Doc};
apply_open_options2(Doc, [ejson_body | Rest]) ->
    apply_open_options2(couch_doc:with_ejson_body(Doc), Rest);
apply_open_options2(Doc, [json_bin_body | Rest]) ->
apply_open_options2(couch_doc:with_json_body(Doc), Rest);
apply_open_options2(Doc,[_|Rest]) ->
    apply_open_options2(Doc,Rest).


% Each returned result is a list of tuples:
% {Id, MissingRev}
% if the is on disk, it's omitted from the results.
get_missing_revs(Db, IdRevList) ->
    Results = get_doc_infos(Db, [Id || {Id, _Rev} <- IdRevList]),
    {ok, find_missing(IdRevList, Results)}.


find_missing([], []) ->
    [];
find_missing([{Id,{RevPos, RevId}}|RestIdRevs],
        [{ok, #doc_info{rev={DiskRevPos, DiskRevId}}} | RestLookupInfo]) ->
    case {RevPos, RevId} of
    {DiskRevPos, DiskRevId} ->
        find_missing(RestIdRevs, RestLookupInfo);
    _ ->
        [{Id, {DiskRevPos, DiskRevId}} |
                find_missing(RestIdRevs, RestLookupInfo)]
    end;
find_missing([{Id, Rev}|RestIdRevs], [not_found | RestLookupInfo]) ->
    [{Id, Rev} | find_missing(RestIdRevs, RestLookupInfo)].


%   returns {ok, DocInfo} or not_found
get_doc_info(Db, Id) ->
    [Result] = get_doc_infos(Db, [Id]),
    Result.

get_doc_infos(Db, Ids) ->
    couch_btree:lookup(Db#db.docinfo_by_id_btree, Ids).

increment_update_seq(#db{update_pid=UpdatePid}) ->
    gen_server:call(UpdatePid, increment_update_seq, infinity).

purge_docs(#db{update_pid=UpdatePid}, IdsRevs) ->
    gen_server:call(UpdatePid, {purge_docs, IdsRevs}, infinity).

get_committed_update_seq(#db{committed_update_seq=Seq}) ->
    Seq.

get_update_seq(#db{update_seq=Seq})->
    Seq.

get_purge_seq(#db{header=#db_header{purge_seq=PurgeSeq}})->
    PurgeSeq.

get_last_purged(#db{header=#db_header{purged_docs=nil}}) ->
    {ok, []};
get_last_purged(#db{fd=Fd, header=#db_header{purged_docs=PurgedPointer}}) ->
    couch_file:pread_term(Fd, PurgedPointer).

get_db_info(Db) ->
    #db{fd=Fd,
        header=#db_header{disk_version=DiskVersion},
        compactor_info=Compactor,
        update_seq=SeqNum,
        name=Name,
        instance_start_time=StartTime,
        committed_update_seq=CommittedUpdateSeq,
        docinfo_by_id_btree = IdBtree,
        docinfo_by_seq_btree = SeqBtree,
        local_docs_btree = LocalBtree
    } = Db,
    {ok, Size} = couch_file:bytes(Fd),
    {ok, DbReduction} = couch_btree:full_reduce(IdBtree),
    InfoList = [
        {db_name, Name},
        {doc_count, element(1, DbReduction)},
        {doc_del_count, element(2, DbReduction)},
        {update_seq, SeqNum},
        {purge_seq, couch_db:get_purge_seq(Db)},
        {compact_running, Compactor/=nil},
        {disk_size, Size},
        {data_size, db_data_size(DbReduction, [SeqBtree, IdBtree, LocalBtree])},
        {instance_start_time, StartTime},
        {disk_format_version, DiskVersion},
        {committed_update_seq, CommittedUpdateSeq}
        ],
    {ok, InfoList}.

db_data_size({_Count, _DelCount}, _Trees) ->
    % pre 1.2 format, upgraded on compaction
    null;
db_data_size({_Count, _DelCount, nil}, _Trees) ->
    null;
db_data_size({_Count, _DelCount, DocAndAttsSize}, Trees) ->
    sum_tree_sizes(DocAndAttsSize, Trees).

sum_tree_sizes(Acc, []) ->
    Acc;
sum_tree_sizes(Acc, [T | Rest]) ->
    case couch_btree:size(T) of
    nil ->
        null;
    Sz ->
        sum_tree_sizes(Acc + Sz, Rest)
    end.


get_design_docs(Db) ->
    get_design_docs(Db, no_deletes).

get_design_docs(Db, DeletedAlso) ->
    {ok,_, Docs} = couch_btree:fold(Db#db.docinfo_by_id_btree,
        fun(#doc_info{deleted = true}, _Reds, AccDocs)
                    when DeletedAlso == no_deletes ->
            {ok, AccDocs};
        (#doc_info{id= <<"_design/",_/binary>>}=DocInfo, _Reds, AccDocs) ->
            {ok, Doc} = open_doc_int(Db, DocInfo, [ejson_body]),
            {ok, [Doc | AccDocs]};
        (_, _Reds, AccDocs) ->
            {stop, AccDocs}
        end,
        [], [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}]),
    {ok, Docs}.

check_is_admin(#db{user_ctx=#user_ctx{name=Name,roles=Roles}}=Db) ->
    {Admins} = get_admins(Db),
    AdminRoles = [<<"_admin">> | couch_util:get_value(<<"roles">>, Admins, [])],
    AdminNames = couch_util:get_value(<<"names">>, Admins,[]),
    case AdminRoles -- Roles of
    AdminRoles -> % same list, not an admin role
        case AdminNames -- [Name] of
        AdminNames -> % same names, not an admin
            throw({unauthorized, <<"You are not a db or server admin.">>});
        _ ->
            ok
        end;
    _ ->
        ok
    end.

check_is_member(#db{user_ctx=#user_ctx{name=Name,roles=Roles}=UserCtx}=Db) ->
    case (catch check_is_admin(Db)) of
    ok -> ok;
    _ ->
        {Members} = get_members(Db),
        ReaderRoles = couch_util:get_value(<<"roles">>, Members,[]),
        WithAdminRoles = [<<"_admin">> | ReaderRoles],
        ReaderNames = couch_util:get_value(<<"names">>, Members,[]),
        case ReaderRoles ++ ReaderNames of
        [] -> ok; % no readers == public access
        _Else ->
            case WithAdminRoles -- Roles of
            WithAdminRoles -> % same list, not an reader role
                case ReaderNames -- [Name] of
                ReaderNames -> % same names, not a reader
                    ?LOG_DEBUG("Not a reader: UserCtx ~p vs Names ~p Roles ~p",[UserCtx, ReaderNames, WithAdminRoles]),
                    throw({unauthorized, <<"You are not authorized to access this db.">>});
                _ ->
                    ok
                end;
            _ ->
                ok
            end
        end
    end.

get_admins(#db{security=SecProps}) ->
    couch_util:get_value(<<"admins">>, SecProps, {[]}).

get_members(#db{security=SecProps}) ->
    % we fallback to readers here for backwards compatibility
    couch_util:get_value(<<"members">>, SecProps,
        couch_util:get_value(<<"readers">>, SecProps, {[]})).

get_security(#db{security=SecProps}) ->
    {SecProps}.

set_security(#db{update_pid=Pid}=Db, {NewSecProps}) when is_list(NewSecProps) ->
    check_is_admin(Db),
    ok = validate_security_object(NewSecProps),
    ok = gen_server:call(Pid, {set_security, NewSecProps}, infinity),
    {ok, _} = ensure_full_commit(Db),
    ok;
set_security(_, _) ->
    throw(bad_request).

validate_security_object(SecProps) ->
    Admins = couch_util:get_value(<<"admins">>, SecProps, {[]}),
    % we fallback to readers here for backwards compatibility
    Members = couch_util:get_value(<<"members">>, SecProps,
        couch_util:get_value(<<"readers">>, SecProps, {[]})),
    ok = validate_names_and_roles(Admins),
    ok = validate_names_and_roles(Members),
    ok.

% validate user input
validate_names_and_roles({Props}) when is_list(Props) ->
    case couch_util:get_value(<<"names">>,Props,[]) of
    Ns when is_list(Ns) ->
            [throw("names must be a JSON list of strings") ||N <- Ns, not is_binary(N)],
            Ns;
    _ -> throw("names must be a JSON list of strings")
    end,
    case couch_util:get_value(<<"roles">>,Props,[]) of
    Rs when is_list(Rs) ->
        [throw("roles must be a JSON list of strings") ||R <- Rs, not is_binary(R)],
        Rs;
    _ -> throw("roles must be a JSON list of strings")
    end,
    ok.

get_revs_limit(#db{revs_limit=Limit}) ->
    Limit.

set_revs_limit(#db{update_pid=Pid}=Db, Limit) when Limit > 0 ->
    check_is_admin(Db),
    gen_server:call(Pid, {set_revs_limit, Limit}, infinity);
set_revs_limit(_Db, _Limit) ->
    throw(invalid_revs_limit).

name(#db{name=Name}) ->
    Name.

update_doc(Db, Docs) ->
    update_doc(Db, Docs, []).

update_doc(Db, Doc, Options) ->
    update_docs(Db, [Doc], Options).

update_docs(Db, Docs) ->
    update_docs(Db, Docs, []).

% This open a new raw file in current process for the duration of the Fun
% execution. For single reads, it's likely slower than regular reads due
% to the overhead of opening a new FD. But for lots of reads like,
% docs_since or enum_docs, it's often faster as it avoids the messaging
% overhead with couch_file.
fast_reads(#db{main_pid=Pid}=Db, Fun) ->
    ok = gen_server:call(Pid, {raw_read_open_ok, self()}, infinity),
    {ok, FastReadFd} = file:open(Db#db.filepath, [binary, read, raw]),
    ok = gen_server:call(Pid, {raw_read_open_done, self()}, infinity),
    put({Db#db.fd, fast_fd_read}, FastReadFd),
    try
        Fun()
    after
        file:close(FastReadFd),
        erase({Db#db.fd, fast_fd_read})
    end.




update_docs(#db{name=DbName}=Db, Docs, Options0) ->
    increment_stat(Db, {couchdb, database_writes}),
    % go ahead and generate the new revision ids for the documents.
    % separate out the NonRep documents from the rest of the documents
    {Docs1, NonRepDocs1} = lists:foldl(
        fun(#doc{id=Id}=Doc, {DocsAcc, NonRepDocsAcc}) ->
            case Id of
            <<?LOCAL_DOC_PREFIX, _/binary>> ->
                {DocsAcc, [Doc | NonRepDocsAcc]};
            Id->
                {[Doc | DocsAcc], NonRepDocsAcc}
            end
        end, {[], []}, Docs),
    case lists:member(sort_docs, Options0) of
    true ->
        Docs2 = lists:keysort(#doc.id, Docs1),
        NonRepDocs = lists:keysort(#doc.id, NonRepDocs1);
    false ->
        Docs2 = Docs1,
        NonRepDocs = NonRepDocs1
    end,
    Options = set_commit_option(Options0),
    FullCommit = lists:member(full_commit, Options),
    Docs3 = write_doc_bodies_retry_closed(Db, Docs2),
    MRef = erlang:monitor(process, Db#db.update_pid),
    try
        Db#db.update_pid ! {update_docs, self(), Docs3, NonRepDocs,
                FullCommit},
        case get_result(Db#db.update_pid, MRef) of
        ok ->
            [couch_db_update_notifier:notify({ddoc_updated, {DbName, Id}})
                    || #doc{id = <<?DESIGN_DOC_PREFIX, _/binary>> = Id} <- Docs],
            ok;
        retry ->
            % This can happen if the db file we wrote to was swapped out by
            % compaction. Retry by reopening the db and writing to the current file
            {ok, Db2} = open_ref_counted(Db#db.main_pid, self()),
            % We only retry once
            Docs4 = write_doc_bodies(Db2, Docs2),
            close(Db2),
            Db#db.update_pid ! {update_docs, self(), Docs4, NonRepDocs,
                    FullCommit},
            case get_result(Db#db.update_pid, MRef) of
            ok ->
                [couch_db_update_notifier:notify({ddoc_updated, {DbName, Id}})
                        || #doc{id = <<?DESIGN_DOC_PREFIX, _/binary>> = Id} <- Docs],
                ok;
            retry -> throw({update_error, compaction_retry})
            end
        end
    after
        erlang:demonitor(MRef, [flush])
    end.

set_commit_option(Options) ->
    CommitSettings = {
        [true || O <- Options, O==full_commit orelse O==delay_commit],
        couch_config:get("couchdb", "delayed_commits", "false")
    },
    case CommitSettings of
    {[true], _} ->
        Options; % user requested explicit commit setting, do not change it
    {_, "true"} ->
        Options; % delayed commits are enabled, do nothing
    {_, "false"} ->
        [full_commit|Options];
    {_, Else} ->
        ?LOG_ERROR("[couchdb] delayed_commits setting must be true/false, not ~p",
            [Else]),
        [full_commit|Options]
    end.

get_result(UpdatePid, MRef) ->
    receive
    {done, UpdatePid} ->
        ok;
    {retry, UpdatePid} ->
        retry;
    {'DOWN', MRef, _, _, Reason} ->
        exit(Reason)
    end.


write_doc_bodies_retry_closed(Db, Docs) ->
    try
        write_doc_bodies(Db, Docs)
    catch
        throw:{update_error, compaction_retry} ->
            {ok, Db2} = open_ref_counted(Db#db.main_pid, self()),
            % We only retry once
            Docs2 = write_doc_bodies(Db2, Docs),
            close(Db2),
            Docs2
    end.


write_doc_bodies(Db, Docs) ->
    lists:map(
        fun(#doc{json = Body, binary = Binary} = Doc) ->
            Prepped = prep_doc_body_binary(Body, Binary),
            case couch_file:append_binary_crc32(Db#db.fd, Prepped) of
            {ok, BodyPtr, Size} ->
                #doc_update_info{
                    id=Doc#doc.id,
                    rev=Doc#doc.rev,
                    deleted=Doc#doc.deleted,
                    body_ptr=BodyPtr,
                    fd=Db#db.fd,
                    size=Size
                };
            {error, write_closed} ->
                throw({update_error, compaction_retry})
            end
        end,
        Docs).

prep_doc_body_binary(Body, Binary) ->
    BodyCompressed = couch_compress:compress(Body, snappy),
    Size = iolist_size(BodyCompressed),
    case Binary of
    nil ->
        % we differentiate between a empty binary and no binary. Clients
        % might set a empty binary as the primary value.
        [<<0:1/integer, Size:31/integer>>, BodyCompressed];
    _ ->
        [<<1:1/integer, Size:31/integer>>, BodyCompressed, Binary]
    end.

read_doc_body_binary(Iolist) ->
    {Len, Rest} = couch_util:split_iolist(Iolist, 4),
    case iolist_to_binary(Len) of
    <<0:1/integer, _Size:31/integer>> ->
        {couch_compress:decompress(iolist_to_binary(Rest)), nil};
    <<1:1/integer, Size:31/integer>> ->
        {Json, Binary} = couch_util:split_iolist(Rest, Size),
        {couch_compress:decompress(iolist_to_binary(Json)), Binary}
    end.

enum_docs_since_reduce_to_count(Reds) ->
    couch_btree:final_reduce(
            fun couch_db_updater:btree_by_seq_reduce/2, Reds).

enum_docs_reduce_to_count(Reds) ->
    FinalRed = couch_btree:final_reduce(
            fun couch_db_updater:btree_by_id_reduce/2, Reds),
    element(1, FinalRed).

changes_since(Db, StartSeq, Fun, Acc) ->
    changes_since(Db, StartSeq, Fun, [], Acc).

changes_since(Db, StartSeq, Fun, Options, Acc) ->
    Wrapper = fun(DocInfo, _Offset, Acc2) -> Fun(DocInfo, Acc2) end,
    {ok, _Reduce, AccOut} = couch_btree:fold(Db#db.docinfo_by_seq_btree,
            Wrapper, Acc, [{start_key, StartSeq + 1}] ++ Options),
    {ok, AccOut}.

count_changes_since(Db, SinceSeq) ->
    BTree = Db#db.docinfo_by_seq_btree,
    {ok, Changes} =
    couch_btree:fold_reduce(BTree,
        fun(_SeqStart, PartialReds, 0) ->
            {ok, couch_btree:final_reduce(BTree, PartialReds)}
        end,
        0, [{start_key, SinceSeq + 1}]),
    Changes.

enum_docs_since(Db, SinceSeq, InFun, Acc, Options) ->
    {ok, LastReduction, AccOut} = couch_btree:fold(
        Db#db.docinfo_by_seq_btree, InFun, Acc,
        [{start_key, SinceSeq + 1} | Options]),
    {ok, enum_docs_since_reduce_to_count(LastReduction), AccOut}.

enum_docs(Db, InFun, InAcc, Options) ->
    {ok, LastReduce, OutAcc} = couch_btree:fold(
        Db#db.docinfo_by_id_btree, InFun, InAcc, Options),
    {ok, enum_docs_reduce_to_count(LastReduce), OutAcc}.

% server functions

init({DbName, Filepath, Fd, Options}) ->
    {ok, UpdaterPid} = gen_server:start_link(couch_db_updater, {self(), DbName, Filepath, Fd, Options}, []),
    {ok, #db{fd_ref_counter=RefCntr}=Db} = gen_server:call(UpdaterPid, get_db, infinity),
    couch_ref_counter:add(RefCntr),
    case lists:member(sys_db, Options) of
    true ->
        ok;
    false ->
        couch_stats_collector:track_process_count({couchdb, open_databases})
    end,
    process_flag(trap_exit, true),
    {ok, Db}.

terminate(_Reason, Db) ->
    couch_util:shutdown_sync(Db#db.update_pid),
    ok.

handle_call({open_ref_count, OpenerPid}, _, #db{fd_ref_counter=RefCntr}=Db) ->
    ok = couch_ref_counter:add(RefCntr, OpenerPid),
    {reply, {ok, Db}, Db};
handle_call(is_idle, _From, #db{fd_ref_counter=RefCntr, compactor_info=Compact,
            waiting_delayed_commit=Delay}=Db) ->
    % Idle means no referrers. Unless in the middle of a compaction file switch,
    % there are always at least 2 referrers, couch_db_updater and us.
    {reply, (Delay == nil) andalso (Compact == nil) andalso (couch_ref_counter:count(RefCntr) == 2), Db};
handle_call({db_updated, NewDb}, _From, #db{fd_ref_counter=OldRefCntr,
        raw_reader_openers=Openers}) ->
    #db{fd_ref_counter=NewRefCntr}=NewDb,
    case NewRefCntr =:= OldRefCntr of
    true -> ok;
    false ->
        couch_ref_counter:add(NewRefCntr),
        couch_ref_counter:drop(OldRefCntr)
    end,
    {reply, ok, NewDb#db{raw_reader_openers=Openers}};
handle_call(get_db, _From, Db) ->
    {reply, {ok, Db}, Db};
handle_call({raw_read_open_ok, Pid}, From, #db{compactor_info=Compactor,
        raw_reader_openers=Openers}=Db) ->
    MonRef = erlang:monitor(process, Pid),
    case Compactor of
    compaction_file_switching ->
        % don't message to caller, it must wait until the
        % compaction_file_switch is done
        {noreply, Db#db{raw_reader_openers=[{Pid, MonRef, From}|Openers]}};
    compaction_file_switch_waiting ->
        % don't message to caller, it must wait until the
        % compaction_file_switch is done (it takes precedence here)
        {noreply, Db#db{raw_reader_openers=[{Pid, MonRef, From}|Openers]}};
    _Else ->
        {reply, ok, Db#db{raw_reader_openers=[{Pid, MonRef, ok}|Openers]}}
    end;
handle_call({raw_read_open_done, Pid}, _From, #db{compactor_info=Compactor,
        raw_reader_openers=Openers}=Db) ->
    {value, {Pid, MonRef, ok}, Openers2} = lists:keytake(Pid, 1, Openers),
    erlang:demonitor(MonRef, [flush]),
    case Compactor of
    compaction_file_switch_waiting ->
        case lists:keysearch(ok, 3, Openers2) of
        false ->
            % no one is opening a raw file. signal back to updater that it's
            % ok to continue to the file switch.
            Compactor2 = compaction_file_switching,
            Db#db.update_pid ! continue_compaction_file_switch;
        _ ->
            % We still have concurrent openers of a raw file, we can't let
            % the compaction file switch continue yet.
            Compactor2 = compaction_file_switch_waiting
        end;
    _ ->
        Compactor2 = Compactor
    end,
    {reply, ok, Db#db{raw_reader_openers=Openers2, compactor_info=Compactor2}};
handle_call(get_current_seq, _From, #db{update_seq = Seq} = Db) ->
    {reply, {ok, Seq}, Db}.


handle_cast(Msg, Db) ->
    ?LOG_ERROR("Bad cast message received for db ~s: ~p", [Db#db.name, Msg]),
    exit({error, Msg}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_info(compaction_file_switch, #db{update_pid = Updater,
        raw_reader_openers=Openers}=Db) ->
    case Openers of
    [] ->
        Updater ! continue_compaction_file_switch,
        {noreply, Db#db{compactor_info=compaction_file_switching}};
    _ ->
        {noreply, Db#db{compactor_info=compaction_file_switch_waiting}}
    end;
handle_info(compaction_file_switch_done, #db{
        raw_reader_openers=Openers}=Db) ->
    Openers2 = lists:map(
            fun({Pid, MonRef, From}) ->
                % signal to all raw file openers they can continue and
                % put them into the opening state.
                % Note, we can't have any active openers at this state,
                % only openers waiting for the ok
                gen_server:reply(From, ok),
                {Pid, MonRef, ok}
            end, Openers),
    % leave the compactor_info as is, will be overwritten soon with
    % db_updated message.
    {noreply, Db#db{raw_reader_openers=Openers2}};
handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, #db{
        raw_reader_openers=Openers}=Db) ->
    case lists:keytake(Pid, 1, Openers) of
    {value, {Pid, _MonRef, _}, Openers2} ->
        ?LOG_INFO("Raw file opener crashed before open completed. ~p", [Db]),
        {noreply, Db#db{raw_reader_openers=Openers2}};
    false ->
        exit({exit, "Error, bad DOWN message!"})
    end;
handle_info({'EXIT', _Pid, normal}, Db) ->
    {noreply, Db};
handle_info({'EXIT', _Pid, Reason}, Server) ->
    {stop, Reason, Server};
handle_info(Msg, Db) ->
    ?LOG_ERROR("Bad message received for db ~s: ~p", [Db#db.name, Msg]),
    exit({error, Msg}).


%%% Internal function %%%

open_doc_int(Db, <<?LOCAL_DOC_PREFIX, _/binary>> = Id, Options) ->
    case couch_btree:lookup(Db#db.local_docs_btree, [Id]) of
    [{ok, {_, BodyData}}] ->
        Doc = #doc{id=Id, json=BodyData},
        apply_open_options({ok, Doc}, Options);
    [not_found] ->
        {not_found, missing}
    end;
open_doc_int(Db, #doc_info{id=Id,deleted=IsDeleted,rev=RevInfo, body_ptr=Bp}=
        DocInfo, Options) ->
    Doc = make_doc(Db, Id, IsDeleted, Bp, RevInfo),
    apply_open_options(
       {ok, Doc#doc{meta=doc_meta_info(DocInfo, Options)}}, Options);
open_doc_int(Db, Id, Options) ->
    case get_doc_info(Db, Id) of
    {ok, DocInfo} ->
        open_doc_int(Db, DocInfo, Options);
    not_found ->
        {not_found, missing}
    end.

doc_meta_info(#doc_info{local_seq=Seq}, Options) ->
    case lists:member(local_seq, Options) of
    false -> [];
    true -> [{local_seq, Seq}]
    end.


make_doc(#db{fd = Fd}, Id, Deleted, Bp, Rev) ->
    {ok, Body} = couch_file:pread_iolist(Fd, Bp),
    {Json, Binary} = read_doc_body_binary(Body),
    #doc{
        id = Id,
        rev = Rev,
        json = Json,
        binary = Binary,
        deleted = Deleted
        }.


increment_stat(#db{options = Options}, Stat) ->
    case lists:member(sys_db, Options) of
    true ->
        ok;
    false ->
        couch_stats_collector:increment(Stat)
    end.
