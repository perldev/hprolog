-module(fact_hbase_thrift).



-export([test/0,
         start_process_loop_hbase/3,
         low_get_key/4,
         get_regions/1,
         get_key_custom/4,
         get_key/4, 
         put_key/5, 
         get_data/8,
         test_get_regions/0,
         generate_scanner/5,
         process_data/6, 
         flash_stat/2,
         thrift_mappper_low/3,
         create_filter4all_values/3
         ]).

-include_lib("deps/thrift/src/hbase_types.hrl").
-include("prolog.hrl").
-record(mapper_state,
        {pids, ets_buffer, ets_stat, out_speed}
    ).

%     ,[{tRowResult,<<"0faf51ea7ba2292dd69f6ec66b1e4ba9">>,
%                   {dict,14,16,16,8,80,48,
%                         {[],[],[],[],[],[],[],[],[],[],[],[],[],...},
%                         {{[],
%                           [[<<"params:5">>|{tCell,<<"1942612008">>,1373889743539}]],
%                           [[<<"params:12">>|{tCell,<<"805012">>,1373889743539}]],
%                           [],
%                           [[<<"params:4">>|{tCell,<<"Jul "...>>,1373889743539}]],
%                           [[<<"params:11">>|{tCell,<<...>>,...}],
%                            [<<"params:9">>|{tCell,...}]],
%                           [],
%                           [[<<"para"...>>|{...}]],
%                           [[<<...>>|...],[...]],
%                           [],


%%%%GREAT SHIT AVALIBLE with big rows
process_one_record(Data)->
    [Record] = Data,
    Dict = Record#tRowResult.columns,
%     [{tCell,
%                                       <<"000028c5bd009fb38237eef14d971e14">>,
%                                       1374493085988}]
    lists:map( fun({_Key, { tCell ,Val,_ } }) ->  unicode:characters_to_list(Val)  end, dict:to_list(Dict) )
.

process_one_record(ProtoType, Family, Data)->
    [Record] = Data,
    Dict = Record#tRowResult.columns,
    Columns = lists:seq(1, length(ProtoType) ),
    ?DEBUG("~p process data after reduce ~p ~n",[{?MODULE,?LINE}, Record ] ),
    Got = lists:map( fun(Elem)->                
                         case dict:find(list_to_binary( Family ++ ":" ++ integer_to_list(Elem) )  , Dict) of
                            error ->  "";
                            { ok, {tCell, Value, _Time } } -> unicode:characters_to_list(Value)
                         end                               
                     end, Columns),
    [ list_to_tuple(Got) ]
.

% connect(default, Opts) ->
%     connect("localhost", 9090, Opts, hbase_thrift).

% connect(Host, Port, Opts, Service)
% {tRowResult,<<"000a35692fe390c84214c683723d7816">>,
%                    [{<<"params:1">>,{tCell,<<"P2490311069">>,1374507319687}},
%                     {<<"params:10">>,{tCell,<<>>,1374507319687}},
%                     {<<"params:11">>,{tCell,<<"305299">>,1374507319687}},
%                     {<<"params:12">>,{tCell,<<"320649">>,1374507319687}},
%                     {<<"params:13">>,{tCell,<<"Ð¡Ð¿Ð»Ð°Ñ"...>>,1374507319687}},
%                     {<<"params:14">>,{tCell,<<"95.69.248.254">>,1374507319687}},
%                     {<<"params:2">>,{tCell,<<"16375.40">>,1374507319687}},
%                     {<<"params:3">>,{tCell,<<"UAH">>,1374507319687}},
%                     {<<"params:4">>,
%                      {tCell,<<"Apr 01 2013 14:1"...>>,1374507319687}},
%                     {<<"params:5">>,{tCell,<<"24269712">>,1374507319687}},
%                     {<<"params:6">>,{tCell,<<"35649894">>,1374507319687}},
%                     {<<"params:7">>,{tCell,<<"2600"...>>,1374507319687}},
%                     {<<"params:8">>,{tCell,<<...>>,...}},
%                     {<<"params:9">>,{tCell,...}}]},
%%TODO WATCH by all mappers

    
put_key(Table, Key, Family, Key2, Value)->
    {KeyC, Connection} = thrift_connection_pool:get_free(),
    Time = now(),
     case catch 
        thrift_client:call(Connection, mutateRow, [Table , Key,
            [#mutation{isDelete=false,column=Family ++ ":" ++ Key2, value = Value} ], dict:new() ] )
        of

        { NewConnection, {ok,ok} } ->
                ?DEBUG("~p put data  in ~p  ~n", [{?MODULE,?LINE}, timer:now_diff(now(), Time ) ]),
                thrift_connection_pool:return({KeyC, NewConnection}),
                true
            ;
        Error -> 
             %%%reconnection !!!??
             thrift_connection_pool:reconnect(KeyC),
             ?DEBUG("puting key ~p error result: ~p ~n", [{Table, Key, Family, Key2, Value},Error]),
             throw( {hbase_exception, Error} )         
     end
  
.

low_get_key(Table, Key, Family,  SecondKey)->
    
        {KeyC, Connection} = thrift_connection_pool:get_free(),
%      thrift_client:call(State, getRowWithColumns, 
%         ["pay","0faf51ea7ba2292dd69f6ec66b1e4ba9",["params"],dict:new()]).
        Time = now(),
        case catch thrift_client:call(Connection, get, [ Table, Key, Family ++ ":" ++ SecondKey, dict:new() ] ) of
        { NewState, {ok, []} } -> 
                    thrift_connection_pool:return({Key, NewState}),
                    [];
        { NewState, {ok, Data} } ->
                ?DEBUG("~p fetch : data ~p in ~p  ~n", [{?MODULE,?LINE}, Data,  timer:now_diff(now(), Time ) ]),
                thrift_connection_pool:return({KeyC, NewState}),
%                 {ok,[{tCell,<<"P2497386422">>,1373889743539}]}}
                [{tCell,Val ,_TimeStamp}] = Data,
                unicode:characters_to_list(Val)
            ;
        Error -> 
             %%%reconnection !!!??
             thrift_connection_pool:reconnect(KeyC),
             ?DEBUG("get key ~p error result: ~p ~n", [{ Table, Family, Key}, Error]),
             {hbase_exception, Error}          
        end

.

get_key_custom( Table, Family, Key, default)->
    get_key_custom( Table, Family, Key, fun process_one_record/1 )
;
get_key_custom( Table, Family, Key, FunProcess)->
        {KeyC, Connection} = thrift_connection_pool:get_free(),
%      thrift_client:call(State, getRowWithColumns, 
%         ["pay","0faf51ea7ba2292dd69f6ec66b1e4ba9",["params"],dict:new()]).
        Time = now(),
        ?DEBUG("~p  starting fetch  ~p ~n", [{?MODULE,?LINE}, Connection]),

        case catch thrift_client:call(Connection, getRowWithColumns, [ Table, Key, [Family], dict:new() ] ) of
            { NewState, {ok, []}  } -> 
                        thrift_connection_pool:return({KeyC, NewState}),
                        [];
            { NewState, {ok, Data} } ->
                    ?DEBUG("~p fetch : data ~p in ~p  ~n", [{?MODULE,?LINE}, Data,  timer:now_diff(now(), Time ) ]),
                    thrift_connection_pool:return({KeyC, NewState}),
                    FunProcess(Data)          
                ;
            Error -> 
                %%%reconnection !!!??
                thrift_connection_pool:reconnect(KeyC),
                ?DEBUG("get key ~p error result: ~p ~n", [{ Table, Family, Key}, Error]),
                {hbase_exception, Error}          
        end.

    
get_key(ProtoType, Table, Family, Key)->
    {KeyC, Connection} = thrift_connection_pool:get_free(),
%      thrift_client:call(State, getRowWithColumns, 
%         ["pay","0faf51ea7ba2292dd69f6ec66b1e4ba9",["params"],dict:new()]).
    Time = now(),
        case catch thrift_client:call(Connection, getRowWithColumns, [ Table, Key, [Family], dict:new() ] ) of
        { NewState, [] } -> 
                    thrift_connection_pool:return({KeyC, NewState}),
                    [];
        { NewState, {ok, Data} } ->
                ?DEBUG("~p fetch : data ~p in ~p  ~n", [{?MODULE,?LINE}, Data,  timer:now_diff(now(), Time ) ]),
                thrift_connection_pool:return({KeyC, NewState}),
                process_one_record(ProtoType,Family, Data)          
            ;
        Error -> 
             %%%reconnection !!!??
             thrift_connection_pool:reconnect(KeyC),
             ?DEBUG("get key ~p error result: ~p ~n", [{ Table, Family, Key}, Error]),
             {hbase_exception, Error}          
        end.



finish_loop_hbase(State)->
    Pids = State#mapper_state.pids,
    lists:foreach(fun({Pid, ConnState, ScannerId } )-> 
                    exit( Pid, finish),
                    delete_scanner({ConnState, ScannerId}) 
                  end, Pids),
    ets:delete(State#mapper_state.ets_buffer),
%             ets:insert(EtsStat, {disk_storage, Reference }),    
%         ets:insert(EtsStat, {disk_name, DiskName }), 
    case ets:lookup(State#mapper_state.ets_stat, disk_storage) of
            [{_, Value}]->
                io:format("close buffer ~p~n",[Value]),
                dets:close(Value),
               [{_, File}] = ets:lookup(State#mapper_state.ets_stat,disk_name ),
               file:delete(File);
               
            []->
                io:format("no disk buffer found ~n",[])
    end,
    ets:delete(State#mapper_state.ets_stat)
.

read_data_from_disk(EtsBuffer, FileName)->

    {ok, Reference} = dets:open_file(FileName),
     ListInsert = 
                dets:foldl(
                    fun(E, Acum)->  
                        [E|Acum]
                    end, [] , Reference ),
                    
    ets:insert(EtsBuffer, ListInsert),
    dets:close(Reference),
    file:delete(FileName),
    exit(normal)
.

check_buffer_sync(EtsBuffer, EtsStat)->       
        case ets:lookup(EtsStat, disk_storage  ) of
                        []-> empty;
                        [ Head| _Tail] ->
                            ets:delete_object(disk_storage, Head ),
                            {disk_storage, FileName} = Head,
                            read_data_from_disk( EtsBuffer, FileName),
                            ok
        end
.   
%%TODO more intellegent method of reading

check_buffer(EtsBuffer, EtsStat, Mappers)->       
       Count = ets:info(EtsBuffer, size),
       case Count < ?MAX_ETS_BUFFER_SIZE/2 of
            true ->
                   case ets:lookup(EtsStat, disk_storage  ) of
                        []-> empty;
                        [ Head| _Tail] ->
                            ets:delete_object(disk_storage, Head ),
                            {disk_storage, FileName} = Head,
                            spawn(?MODULE, read_data_from_disk, [ EtsBuffer, FileName ])
                            
                   end;                 
            false ->
                do_nothing
       end,
       PrevIterSize = ets:lookup(EtsStat, prev_iter_size),
       ?DEBUG("~p prev size of buffer ~p ~n",[ {?MODULE,?LINE}, { PrevIterSize, Count } ]),
       case PrevIterSize of
            []->
                ets:insert(EtsStat, {prev_iter_size,Count });
            [{_, Size}]->
                    case Count < Size   of
                        true ->
                                 ets:delete(EtsStat, prev_iter_size),
                                 ets:insert(EtsStat, { prev_iter_size, Count } ),
                                 io:format("~p continue read from hbase ~n",[ {?MODULE,?LINE} ]),
                                 lists:foreach(fun({_, Pid})->  
                                                    Pid ! continue_ping   %%%TODO WARNING process could be dead                                                 
                                                end, Mappers  );                       
                        false ->
                           do_nothing
                            %%do not send continue_ping signal to ther mappers
                    end
       end  
.       
       
to_unicode_list( Val )->
        list_to_tuple( lists:map( fun to_unicode/1, Val ) )
.
to_unicode(E)  when is_binary(E)->
    unicode:characters_to_list(E);
to_unicode(E)->
    E.
    
    
       
get_buffer_value(State)->
    EtsBuffer  = State#mapper_state.ets_buffer,
    Key = ets:first(EtsBuffer),
    %%%very very bad
    case Key of 
        '$end_of_table'-> 
                case check_buffer_sync(EtsBuffer, State#mapper_state.ets_stat) of
                        empty ->
                            [];
                        _ ->
                            get_buffer_value(State)
                end;
        _ ->
                [ {Key, Val} ] = ets:lookup(EtsBuffer, Key),
                ets:delete(EtsBuffer, Key),
                [ to_unicode_list(Val) ]
     end
    
.    

delete_scanner({ State, Scanner })->
    thrift_client:call(State, scannerClose, [Scanner]).

    
process_loop_hbase( start,    ProtoType, State = #mapper_state{ pids = [] } )->
    io:format("~p nothing find ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
    
    receive 
            {PidReciverResult ,get_pack_of_facts} ->
                   PidReciverResult ! [];
            {'DOWN', _MonitorRef, _Type, Object, Info}->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got monitor exit signal   ~p ~n",[{?MODULE,?LINE}, {Object, Info} ]);

             Some ->%%may be finish
                  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
     end,
     finish_loop_hbase(State),
     exit(normal)

;
process_loop_hbase(start,   ProtoType,  State )->
        ?WAIT("~p GOT  wait thrift  hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
        receive 
            %%as we got first result we can continue the work
            {result, ack} ->
                  process_loop_hbase(normal,  ProtoType, State);
            %%if mappper have sent finish it will close the connection by it self
            finish ->
                  finish_loop_hbase(State),
                  %%in order to get died all linked processed
                  exit(finished);
            {finish, Pid} ->
%             keydelete(Key, N, TupleList1) -> TupleList2
                  Pids =  State#mapper_state.pids, 
                  NewPids = lists:keydelete(Pid, 1,  Pids),
                  process_loop_hbase( start,   ProtoType, State#mapper_state{ pids = NewPids} );
             {'EXIT', From, normal} -> %%all except of normal
                 process_loop_hbase(start,   ProtoType,  State );
                  
            {'EXIT', From, Reason} -> %%all except of normal
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got exit signal  ~p ~n",[{?MODULE,?LINE}, {From, Reason} ]),
                  finish_loop_hbase(State),
                  process_loop_hbase( {hbase_exception, Reason},  ProtoType, State);
            % if user finish process      
            {'DOWN', _MonitorRef, _Type, Object, Info}->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got monitor exit signal   ~p ~n",[{?MODULE,?LINE}, {Object, Info} ]),
                  finish_loop_hbase(State),
                  %%in order to get died all linked processed
                  exit(finished)                  
        end
        
;
process_loop_hbase( {hbase_exception, Reason},  ProtoType, _State)->
     ?WAIT("~p GOT  wait thrift  hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),

    receive 
            {PidReciverResult ,get_pack_of_facts} ->
                    PidReciverResult ! {hbase_exception, Reason};
            {'DOWN', _MonitorRef, _Type, Object, Info}->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got monitor exit signal   ~p ~n",[{?MODULE,?LINE}, {Object, Info} ]);
             Some ->%%may be finish
                  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
     end,    
     exit(normal)
;
%%TODO adding supporting of adding data to the EtsBuffer from disk
%%TODO share EtsBuffer with the prolog leap
process_loop_hbase(normal ,  ProtoType, State)->
 ?WAIT("~p GOT  wait thrift  hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
  receive 
            {PidReciverResult ,get_pack_of_facts} ->                  
                   PidReciverResult !  get_buffer_value(State),
                   process_loop_hbase( normal,    ProtoType, State);
            {'EXIT', From, normal} -> %%all except of normal
                 process_loop_hbase(start,   ProtoType,  State );
            {'EXIT', From, Reason} ->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got exit signal  ~p ~n",[{?MODULE,?LINE}, {From, Reason} ]),
                  finish_loop_hbase( State),
                  process_loop_hbase( {hbase_exception, Reason},  ProtoType, State);                  
            {'DOWN', _MonitorRef, _Type, Object, Info}->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got monitor exit signal   ~p ~n",[{?MODULE,?LINE}, {Object, Info} ]),
                  finish_loop_hbase( State),
                  exit(finished);
            finish ->
                  finish_loop_hbase(State),
                  %%in order to get died all linked processed
                  exit(finished);
            {result, ack}->
                  %TODO try to avoid this operation
                  process_loop_hbase( normal,  ProtoType, State);
            {finish, Pid}->
                  Pids =  State#mapper_state.pids, 
                  
                  NewPids = lists:keydelete(Pid, 1,  Pids),
%                   ets:insert(EtsStat, {worker, Pid} ),
                  
                  process_loop_hbase( normal ,  ProtoType, State#mapper_state{pids = NewPids });
            Some ->%%may be finish
                  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ]),
                  finish_loop_hbase(State),
                  process_loop_hbase( {hbase_exception, Some},  ProtoType, State)
    end
.
test_get_regions()->

    inets:start(),
    ListOfRegions = get_regions("pay"),
    io:format("we got regions for pay ~p~n", [ListOfRegions])

.
get_rand_host()->
    {"http://hd-test-2.ceb.loc:60050/", "hd-test-2.ceb.loc"}
.

% [{<<"endKey">>,
%                             <<"MGZhZjUxZWE3YmEyMjkyZGQ2OWY2ZWM2NmIxZTRiYTk=">>},
%                            {<<"id">>,1374668512654},
%                            {<<"location">>,<<"es-6.ceb.loc:60020">>},
%                            {<<"name">>,
%                             <<"pay,,1374668512654.66147f3f0605e74a7e9262ed36cc2bed.">>},
%                            {<<"startKey">>,<<>>}],
%                           [{<<"endKey">>,
%                             <<"MWY2Yzk0MzIwZjgyMDhhMDc5ODdkZTA3ZDg1ZmUxMmY=">>},
%                            {<<"id">>,1374668512654},
%                            {<<"location">>,<<"es-3.ceb.loc:60020">>},
%                            {<<"name">>,
%                             <<"pay,0faf51ea7ba2292dd69f6ec66b1e4ba9,1374668512654.82bb149e03df8837554526d2fdd8a4df.">>},
%                            {<<"startKey">>,
%                             <<"MGZhZjUxZWE3YmEyMjkyZGQ2OWY2ZWM2NmIxZTRiYTk=">>}],
process_region_info(TupleList)->

    {value, {_, Location } } = lists:keysearch(<<"location">>, 1, TupleList),
    {value, {_, StartKey } } = lists:keysearch(<<"startKey">>, 1, TupleList),
    {value, {_, EndKey } } = lists:keysearch(<<"endKey">>, 1, TupleList),    
    {
       binary_to_list(Location), 
       base64:decode(StartKey) ,
       base64:decode(EndKey)  
    }                        
.

get_regions(Table)->
       { Hbase_Res, Host } = get_rand_host(),
       Url = Hbase_Res ++ Table ++"/regions",
       io:format("~p get full info ~p",[{?MODULE,?LINE}, Url]),
       case catch  httpc:request( get, 
                                    { Url,
                                    [ {"Accept","application/json"}, {"Host", Host }]},
                                    [ {connect_timeout,?DEFAULT_TIMEOUT },
                                      {timeout, ?DEFAULT_TIMEOUT }
                                       ],
                                    [ {sync, true},
                                      { body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _Reason}, _NewHeaders, Text1 } } ->  
                            Result  = jsx:decode(Text1),
                            io:format("~p get full info ~p",[{?MODULE,?LINE}, Text1]),
                            [ _Name, {_, Vals } ] = Result,
                            lists:map(fun process_region_info/1, Vals );
                    Res -> 
                            ?DEBUG("~p cant get region info ~p  ~n",
                                    [{?MODULE,?LINE},
                                    Res ]),

                            throw({hbase_exception, Res})
        end
.

test()->
    inets:start(),
%     Name = "user_oc_c",
%     ProtoType= [{'X1'},{'X2'},{'X3'}],
    
     Name = "pay",
   
     ProtoType = [{'X1'}, {'X2'}, {'X3'}, {'X4'}, {'X5'}, {'X6'}, {'X7'}, {'X8'}, {'X9'}, {'X10'}, {'X11'}, {'X12'}, {'X13'}, {'X14'} ], 
%     start_process_loop_hbase(Name, ProtoType, empty_ets)
    start_process_loop_hbase(Name, ProtoType, "")
.

flash_stat(Ets, EtsBuffer)->
    List = ets:lookup(Ets, count_processed),
    Value = lists:foldl(fun({_, Val}, Acum)->
                Acum + Val 
                end, 0, List),
    io:format("requests per second ~p~n",[Value]),
    [{_, Value1}] = ets:lookup(Ets, count_processed_all),
    io:format("requests  all ~p~n",[Value + Value1]),
    Mappers =  ets:lookup(Ets, mappers) ,
    WorkSize = length(Mappers),
    
    Workers =  ets:lookup(Ets, workers) ,
    io:format("working mappers now ~p~n",[ WorkSize ]),
    
    ets:delete(Ets, count_processed_all),
    ets:insert(Ets, {count_processed_all, Value+Value1 }),
    ets:delete(Ets, count_processed),
    
    check_buffer(EtsBuffer, Ets, Workers)
.


    
    
thrift_mappper(Conn, Acum)->
        {Host, Port} = ?THRIFT_CONF, 
        io:format("~p connect to ~p~n",[{?MODULE,?LINE}, {{Host, Port}, Conn }]),
        Pid = spawn_link(?MODULE, thrift_mappper_low, [self(), Conn, Acum]  ),
        receive 
            { ListItem, Acum  } ->
                     Pid ! continue_ping,
                    { ListItem, Acum  }
        after ?THRIFT_RECONNECT_TIMEOUT ->
                    exit(Pid, finish),
                    thrift_mappper(Conn, Acum)
        end.           
        
        
thrift_mappper_low(PidReducer,  {_Location, StartKey, EndKey},  {EtsStat, EtsBuffer, Table, ProtoType})->
                   {Host, Port} = ?THRIFT_CONF, 
                   {ok, State}  = thrift_connection_pool:connect(Host, Port, [], hbase_thrift ),
                    io:format("connecting ~p ~n",[ State] ),             
                    Filter = create_filter4all_values(ProtoType, undefined, 1),                    
                    io:format(" generating filter  ~p ~n",[Filter] ),   
                    {State1, {ok, ScannerId}} = generate_scanner(Table,?LIMIT, Filter, State, StartKey, EndKey),
                    %%START mappers
                    io:format("~n ~p create scanner now i will be listening to it ~n",[{?MODULE,?LINE}]),
               
                    ets:insert(EtsStat, { workers, self() } ),
                    PidReducer !  { { self(), State1, ScannerId  }, {EtsStat, EtsBuffer, Table, ProtoType}  },
                    get_data(EtsStat,  EtsBuffer,  PidReducer, 
                                                        ?LIMIT, ScannerId, ProtoType, State1, 0  )
.
    
start_process_loop_hbase(Name, ProtoType, TreeEts)->

        Table =  atom_to_list(common:get_logical_name(TreeEts, Name ) ) ,
        Regions = get_regions(Table),        
        ?DEBUG("~p start_process_loop_hbase ~p ~n", [{?MODULE,?LINE}, {Table, ProtoType, TreeEts} ] ),

        EtsBuffer = ets:new(result_table, [set, public] ),
        EtsStat = ets:new(my_state, [ bag, public  ]),
        ets:insert(EtsStat, {count_processed_all, 0 }),    
%         ets:insert(EtsStat, {disk_storage, Reference }),    
        ?DEBUG("~p starting fetching records through thrift ~n", [{?MODULE,?LINE}] ),
        {Pids, _ } = lists:mapfoldl(fun thrift_mappper/2, { EtsStat, EtsBuffer,Table, ProtoType}, Regions ),
        timer:apply_interval(1000, fact_hbase_thrift, flash_stat,[ EtsStat, EtsBuffer ]),
        %%TODO add continue_ping to 
        ?DEBUG("~p starting listener  ALL for thrift connections ~n", [{?MODULE,?LINE}] ),
        process_loop_hbase(start,   ProtoType, #mapper_state{pids = Pids, ets_buffer = EtsBuffer, ets_stat = EtsStat } )

.
%     0001c378c7e1965cfe540c249850c66c
%%TODO add matching with current context, adding support finishing
get_data(EtsStat, EtsBuffer, Pid, Limit, Id, Prototype, State, I) ->
   
    receive 
        continue_ping ->
            Time = now(),
             io:format("~p connnecting to thrift  : ~p  and ~p ~n", [{?MODULE,?LINE}, { Id, I + Limit  } , EtsBuffer]),
            case catch thrift_client:call(State, scannerGetList, [Id, Limit]) of
                { NewState, {ok, []} } ->            
                            Pid ! {finish, self() },
                            io:format("~p  got finish  in ~p ~n", [{?MODULE,?LINE},  timer:now_diff( now(), Time )] ),
                            delete_scanner({NewState, Id });
                { NewState, {ok, Data} } ->
                    
                    io:format("~p fetch : data   in ~p ~n", [{?MODULE,?LINE}, timer:now_diff(now(),Time ) ]),
                    Size = ets:info(EtsBuffer, size ),            
                    spawn_link(?MODULE, process_data, [Pid, EtsStat, EtsBuffer, Data,  Prototype, Size > ?MAX_ETS_BUFFER_SIZE ]),
                   
                    get_data(EtsStat, EtsBuffer, Pid, Limit, Id, Prototype, NewState, I + Limit)
                    ;
                Error -> 
                    io:format("~p error result: ~p ~n", [Id, Error]),
                    delete_scanner({State, Id }),
                    Pid ! {hbase_exception,Prototype }            
            end;
       Unexpected->
           io:format("~p thrift mapper stoping due to  ~p ~n", [Id, Unexpected])
   end
.

create_uniq_filename()->
    {T1,T2,T3} = now(),
    io_lib:format("~p~p~p",[T1,T2,T3])
.


%%%write new data to the disk   
process_data(Pid, EtsStat, _EtsBuffer, ResultList, ProtoType, true) ->
    %Data = [{tRowResult,<<"90c68ea27aab41601cd822bc5e84214f">>,
    %      [{<<"params:2">>,{tCell,<<"1">>,1371731480251}}]}],
    PreColumns = lists:seq(1, length(ProtoType)),
    Time1 = now(),
    Pid = self(),
    ets:insert(EtsStat, {mappers,Pid}),
    
%     [ { _,Storage } ] = ets:lookup(EtsStat, disk_storage),   

    DiskName = create_uniq_filename(),
    {ok, Storage} = dets:open_file( DiskName, [  ] ),

    
    Columns = lists:map( 
                       fun(E)-> 
                            list_to_binary( ?FAMILY ++ ":" ++ integer_to_list(E) ) 
                       end,
                       PreColumns),
    Time3 = now(),                  
    {_,_, _, FoundCount, AllCount }  = lists:foldr(fun process_record_disk/2, { ProtoType, Columns, Storage, 0,0 }, ResultList ),
    Time2 = now(),
    io:format("process : ~p records in  ~p microseconds and find ~p records ~n", 
                [AllCount,  { timer:now_diff(Time3, Time1), timer:now_diff(Time2, Time1)}, FoundCount ]),
    ets:insert(EtsStat, {count_processed, AllCount} ),
    
    dets:close(Storage),
    ets:insert(EtsStat, {disk_storage, DiskName} ),

    ets:delete_object(EtsStat, {mappers,Pid}),
     Pid ! {result, ack}
;
%%write new data to the memory
process_data(ParentPid, EtsStat, EtsBuffer, ResultList, ProtoType, false) ->
    %Data = [{tRowResult,<<"90c68ea27aab41601cd822bc5e84214f">>,
    %      [{<<"params:2">>,{tCell,<<"1">>,1371731480251}}]}],
    PreColumns = lists:seq(1, length(ProtoType)),
    Time1 = now(),
    Pid = self(),
    ets:insert(EtsStat, {mappers,Pid}),

    Columns = lists:map( 
                       fun(E)-> 
                            list_to_binary( ?FAMILY ++ ":" ++ integer_to_list(E) ) 
                       end,
                       PreColumns),
    Time3 = now(),                  
    
    {_,_, _, FoundCount, AllCount }  = lists:foldr(fun process_record/2, { ProtoType, Columns, EtsBuffer, 0,0 }, ResultList ),
    Time2 = now(),
    io:format("process : ~p records in  ~p microseconds and find ~p records ~n", 
                [AllCount,  { timer:now_diff(Time3, Time1), timer:now_diff(Time2, Time1)}, FoundCount ]),
    ets:insert(EtsStat, {count_processed, AllCount} ),
    ets:delete_object(EtsStat, {mappers,Pid}),
    ParentPid ! {result, ack}
.

process_record_disk(E,  {ProtoType, Columns, DEtsBuffer, CountS, CountB } )->
    ColumnsData  = E#tRowResult.columns,
    
    { _ColumnsData, Record,_, _, Res } = lists:foldr(fun process_tCell/2,  {ColumnsData, [], ProtoType, dict:new(), true }, Columns),
    
%       io:format(" compare ~p and ~p ",[Record, ProtoType]),
     case Res of
         false -> {ProtoType, Columns, DEtsBuffer, CountS, CountB + 1 };
         true ->   
             Key = erlang:make_ref( ),
             dets:insert(DEtsBuffer, { Key, Record } ),
             {ProtoType, Columns, DEtsBuffer, CountS + 1, CountB + 1 }        
    end   
.
%TODO remove coverts tuple_to_list
process_record(E,  {ProtoType, Columns, EtsBuffer, CountS, CountB } )->
    ColumnsData  = E#tRowResult.columns,
    
    { _ColumnsData, Record,_, _, Res } = lists:foldr(fun process_tCell/2,  {ColumnsData, [], ProtoType, dict:new(), true }, Columns),
    
%       io:format(" compare ~p and ~p ",[Record, ProtoType]),
     case Res of
         false -> {ProtoType, Columns, EtsBuffer, CountS, CountB + 1 };
         true ->   
             Key = erlang:make_ref( ),
             ets:insert(EtsBuffer, { Key, Record } ),
             {ProtoType, Columns, EtsBuffer, CountS + 1, CountB + 1 }        
    end   
.

process_tCell(_Key, { ColumnsData, Record, [_Head|ProtoType],  Context, false })->
     { ColumnsData, Record,ProtoType, Context, false };
     
process_tCell(Key, { ColumnsData, Record, [Head|ProtoType],  Context, true })->

        {NewVal, Res, NewContext } =
                case dict:find(Key,  ColumnsData ) of
                  error -> {"", true, Context};
                  { ok,  {tCell, Value, _TimeStamp}  }->
                    %%CONVERT TO LIST when sending result to code processing
                    case Head  of
                        {R} when is_atom(R)->
                             case  dict:find(Head,  Context ) of
                                 error ->
                                    {Value, true,  dict:store(Head, Value, Context) };
                                 {ok, Value} ->
                                    {Value, true, Context};
                                 {ok, _Something} -> %%bad match
                                    {Value, false, Context}
                             end
                            ;
                        %%CAUSE there filter logic must work    
                       _Some-> %%process all items in prototype to the unicode binary
                            {Head, true, Context}                
                         
                    end    
%                   unicode:characters_to_list(Value)
               end,
     { ColumnsData, [ NewVal| Record ], ProtoType, NewContext,  Res }.



	
%% gen scanner



    


    
generate_scanner(Table, Limit,Filter, State, StartKey)->
    generate_scanner(Table,Limit, Filter, State, StartKey, undefined)
.

generate_scanner(Table,Limit, Filter, State, StartKey, [])->
    generate_scanner(Table,Limit, Filter, State, StartKey, undefined)
;
generate_scanner(Table,Limit,  Filter, State, StartKey, EndKey) ->

    Args = [ Table,
             #tScan{
                    startRow = StartKey,
                    stopRow = EndKey,
                    columns = [<<"params">>], 
                    filterString = Filter,
                    batchSize = Limit
                    },
                    dict:new()
    ],
    io:format("~p generate scanner ~p ~n",[{?MODULE,?LINE}, Args]),
    thrift_client:call(State,  scannerOpenWithScan, Args).
    
    
    
    
    
%% gen filters
%TODO add filtering structs

create_filter4all_values([], Filters, _) ->
    Filters;
create_filter4all_values([{Var}|T], Filters, I) when is_atom(Var) ->
    create_filter4all_values(T, Filters, I + 1);
    %%STARTING of filter string
create_filter4all_values([Value|T], Filter, I) when is_binary(Value), byte_size(Filter) =:= 0 ->
    NewFilter = create_one_filter(Value, I), 
    create_filter4all_values(T, NewFilter, I + 1); 
    
create_filter4all_values([Value|T], undefined, I) when is_binary(Value) ->
    NewFilter = create_one_filter(Value, I),
    create_filter4all_values(T, NewFilter, I + 1);
    
create_filter4all_values([Value|T], Filter, I) when is_binary(Value) ->
    NewFilter = create_one_filter(Value, I),
    Compound = thrift_filters:compound_filters(default, Filter, NewFilter, <<"AND">>),
    create_filter4all_values(T, Compound, I + 1);
    
create_filter4all_values([Value|T], Filter, I) when is_integer(Value) ->
    NewVal = list_to_binary( integer_to_list(Value) ),
    create_filter4all_values([NewVal|T], Filter, I);    
create_filter4all_values([Value|T], Filter, I) when is_float(Value) ->
    NewVal = list_to_binary( float_to_list(Value) ),
    create_filter4all_values([NewVal|T], Filter, I);    

create_filter4all_values([Value|T], Filter, I) when is_atom(Value) ->
    NewVal = list_to_binary( atom_to_list(Value) ),
    create_filter4all_values([NewVal|T], Filter, I);       
create_filter4all_values([Value|T], Filter, I) when is_list(Value) ->
    NewVal = unicode:characters_to_binary(Value),
    create_filter4all_values([NewVal|T], Filter, I).

create_one_filter(Value, Ar) ->
    {ok, Filter} = thrift_filters:init([]),
    
    ArB = list_to_binary( integer_to_list(Ar) ),
    
    Filter1 = Filter#filter{name = 'SingleColumnValueFilter',
                            compare_operator = <<"=">>,
                            comparator = Value,
                            family = list_to_binary(?FAMILY),
                            qualifier = ArB
    },
    thrift_filters:generate(Filter1).   
