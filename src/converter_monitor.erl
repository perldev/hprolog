-module(converter_monitor).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/0,start_link/1, stop/0, status/0,stat/4,statistic/0, regis_timer_restart/1, regis/2 ,regis/1, kill_process_after/1 ]).
-export([stop_converter/0, start_converter/0, start_statistic/0, update_hbase_stat/1, check_run/0, find_shortes/2,process_stat/2]).



-record(monitor,{
		  proc_table
                }
                ).
-include("prolog.hrl").

start_link(LogFun) ->
          
          gen_server:start_link({local, ?MODULE},?MODULE, [ LogFun ],[]).

start_link() ->
          Fun = fun(Str, Params)-> ?LOG(Str, Params) end,
	  gen_server:start_link({local, ?MODULE},?MODULE, [ Fun ],[]).

	  
%%TODO name spaces

init([LogFun]) ->
	 common:prepare_log("log/e_"),
	 inets:start(),
	 crypto:start(),
	 ?LOG_APPLICATION,
         ets:new(?ERWS_LINK, [set, public,named_table ]),
         start_statistic(),
         timer:apply_interval(?UPDATE_STAT_INTERVAL, ?MODULE, update_hbase_stat, [ LogFun ] ),
         { ok, #monitor{proc_table = ets:new( process_information, [named_table ,public ,set ] ) } }
.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_statistic()->
    ets:new(?SCANNERS_HOSTS_TABLE,[named_table, set, public]),
    ets:new(?STAT, [named_table, set, public]),  
    ets:new(?APPLICATION, [named_table, set, public]),
    lists:foreach(fun(E)-> ets:insert(?SCANNERS_HOSTS_TABLE, {E,0,0} )  end,?HBASE_HOSTS),
    ets:new( ?SCANNERS_HOSTS_LINK, [named_table, set, public] )

    .

check_run()->
      case ets:lookup(?APPLICATION, converter_run) of
            []-> false;
            [{_, Res}] -> Res
    
    end.

    
handle_call(Info,_From ,State) ->
    ?WAIT("get msg call ~p ~n",
                           [Info]),
    {reply,nothing ,State}
.
stop() ->
    gen_server:cast(?MODULE, stop).


handle_cast( { regis_timer_restart,  Pid }, MyState) ->
 	 ?WAIT("~p start monitor ~p ~n",
                           [ { ?MODULE, ?LINE }, Pid ]),
         erlang:monitor( process,Pid ),
         timer:apply_after(?RESTART_CONVERTER,
                                  ?MODULE,
                                  kill_process_after, [ Pid ]
                                 ),
         ets:insert(MyState#monitor.proc_table, {Pid, timer}),
   
         {noreply, MyState};
handle_cast( { kill_process_after,  Pid }, MyState) ->
 	?WAIT("~p start monitor ~p ~n",
                           [ { ?MODULE, ?LINE }, Pid ]),
	 %demonitor(Pid),
         erlang:exit(Pid, by_timer),
         ets:delete(MyState#monitor.proc_table, Pid),
         {noreply, MyState};
handle_cast( { regis,  Pid, Description }, MyState) ->
 	?WAIT("~p start monitor ~p ~n",
                           [ { ?MODULE, ?LINE }, Pid ]),
         erlang:monitor( process, Pid ),
         ets:insert(MyState#monitor.proc_table, { Pid, Description }),
         {noreply, MyState};
         
handle_cast( { regis,  Pid }, MyState) ->
 	?WAIT("~p start monitor ~p ~n",
                           [ { ?MODULE, ?LINE }, Pid ]),
         erlang:monitor( process, Pid ),
         ets:insert(MyState#monitor.proc_table, {Pid, watch}),
         {noreply, MyState}.
% ----------------------------------------------------------------------------------------------------------
% Function: handle_info(Info, State) -> {noreply, State} | {noreply, State, Timeout} | {stop, Reason, State}
% Description: Handling all non call/cast messages.
% ----------------------------------------------------------------------------------------------------------
% handle info when child server goes down
% {'DOWN',#Ref<0.0.0.73>,process,<0.56.0>,normal}
kill_process_after(Pid)->
     gen_server:cast(?MODULE, {kill_process_after, Pid}).

    


handle_info({'DOWN',_,_,Pid,Reason}, State)->
       
       ?WAIT("~p process  msg ~p  ~n",
                           [ {?MODULE,?LINE}, { Pid,Reason } ]),
       
       ets:delete(State#monitor.proc_table, Pid),
       
       {noreply,  State}
;
handle_info(Info, State) ->
    ?WAIT("get msg  unregistered msg ~p ~n",
                           [Info]),

    {noreply,  State}.


% ----------------------------------------------------------------------------------------------------------
% Function: terminate(Reason, State) -> void()
% Description: This function is called by a gen_server when it is about to terminate. When it returns,
% the gen_server terminates with Reason. The return value is ignored.
% ----------------------------------------------------------------------------------------------------------
terminate(_Reason, _State) ->
   terminated.

status()->
      List = ets:tab2list( process_information ),List.

statistic()->
    ListType = ets:foldl(fun( {  { Type, Name }  , {true, TrueCount, false, FalseCount } }, In )->
		  NewVal = [ { Type, [   [ { Name, TrueCount } ], [ {Name, FalseCount } ]  ]  }  ],
		  [NewVal | In]
		  end, [], ?STAT),
    ListType.
    
    
    
regis_timer_restart(Pid)->
    gen_server:cast(?MODULE,{regis_timer_restart, Pid}).


regis(Pid, Description)->
    gen_server:cast(?MODULE,{regis, Pid, Description}).
    
    
regis(Pid)->
    gen_server:cast(?MODULE,{regis, Pid}).
    
stop_converter()->
    ets:insert(?APPLICATION,{converter_run, false}).
start_converter()->
    ets:insert(?APPLICATION,{converter_run, true}).   

    
    
    
%%update statistic of     
-ifdef(USE_HBASE).

update_hbase_stat(Fun)->
      NameSpaces = fact_hbase:get_list_namespaces(),
      Stat = ets:tab2list(?STAT),
      ?LOG("~p update  stat of hbase   ~p~n",[{?MODULE,?LINE}, {NameSpaces, Stat} ]),
      lists:foldl(fun process_stat/2, {NameSpaces, Fun}, Stat  ),	 
      ets:delete_all_objects(?STAT).
      
-else.

update_hbase_stat(_Fun)->
	ets:delete_all_objects(?STAT).

   
-endif.

find_shortes(LongName, Prefixes) when is_atom(LongName) ->
  find_shortes(atom_to_list(LongName), Prefixes);
find_shortes(LongName, Prefixes) ->
    NameSpace = lists:foldl(fun(E, Prefix)-> 
                                      case lists:prefix(E, LongName) of
                                            true ->
                                                 case length(Prefix)>length(E) of
                                                    true  -> Prefix;
                                                    false ->  E
                                                 end; 
                                            false ->
                                                  Prefix
                                      end
                             end, "" , Prefixes ),
    Name = common:get_namespace_name(NameSpace, LongName),                   
    { Name, common:get_logical_name(NameSpace, ?META_FACTS) }
.
      
%TODO do not add inner predicates
%    hbase_low_get_key(MetaTable, "stat", FactNameL, "facts_count")
% 
% ;
% meta_info({'meta', FactName, requests, Val },  Prefix) when is_atom(FactName) ->
%       MetaTable = common:get_logical_name(Prefix, ?META_FACTS),
%       FactNameL = erlang:atom_to_list(FactName),
%       hbase_low_get_key(MetaTable, "stat", FactNameL, "facts_reqs")
%


%%%VERY IMPORTANT PART!!!!
process_stat({ {add, RealFactName }, {_, TrueCount,_, _FalseCount }  }, Acum)->
          { Namespaces, LogFun } = Acum,
	  %HACK replace it
	  {Name, MetaTable} = find_shortes(RealFactName, Namespaces),
	  LogFun("~p save count to stat hbase ~p to ~p ~n",[{?MODULE,?LINE}, {RealFactName, TrueCount},{Name, MetaTable} ]),
	  case catch fact_hbase:hbase_low_get_key(MetaTable,  Name, "stat", "facts_count") of
                        {hbase_exception, not_found} -> 
                            LogFun("~p key not found  ~p ~n",[{?MODULE,?LINE}, {facts_count ,Name, MetaTable} ]),
                            NewCount = integer_to_list( TrueCount ),
                            fact_hbase:hbase_low_put_key(MetaTable, Name, "stat", "facts_count", NewCount );
                        {hbase_exception, Res }->   
                            LogFun("~p hbase exception   ~p ~n",[{?MODULE,?LINE}, { Name, MetaTable, Res} ]);
                        Res ->
                           PreVal = common:inner_to_int( Res ),
                           LogFun("~p new count ~p ~n",[{?MODULE,?LINE}, {Name, MetaTable, PreVal} ]),
                           NewCount = integer_to_list( PreVal + TrueCount ),
                           fact_hbase:hbase_low_put_key(MetaTable, Name, "stat", "facts_count", NewCount )
          end,
          Acum
	 
	  
	  
;
process_stat({ {'search', RealFactName }, {_, TrueCount,_, _FalseCount }  }, Acum )->
          { Namespaces, LogFun } = Acum,
	  { Name, MetaTable } = find_shortes(RealFactName, Namespaces),
  	  LogFun("~p save count to stat hbase ~p to ~p ~n",[{?MODULE,?LINE}, {RealFactName, TrueCount}, {Name, MetaTable} ]),
          case catch fact_hbase:hbase_low_get_key(MetaTable,  Name, "stat", "facts_reqs") of
                        {hbase_exception, not_found} -> 
                            LogFun("~p key not found  ~p ~n",[{?MODULE,?LINE}, {fact_reqs, Name, MetaTable} ]),
                            NewCount = integer_to_list( TrueCount ),
                            fact_hbase:hbase_low_put_key(MetaTable, Name, "stat", "facts_reqs", NewCount );
                        {hbase_exception, Res }->
                                LogFun("~p hbase exception   ~p ~n",[{?MODULE,?LINE}, { Name, MetaTable, Res} ]);
                        Res ->
                                PreVal = common:inner_to_int( Res ),
                                LogFun("~p new count ~p ~n",[{?MODULE,?LINE}, {Name, MetaTable, PreVal} ]),
                                NewCount = integer_to_list( PreVal + TrueCount ),
                                fact_hbase:hbase_low_put_key(MetaTable, Name, "stat", "facts_reqs", NewCount )
          end,
          Acum
;
process_stat(Nothing, Acum )->
         { Namespaces, LogFun } = Acum,
	 LogFun("~p dont logging this ~p  ~n",[{?MODULE,?LINE}, Nothing]),
	 Acum
.



	
   
%%prototype do not use     
stat(Type, Name,  _ProtoType, Res)->
    Key  =  { Type, Name },
     ?LOG("~p stat  ~p  ~n",[{?MODULE,?LINE}, Key]),
    case ets:lookup(?STAT, { Type, Name }) of
	[ {Key, {true, TrueCount, false, FalseCount  }} ]->
		case Res of
		    true->
			ets:insert(?STAT, { Key, {true, TrueCount+1, false, FalseCount }  } );
		    false->
			ets:insert(?STAT, { Key, {true, TrueCount, false, FalseCount+1 }   })
		end;
	      
	[  ]->
		case Res of
		    true->
			ets:insert(?STAT, { Key, {true, 1, false, 0 } }  );
		    false->
			ets:insert(?STAT ,{Key, {true, 0, false, 1 } }  )
		end
		
	end


    
.
    
