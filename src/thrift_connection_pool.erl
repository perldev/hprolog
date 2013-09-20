-module(thrift_connection_pool).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1, stop/0, status/0 ]).

-export([get_free/0, return/1,reconnect/1, connect/4,reconnect_long/1 ]).

-include("prolog.hrl").
-define(CRITICAL, 3500000).%%%TODO move it to config
-define(SLEEP_CRITICAL, 1000).%%%TODO move it to config
-record(thrift_pool,{
                  free,
                  busy,
                  speed_get,
                  speed_return,
                  sleep,
                  sleep_timeout
                }
                ).
                
-define(THRIFT_BUSY, thrift_connection_pool_busy ).



start_link(Count) ->
          gen_server:start_link({local, ?MODULE},?MODULE, [ Count  ],[]).

          
%%TODO name spaces

init([Count]) ->

        Free =  start_pool(Count),
        timer:apply_interval(?SLEEP_CRITICAL, ?MODULE, reconnect_long, [ ?CRITICAL ] ),
        { ok,#thrift_pool{free = Free,speed_get = 0, sleep = 0, sleep_timeout = ?SLEEP_CRITICAL, speed_return = 0, 
        busy = ets:new(?THRIFT_BUSY, [set,  public]) } }
.

start_pool(Count)->
                      {Host, Port}  = ?THRIFT_CONF,
                      lists:map( fun(E)-> 
                            case catch thrift_connection_pool:connect(Host, Port, [], hbase_thrift ) of
                                {ok, State} ->
                                        {E, State};
                                Exception ->
                                    throw({hbase_thrift_exception, Exception})
                            end
                       end, lists:seq(1, Count) )
                       
.


reconnect_long( ReasonTimeout )->
    Now = erlang:now(),
    ListTimeout = 
      ets:foldl(
        fun( {Key, _Connection , Started }, Acc )->
            case timer:now_diff(Now, Started) > ReasonTimeout of
                    true->
                        [Key | Acc];
                    false->
                        Acc        
            end
        end
    , [], ?THRIFT_BUSY),
    lists:foreach(fun(Key)->
                    ?WAIT("~p THROW connection  ~p ~n",
                           [{?MODULE,?LINE}, Key ]),
                    ?MODULE:reconnect(Key)
                  end, ListTimeout)
.


get_free()->
    gen_server:call(?MODULE, get_free).
    
return( NewState )->
    gen_server:cast(?MODULE, {return, NewState}).
    
reconnect(Key)->
    gen_server:cast(?MODULE,{reconnect, Key}).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(status,_From ,State)->
    { reply,  { ets:tab2list(State#thrift_pool.busy),
                State#thrift_pool.busy, 
                ets:info(State#thrift_pool.busy, size),
                length(State#thrift_pool.free) }, State }
;    



handle_call(get_free,_From, State = #thrift_pool{ free  = [] }) ->
    { reply,  {hbase_thrift_exception, empty_pool }, State }
;
handle_call(get_free,_From, State) ->
    [Head|Tail] = State#thrift_pool.free,    
    {Key, Connection} = Head,
    Now = now(),
    ets:insert(State#thrift_pool.busy, {Key, Connection , Now }  ),  
    ?WAIT("return  free  call ~p ~n",
                           [ Head ]),
    {reply,  Head  ,State#thrift_pool{ free = Tail, sleep = 0 , speed_get = Now } }
.

stop() ->
    gen_server:cast(?MODULE, stop).

    
handle_cast( { return,  NewState = {Key, _NewConn} }, MyState) ->
         ?WAIT("~p got back new connection  ~p ~n",
                           [ { ?MODULE, ?LINE }, NewState ]),
         Stack = MyState#thrift_pool.free,
         ets:delete( MyState#thrift_pool.busy, Key  ),
         {noreply, MyState#thrift_pool{ free = [ NewState|Stack ], speed_return = now() } } ;
         
         
handle_cast( { reconnect,  hbase_thrift_exception }, MyState) -> 
        { noreply, MyState } ;
handle_cast( { reconnect,  Key }, MyState) ->
         ?WAIT("~p got back new connection  ~p ~n",
                           [ { ?MODULE, ?LINE }, Key ]),
         {Host, Port}  = ?THRIFT_CONF,
         Stack = MyState#thrift_pool.free,
         ets:delete( MyState#thrift_pool.busy, Key  ),
         case catch thrift_connection_pool:connect(Host, Port, [], hbase_thrift ) of
                                {ok, NewConn} ->
                                        {noreply, MyState#thrift_pool{ free = [ {Key, NewConn} | Stack ] } } ;  
                                Exception ->
                                    ?WAIT("got exception during the reconnect ~p ~n", [Exception]),
                                    timer:apply_after(?THRIFT_RECONNECT_TIMEOUT, thrift_connection_pool, reconnect, [ Key ] ),
                                    { noreply, MyState} 
         end
;        
handle_cast( Undef, MyState) ->
        ?WAIT("~p undefined msg ~p ~n",
                           [ { ?MODULE, ?LINE }, Undef ]),
         {noreply, MyState}.
% ----------------------------------------------------------------------------------------------------------
% Function: handle_info(Info, State) -> {noreply, State} | {noreply, State, Timeout} | {stop, Reason, State}
% Description: Handling all non call/cast messages.
% ----------------------------------------------------------------------------------------------------------
% handle info when child server goes down
% {'DOWN',#Ref<0.0.0.73>,process,<0.56.0>,normal}

handle_info({'DOWN',_,_,Pid,Reason}, State)->
       
       ?WAIT("~p process  msg ~p  ~n",
                           [ {?MODULE,?LINE}, { Pid,Reason } ]),
       
       
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
      gen_server:call(?MODULE, status).
      
connect(Host, Port, Opts, Service) ->
    thrift_client_util:new(Host, Port, Service, Opts).      
      

