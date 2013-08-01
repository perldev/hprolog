-module(thrift_connection_pool).
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1, stop/0, status/0 ]).

-export([get_free/0, return/1,reconnect/1 ]).

-include("prolog.hrl").

-record(thrift_pool,{
                  free,
                  busy
                  
                }
                ).
                
-define(THRIFT_BUSY, some ).



start_link(Count) ->
          gen_server:start_link({local, ?MODULE},?MODULE, [ Count  ],[]).

          
%%TODO name spaces

init([Count]) ->

        Free =  start_pool(Count),
        { ok,#thrift_pool{free = Free, busy = ets:new(?THRIFT_BUSY, [set,  private]) } }
.

start_pool(Count)->
                      {Host, Port}  = ?THRIFT_CONF,
                      lists:map( fun(E)-> 
                            case catch hbase_thrift_api:connect(Host, Port, [], hbase_thrift ) of
                                {ok, State} ->
                                        {E, State};
                                Exception ->
                                    throw({hbase_thrift_exception, Exception})
                            end
                       end, lists:seq(1, Count) )
                       
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
    { reply,  { ets:tab2list(State#thrift_pool.busy), State#thrift_pool.free }, State }
;    
handle_call(get_free,_From ,State) ->
    [Head|Tail] = State#thrift_pool.free,
    {Key, Connection} = Head,
    ets:insert(State#thrift_pool.busy, {Key, Connection , now() }  ),
    ?WAIT("return  free  call ~p ~n",
                           [ Head ]),
    {reply,  Head  ,State#thrift_pool{ free = Tail } }
.

stop() ->
    gen_server:cast(?MODULE, stop).

    
handle_cast( { return,  NewState = {Key, _NewConn} }, MyState) ->
         ?WAIT("~p got back new connection  ~p ~n",
                           [ { ?MODULE, ?LINE }, NewState ]),
         Stack = MyState#thrift_pool.free,
         ets:delete( MyState#thrift_pool.busy, Key  ),
         {noreply, MyState#thrift_pool{ free = [ NewState|Stack ] } } ;
         
handle_cast( { reconnect,  Key }, MyState) ->
         ?WAIT("~p got back new connection  ~p ~n",
                           [ { ?MODULE, ?LINE }, Key ]),
         {Host, Port}  = ?THRIFT_CONF,
         Stack = MyState#thrift_pool.free,
         ets:delete( MyState#thrift_pool.busy, Key  ),
         case catch hbase_thrift_api:connect(Host, Port, [], hbase_thrift ) of
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

