-module(prolog_trace).
-compile(export_all).
-include("prolog.hrl").



trace_on(?TRACE_OFF, TreeEts,_Pid )->
	ets:insert(TreeEts, {?DEBUG_STATUS, false})
;
trace_on(?TRACE_ON, TreeEts,TracePid )->
    ets:insert(TreeEts, {?DEBUG_STATUS, true, ?TRACE_ON, TracePid })
;
trace_on(?DEBUG_OFF, TreeEts, _ )->
    ets:insert(TreeEts, {?DEBUG_STATUS,false })
;
trace_on(?DEBUG_ON, TreeEts, DebugPidLogger )->
    ets:insert(TreeEts, {?DEBUG_STATUS, true, ?DEBUG_ON,  DebugPidLogger})
.  

  
trace_on(?TRACE_OFF, TreeEts )->
	ets:insert(TreeEts, {?DEBUG_STATUS, false})
;
trace_on(?TRACE_ON, TreeEts )->
    TracePid = spawn_link(?MODULE, start_trace, []),
    ets:insert(TreeEts, {?DEBUG_STATUS, true, ?TRACE_ON, TracePid })
;
trace_on(?DEBUG_OFF, TreeEts )->

    ets:insert(TreeEts, {?DEBUG_STATUS,false })

;
trace_on(?DEBUG_ON, TreeEts )->
    DebugPidLogger = spawn_link(?MODULE, start_debug_logger, []),
    ets:insert(TreeEts, {?DEBUG_STATUS, true, ?DEBUG_ON,  DebugPidLogger})

.

%%TODO change it it must be turn off not only via MACROS
trace(Index, Tree,  Body, Context)->

      case ets:lookup(Tree,?DEBUG_STATUS ) of
	  [ ]->   true;
	  [ {_, false} ]-> true;
	  [ {_, true, ?TRACE_ON, TracerPid }]->
		 ?WAIT("~p trace  ~p ~n",[{?MODULE,?LINE}, TracerPid]), 
		 TracerPid ! {debug_msg, self(),Index, prolog_matching:bound_body(Body,Context) },
		 receive 
		      next ->
			    ?WAIT("~p got ~p ~n",[{?MODULE,?LINE}, next]), 
			    true;
		      finish -> 
			    ?WAIT("~p got ~p ~n",[{?MODULE,?LINE}, finish]), 
			    exit(trace_finish)
		 end;
	  [ {_, true, ?DEBUG_ON, DebugPidLogger }]->
		  DebugPidLogger ! {debug_msg,Index, prolog_matching:bound_body(Body,Context) },
		  true
      end
.

trace2(Index, Tree,  Atom, Body) when is_atom(Atom)->

      ?WAIT("~p tracer got ~p ~n",[{?MODULE,?LINE}, { Body, Atom } ]),
      case ets:lookup(Tree,?DEBUG_STATUS ) of
	  [ ]->   true;
	  [ {_, false} ]-> true;
	  [ {_, true, ?TRACE_ON, TracerPid }]->
		 TracerPid ! {res_msg, self(), Index ,{Atom, Body} },
		 receive 
		      next ->
			    ?WAIT("~p got ~p ~n",[{?MODULE,?LINE}, next]), 
			    true;
		      finish -> 
			    ?WAIT("~p got ~p ~n",[{?MODULE,?LINE}, finish]), 
			    exit(trace_finish)
		 end;
	  [ {_, true, ?DEBUG_ON, DebugPidLogger } ] ->
		  DebugPidLogger ! {res_msg, Index, {Atom, Body} },
		  true
      end
;
trace2(Index, Tree,  {Res, Context}, Body)->
      ?WAIT("~p tracer got ~p ~n",[{?MODULE,?LINE}, { Body, Res } ]),

      case ets:lookup(Tree,?DEBUG_STATUS ) of
	  [ ]->   true;
	  [ {_, false} ]-> true;
	  [ {_, true, ?TRACE_ON, TracerPid }]->
	         ?WAIT("~p send res ~p ~n",[{?MODULE,?LINE}, {TracerPid, Body, Res } ]),
		  
		 TracerPid ! {res_msg, self(), Index,   Res },
		 receive 
		      next ->
			    ?WAIT("~p got ~p ~n",[{?MODULE,?LINE}, next]), 
			    true;
		      finish -> 
			    ?WAIT("~p got ~p ~n",[{?MODULE,?LINE}, finish]), 
			    exit(trace_finish)
		 end;
	  [ {_, true, ?DEBUG_ON, DebugPidLogger } ] ->
		  DebugPidLogger ! {res_msg, Index,  Res },
		  true
      end
.

start_trace()->
    trace_loop(0)
.

trace_loop(TraceIndex)->
	receive 
	    {debug_msg, BackPid,Index, Body } ->
		  io:fwrite("~p aim ~p call  ~p ? ~n yes or no~n  ", [TraceIndex, Index, Body ] ),
		  Line = io:get_line(""),
		  case Line of
		      [$y,$e,$s| _ ] ->
			  BackPid ! next,
			  trace_loop(TraceIndex+1);
		      "\n" ->
			  BackPid ! next,
			  trace_loop(TraceIndex+1);
		      _ ->
			 io:fwrite("finish job ~n"),
			 exit(normal),
			 BackPid ! finish
		  end
	    ;
	    {res_msg, BackPid, Index, Body } ->
		  io:fwrite("aim ~p got  ~p ?  ~n  ", [Index ,Body ] ),
		  Line = io:get_line(" "),
		  case Line of
		      [$y,$e,$s| _ ] ->
			  BackPid ! next,
			  trace_loop(TraceIndex+1);
		      "\n" ->
			  BackPid ! next,
			  trace_loop(TraceIndex+1);
		      _ ->
			 io:fwrite("finish job ~n"),
 			 exit(normal),
			 BackPid ! finish
		  end
	    ;
	    finish ->
		?LOG("~p got finish msg  ~n",[{?MODULE,?LINE} ]), 
		exit(normal);
	    _Unexpected->
		?LOG("~p got unexpected msg ~p ~n",[{?MODULE,?LINE}, _Unexpected]), 
		exit(normal)
        end
.
  

start_debug_logger()->
    debug_logger([])
.

debug_logger(List)->
	receive 
	    {debug_msg, Body}->
		    debug_logger( [ Body|List ] )
		;
	    {stack, Back}->
		Back ! lists:reverse(List),
		debug_logger( [] );
	    finish->
		exit(debug_normal)
        end
.
