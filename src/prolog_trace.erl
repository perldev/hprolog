-module(prolog_trace).
-compile(export_all).
-include("prolog.hrl").



trace_on(?TRACE_OFF, TreeEts,_Pid )->
	ets:insert(TreeEts, {system_record, ?DEBUG_STATUS, false})
;
trace_on(?TRACE_ON, TreeEts,TracePid )->
    ets:insert(TreeEts, {system_record, ?DEBUG_STATUS, true, ?TRACE_ON, TracePid })
;
trace_on(?DEBUG_OFF, TreeEts, _ )->
    ets:insert(TreeEts, {system_record, ?DEBUG_STATUS,false })
;
trace_on(?DEBUG_ON, TreeEts, DebugPidLogger )->
    ets:insert(TreeEts, {system_record, ?DEBUG_STATUS, true, ?DEBUG_ON,  DebugPidLogger})
.  

  
trace_on(?TRACE_OFF, TreeEts )->
	ets:insert(TreeEts, {system_record,?DEBUG_STATUS, false})
;
trace_on(?TRACE_ON, TreeEts )->
    ets:insert(TreeEts, {system_record, ?DEBUG_STATUS, true, ?TRACE_ON })
;
trace_on(?DEBUG_OFF, TreeEts )->

    ets:insert(TreeEts, {system_record, ?DEBUG_STATUS,false })

;
trace_on(?DEBUG_ON, TreeEts )->
    ets:insert(TreeEts, {system_record, ?DEBUG_STATUS, true, ?DEBUG_ON})

.

%%TODO change it it must be turn off not only via MACROS
trace(Index, Tree,  Body, Context)->

      case ets:lookup(Tree,?DEBUG_STATUS ) of
	  [ ]->   true;
	  [ {system_record, _, false} ]-> true;
	  [ {system_record, _, true, ?TRACE_ON }]->
                 ?SYSTEM_STAT(Tree, Index),
		 call_tracer(Index, Body);
	  [ {system_record, _, true, ?DEBUG_ON, DebugPidLogger }]->
		  DebugPidLogger ! {debug_msg,Index, prolog_matching:bound_body(Body,Context) },
		  true
      end
.

trace2(Index, Tree,   Body, _Context) ->

      ?WAIT("~p tracer got ~p  is ~p  ~n",[{?MODULE,?LINE},Index, Body ]),
      case ets:lookup(Tree,?DEBUG_STATUS ) of
	  [ ]->   true;
	  [ {system_record, _, false} ]-> true;
	  [ {system_record, _, true, ?TRACE_ON }]->
                  ?SYSTEM_STAT(Tree, Index),
		  got_tracer(Index, Body);
	  [ {system_record, _, true, ?DEBUG_ON, DebugPidLogger } ] ->
		  DebugPidLogger ! {res_msg, Index, Body },
		  true
      end
.

call_tracer(Index, Body)->
    io:fwrite("~p aim  call  ~ts ? ~n yes or no~n  ", [Index, lists:flatten(erlog_io:write1( Body ) ) ] ),
    
    Line = io:get_line(""),
    case Line of
        [$y,$e,$s| _ ] ->
            true;                       
         "\n" ->
            true;
         _ ->
            io:fwrite("finish job ~n"),
            throw({tracer, finished})
                        
    end.
                  
got_tracer(Index, Body)->
                  io:fwrite("~p aim  got  ~ts ? ~n yes or no~n  ", [Index, lists:flatten(erlog_io:write1(Body)) ] ),
                  Line = io:get_line(""),
                  case Line of
                      [$y,$e,$s| _ ] ->
                          true;                       
                      "\n" ->
                          true;
                      _ ->
                         io:fwrite("finish job ~n"),
                         throw({tracer, finished})
                         
                  end.                  





  

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
