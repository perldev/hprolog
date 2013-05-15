%%% File    : prolog_shell.erl
%%% Author  : Bogdan Chaicka
%%% Purpose : A simple  shell.
-module(prolog_shell).

-export([start/0,start/1,server/1]).

-import(lists, [foldl/3,foreach/2]).
-compile(export_all).
-include("prolog.hrl").


start() -> start("").

api_start(Prefix)->
    prolog:create_inner_structs(Prefix),
    ?INCLUDE_HBASE( Prefix )
    
.

start(default) ->
    prolog:create_inner_structs(""),
    ?INCLUDE_HBASE(""),
    server("") 
;

start(Prefix)-> 
    inets:start(),
    crypto:start(),
    ?LOG_APPLICATION,
    prolog:create_inner_structs(Prefix),
    ?INCLUDE_HBASE( Prefix ),
    server(Prefix) 

.


server(Prefix) ->
    io:fwrite("Prolog Shell V~s (abort with ^G)\n",
	      [erlang:system_info(version)]),
    server_loop(Prefix, ?TRACE_OFF).

%% A simple Prolog shell similar to a "normal" Prolog shell. It allows
%% user to enter goals, see resulting bindings and request next
%% solution.

server_loop(P0, TraceOn) ->
    process_flag(trap_exit, true),
    case erlog_io:read('| ?- ') of
	{ok,halt} -> ok;
	{ok, trace_on}->
	      io:fwrite("trace on Yes~n"),  
	      server_loop(P0, ?TRACE_ON)
	;
	{ok, nl}->
	       io:fwrite("~n"),  
	       server_loop(P0, TraceOn);
	{ok, stat}->
	       io:fwrite("todo this is ~n"),  
	       server_loop(P0, TraceOn);
	{ok, listing}->
	      io:fwrite("listing is ~n"),  
	      Code = get_code_memory(P0),
	      io:fwrite("~p ~n", [Code]),   
	      server_loop(P0, TraceOn)
	;
	{ok, trace_off}->
	    io:fwrite("trace off Yes~n"),    
	    server_loop(P0, ?TRACE_OFF)
	;
	
	{ok, debug_on}->
	    io:fwrite("debug on Yes~n"),  
	    server_loop(P0, ?DEBUG_ON)
	;
	{ok, debug_off}->
	    io:fwrite("debug off Yes~n"),  

	    server_loop(P0, ?DEBUG_OFF)
	;
	{ok, Goa1l} when is_atom(Goa1l)->
	      io:fwrite("Goal complex: ~p~n", [Goa1l]),
	      Goal = {Goa1l, true},
%%TODO  process body if we use  complain request
%% solution make temp aim with updated variables
	      ets:new(tree_processes,[ public, set, named_table ] ), 
	      prolog_trace:trace_on(TraceOn, tree_processes ),
	      ets:insert(tree_processes, {?PREFIX, P0 } ),
      	      ?DEV_DEBUG("trace config : ~p~n", [ {ets:tab2list(tree_processes) } ]),   
	      ?DEBUG("TempAim : ~p~n", [ Goal  ]),
              StartTime = erlang:now(), 
	      BackPid = spawn_link(prolog, conv3, [{true}, Goal,  dict:new(),
			      erlang:self(), now(), tree_processes]),
	      process_prove(Goal, Goal, BackPid, StartTime ),	       
	      ets:delete(tree_processes),%%%this
	      server_loop(P0, TraceOn);
	{ok,Files} when is_list(Files) ->
% 	    {{ok,Db0},P1} = P0(get_db),
	    lists:foreach(fun(File)->
			      case catch(prolog:compile(P0, File)) of
				  ok ->
				      io:fwrite("Yes~n"),    
				      server_loop(P0, TraceOn);
				  Error ->
				      io:fwrite("Error: ~p~n", [Error]),
				      server_loop(P0, TraceOn)
			      end
			    end, Files)
	    
	    ;
	 {ok, Goal = {':-',_,_ } } ->
		io:fwrite("syntax error may be you want use assert ~p ~n",[Goal]),
		server_loop(P0, TraceOn);
	{ok,Goal = {';', _Rule, _Body} } ->
	      io:fwrite("Goal complex: ~p~n", [Goal]),
%%TODO  process body if we use  complain request
%% solution make temp aim with updated variables
	      {TempAim, Dict} =  make_temp_complex_aim(Goal, dict:new()), 
	      ets:new(tree_processes,[ public, set, named_table ] ),
	      ets:insert(tree_processes, {?PREFIX, P0 } ),
	      prolog_trace:trace_on(TraceOn, tree_processes ),
      	      ?DEV_DEBUG("trace config : ~p~n", [ {ets:tab2list(tree_processes) } ]),
	      ListVars  = dict:to_list(Dict),
	      TempAim1 = list_to_tuple([ ?TEMP_SHELL_AIM  |lists:map(fun({ Normal, Temp})->  Temp  end, ListVars ) ]),
     	      NewGoal = list_to_tuple([ ?TEMP_SHELL_AIM  |lists:map(fun({ Normal, Temp})->  Normal  end, ListVars ) ]),
     	      ProtoType = list_to_tuple( lists:map(fun({ Normal, Temp})->  Normal  end, ListVars )  ), 
              TempProto   =  lists:map(fun({ Normal, Temp})->  Temp  end, ListVars ),          	      
     	      ets:insert(?RULES,{?TEMP_SHELL_AIM, ProtoType, Goal }  ),
	      ?DEBUG("TempAim : ~p~n", [{TempAim1, NewGoal, Goal } ]),
              StartTime = erlang:now(), 
	      BackPid = spawn_link(prolog, conv3, [list_to_tuple(TempProto), TempAim1,  dict:new(),
			      erlang:self(), now(), tree_processes]),
	      process_prove(TempAim1, NewGoal, BackPid, StartTime ),	       
	      ets:delete(tree_processes),%%%this
	      ets:delete(?RULES, ?TEMP_SHELL_AIM ),
	      server_loop(P0, TraceOn);
	{ok,Goal = {',', _Rule, _Body} } ->
	      io:fwrite("Goal complex: ~p~n", [Goal]),
%%TODO  process body if we use  complain request
%% solution make temp aim with updated variables
	      {TempAim, Dict} =  make_temp_complex_aim(Goal, dict:new()), 
	      ets:new(tree_processes,[ public, set, named_table ] ), 
	      ets:insert(tree_processes, {?PREFIX, P0 } ),
	      prolog_trace:trace_on(TraceOn, tree_processes ),
      	      ?DEV_DEBUG("trace config : ~p~n", [ {ets:tab2list(tree_processes) } ]),

	      ListVars  = dict:to_list(Dict),
	      TempAim1 = list_to_tuple([ ?TEMP_SHELL_AIM  |lists:map(fun({ Normal, Temp})->  Temp  end, ListVars ) ]),
     	      NewGoal = list_to_tuple([ ?TEMP_SHELL_AIM  |lists:map(fun({ Normal, Temp})->  Normal  end, ListVars ) ]),
     	      ProtoType = list_to_tuple( lists:map(fun({ Normal, Temp})->  Normal  end, ListVars )  ), 
              TempProto   =  lists:map(fun({ Normal, Temp})->  Temp  end, ListVars ),          	      
     	      ets:insert(?RULES,{?TEMP_SHELL_AIM, ProtoType, Goal }  ),
	      ?DEBUG("TempAim : ~p~n", [{TempAim1, NewGoal, Goal } ]),
              StartTime = erlang:now(), 
              
	      BackPid = spawn_link(prolog, conv3, [list_to_tuple(TempProto), TempAim1,  dict:new(),
			      erlang:self(), now(), tree_processes]),
	      process_prove(TempAim1, NewGoal, BackPid, StartTime ),	       
	      ets:delete(tree_processes),%%%this
	      ets:delete(?RULES,?TEMP_SHELL_AIM ),
	      server_loop(P0, TraceOn);
% 	    shell_prove_result(P0({prove,Goal}));	
	{ok,Goal} ->
	      io:fwrite("Goal : ~p~n", [Goal]),
%%TODO  process body if we use  complain request
%% solution make temp aim with updated variables
	      {TempAim, _ShellContext }=  make_temp_aim(Goal), 
	      ?DEBUG("TempAim : ~p~n", [TempAim]),
	      ets:new(tree_processes,[ public, set, named_table ] ), 
	      ets:insert(tree_processes, {?PREFIX, P0 } ),%%tell   prefix of rules to  the system 
	      prolog_trace:trace_on(TraceOn,tree_processes ),
	      ?DEBUG("~p make temp aim ~p ~n",[ {?MODULE,?LINE}, {TempAim,ets:tab2list(tree_processes)}]),
	      [_UName|Proto] = tuple_to_list(TempAim),
	      StartTime = erlang:now(),
	      BackPid = spawn_link(prolog, conv3, [list_to_tuple(Proto), TempAim,  dict:new(), 
				    erlang:self(), now() , tree_processes]),
	      process_prove(TempAim, Goal, BackPid, StartTime ),
	      ets:delete(tree_processes),%%%this is very bad design or solution
	      server_loop(P0, TraceOn);
% 	    shell_prove_result(P0({prove,Goal}));
	{error,P = {_, Em, E }} ->
	    io:fwrite("Error: ~p~n", [P]),
	    server_loop(P0, TraceOn)
    end.

    
    
process_prove_erws(TempAim , Goal, BackPid, WebPid,  StartTime)->
      ProtoType = common:my_delete_element(1, Goal),
      receive 
	   
	    {'EXIT',FromPid,Reason}->
		  ?DEBUG(" ~p exit aim ~p~n",[?LINE,FromPid]),
		  MainRes = io_lib:format("No<br/>",[]),
		  FinishTime = erlang:now(),
		  ElapsedTime = time_string(FinishTime, StartTime ), 
		  Main =  concat_result( [MainRes,ElapsedTime] ),
		  ?LOG("~p send back restul to web console ~p",[{?MODULE,?LINE},{Main, WebPid}]),
		  WebPid ! {result, Main, finish, self() },
		  finish_web_session();
		
	    finish ->
		   MainRes = io_lib:format("No<br/>",[]),FinishTime = erlang:now(),
    		   ElapsedTime = time_string(FinishTime, StartTime),
    		   Main =  concat_result( [MainRes,ElapsedTime] ),
		   WebPid ! {result, Main, finish, self() },
		   finish_web_session();

	    {result, {false, _ } }->
		   MainRes = io_lib:format("No<br/>",[]),FinishTime = erlang:now(),
     		  ElapsedTime = time_string(FinishTime, StartTime),
     		   Main =  concat_result( [MainRes,ElapsedTime] ),
		   WebPid ! {result, Main, finish, self() },
		   finish_web_session();

	    {result, {empty, _ } }->
		   MainRes = io_lib:format("No<br/>",[]),FinishTime = erlang:now(),
    		  ElapsedTime = time_string(FinishTime, StartTime),
    		   Main =  concat_result( [MainRes,ElapsedTime] ),
		   WebPid ! {result, Main, finish, self() },
		   finish_web_session();

	    {result, {Result, SomeContext} }->
   		   ?DEBUG("~p got from prolog shell aim ~p~n",[?LINE, {Result,  ProtoType, SomeContext} ]),
		  FinishTime = erlang:now(),
		  NewLocalContext = prolog:fill_context( 
					Result, 
					ProtoType,
					dict:new() ), 
					
		  ?DEBUG("~p got from prolog shell aim ~p~n",[?LINE, {WebPid,Result,  ProtoType, NewLocalContext} ]),
		  VarsRes = lists:map(fun shell_var_match_str/1, dict:to_list(NewLocalContext) ),
		  ElapsedTime = time_string(FinishTime, StartTime),
  		  ResStr = io_lib:format("<br/>Yes looking next ?",[] ),
		  Main =  concat_result( [VarsRes, ResStr, ElapsedTime] ),
		  
		  WebPid ! {result, Main, has_next, self() },
 		  receive 
			{Line, NewWebPid} ->
			  case Line of
			      finish ->
				    VarRes = io_lib:format("Yes~n",[]),
				    WebPid ! {result, concat_result(VarRes), finish, self() },
				    exit(BackPid, kill),
				    finish_web_session();
			      _ ->
				    ?DEBUG("~p send next to pid ~p",[{?MODULE,?LINE}, BackPid]),
				    BackPid ! next,
				    process_prove_erws( TempAim , Goal, BackPid, NewWebPid, erlang:now() )		    
			  end
		end
      end     
.

finish_web_session()->
    true.

concat_result(List)->
    list_to_binary(lists:flatten(List)).


shell_var_match_str({ { Key }, Val} ) when is_tuple(Val)->
        shell_var_match_str({ { Key },  io_lib:format("~p",[Val]) } );
shell_var_match_str({ { Key }, Val} ) when is_float(Val)->
        shell_var_match_str({ { Key }, float_to_list(Val) } );
shell_var_match_str({ { Key }, Val} ) when is_integer(Val)->
	shell_var_match_str({ { Key }, integer_to_list(Val) } );
shell_var_match_str({ { Key }, Val} ) when is_binary(Val) -> 
			    shell_var_match_str({ { Key }, unicode:characters_to_list(Val) } );
			    
shell_var_match_str({ { Key }, []} )-> 
    io_lib:format("<strong>~p</strong> = nothins ~n", [Key ])
;
shell_var_match_str({ { Key }, Val} )-> 

    case shell_check(Val) of
	true ->
	    ?DEBUG("~p fill shell vars ~p",[{?MODULE,?LINE}, {  Key , Val} ]),
	    io_lib:format("<strong>~p</strong> = ~ts ~n", [Key,Val]);
	false->
	    io_lib:format("<strong>~p</strong> = ~p ~n", [Key,Val])
    end; 
    
shell_var_match_str( V )->
    "".    
shell_check([])->
    true;
shell_check([Head|Tail]) when Head<20->
    false
;
shell_check([Head|Tail]) when is_integer(Head)->
    shell_check(Tail)
;

shell_check(_L)->
    false
.

process_prove(  TempAim , Goal, BackPid, StartTime)->
      ProtoType = common:my_delete_element(1, Goal),
      receive 
	    {'EXIT',FromPid,Reason}->
		  ?DEBUG("~p exit aim ~p~n",[{?MODULE,?LINE}, {FromPid,Reason} ]),
		  io:fwrite("No~n"),
		  FinishTime = erlang:now(),
		  io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] );
	    finish ->
		   io:fwrite("No~n"),FinishTime = erlang:now(),
    		   io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] );

	    {result, {false, _ } }->
		   io:fwrite("No~n"),FinishTime = erlang:now(),
     		   io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] );

	    {result, {empty, _ } }->
		   io:fwrite("No~n"),FinishTime = erlang:now(),
    		   io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] );

	    {result, {Result, SomeContext} }->
   		   ?DEBUG("~p got from prolog shell aim ~p~n",[?LINE ,{Result,  ProtoType, dict:to_list(SomeContext)} ]),
		  FinishTime = erlang:now(),
		  NewLocalContext = prolog:fill_context( 
					Result, 
					ProtoType,
					dict:new() ), 
					
		  ?DEBUG("~p got from prolog shell aim ~p~n",[?LINE, {Result,  ProtoType, NewLocalContext} ]),
		  lists:foreach(fun shell_var_match/1, dict:to_list(NewLocalContext) ),
		  ?SYSTEM_STAT(tree_processes),
  		  io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] ),
 		  Line = io:get_line(': '),
		  case string:chr(Line, $;) of
		      0 ->
			io:fwrite("Yes~n"),
			BackPid ! finish,
			receive 
			    {'EXIT',BackPid,Reason}->
				io:fwrite("aim process calculation has finished ~p ~n",[BackPid])
			end,
			finish;
		      _ ->
			?DEBUG("~p send next to pid ~p",[{?MODULE,?LINE}, BackPid]),
		        BackPid ! next,
			process_prove( TempAim , Goal, BackPid, erlang:now() )		    
		  end    		  
      end     
.



shell_var_match({ { Key }, Val} ) when is_binary( Val )->
			    io:fwrite("~p = ~p~n", [Key, binary_to_list(Val)]);
shell_var_match({ { Key }, Val} )->
			    io:fwrite("~p = ~p~n", [Key,Val]);
shell_var_match( V )->
    next.

% {asserta, {':-',{add,[{'X'}|{'R1'}],{'R2'},[{'X'}|{'R3'}]},
%                       {add,{'R1'},{'R2'},{'R3'}}} }    
make_temp_aim(Goal = {assert, _})->
   {Goal, dict:new()}
;
make_temp_aim(Goal = {asserta, _})->
   {Goal, dict:new()}
;
make_temp_aim(Goal = {assertz, _})->
   {Goal, dict:new()}
;
make_temp_aim(Goal = {retract, _})->
   {Goal, dict:new()}
;
make_temp_aim(Goal)->
    Name = erlang:element(1, Goal ),
    ProtoType = common:my_delete_element(1, Goal),      
    { NewGoal, NewContext } = prolog:proc_vars_hack(ProtoType),
    {erlang:list_to_tuple( [ Name | tuple_to_list( NewGoal ) ] ), NewContext}
.
make_temp_complex_aim( Tree = {';', Goal, Body},  Context )->
      Name = erlang:element(1, Goal ),
      ProtoType = common:my_delete_element(1, Goal),  
      { NewGoal, NewContext } =   prolog:proc_vars_hack(ProtoType, Context ),
      NewAim = erlang:list_to_tuple( [ Name | tuple_to_list( NewGoal ) ] ),
      { NewBody, NewContext2 } = make_temp_complex_aim(Body, NewContext),
      {  { ';',NewAim, NewBody   }, NewContext2 }
; 
make_temp_complex_aim( Tree = {',', Goal, Body},  Context )->
      Name = erlang:element(1, Goal ),
      ProtoType = common:my_delete_element(1, Goal),  
      { NewGoal, NewContext } =   prolog:proc_vars_hack(ProtoType, Context ),
      NewAim = erlang:list_to_tuple( [ Name | tuple_to_list( NewGoal ) ] ),
      { NewBody, NewContext2 } = make_temp_complex_aim(Body, NewContext),
      {  { ',',NewAim, NewBody   }, NewContext2 }
;
make_temp_complex_aim( Goal,  Context )->
    Name = erlang:element(1, Goal ),
    ProtoType = common:my_delete_element(1, Goal),      
    { NewGoal, NewContext } = prolog:proc_vars_hack(ProtoType, Context),
    NewRule = erlang:list_to_tuple( [ Name | tuple_to_list( NewGoal ) ] ),
    {NewRule, NewContext }
.
% -record(tree, {operator, prototype, body } ).
get_code_memory(Prefix)->
	   LocalFacts = ets:tab2list( common:get_logical_name(Prefix,?INNER ) ),
	   Rules = ets:tab2list(common:get_logical_name(Prefix, ?RULES) ),
	   Meta = ets:tab2list( common:get_logical_name(Prefix, ?META) ),
% 	   add_new_rule(Tree = { ':-' ,ProtoType, BodyRule}, Pos )->
% 	   ?DEBUG("~p new rule to hbase  ~p ~n",[ {?MODULE,?LINE}, Tree ] ),
%          [ Name | _ProtoType ] = tuple_to_list(ProtoType),
	     FactsCode = lists:foldl(fun( Tree, In  )->
				PrologCode =  erlog_io:write1(Tree),
				?DEBUG("  fact  ~p\n", [ PrologCode ]),
				V2 = list_to_binary( lists:flatten(PrologCode)++".\n" ),
				<<In/binary, V2/binary>>
			end, <<>>, LocalFacts  ),
	     MetaCode = lists:foldl(fun( {Name,Count,_Hash}, In  )->
				?DEBUG(" meta fact  ~p\n", [{Name,Count}]),

				LName = atom_to_list(Name),
				LCount = integer_to_list(Count),
				V2 = list_to_binary(""++LName++ " arity  - "++ LCount ++ ". \n") ,
				<<In/binary, V2/binary>>
			end, <<>>, Meta  ),

	
	    RulesCode = lists:foldl(fun( {Name, ProtoType ,Body }, In  )->
 				RestoreTree1 =  list_to_tuple( 
 						[Name|tuple_to_list(ProtoType) ] 
 					      ),
				RestoreTree = {':-', RestoreTree1, Body },
				PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
				?DEBUG(" restore code  ~p\n", [PrologCode]),
				V2 = list_to_binary( PrologCode++".\n\n\n\n" ),
				<<In/binary, V2/binary>>
			end, <<>> , Rules  ),
	   FormatedCode1 = binary:replace(RulesCode,[<<" , ">>],<<"   ,        \n">>, [ global ] ),
	   FormatedCode = binary:replace(FormatedCode1,[<<":-">>],<<" :-\n">>, [ global ] ),
	   ResBin = << "\n", MetaCode/binary, FactsCode/binary,FormatedCode/binary >>,
	   binary_to_list(ResBin).
	   
get_code_memory_html()->
	  LocalFacts = ets:tab2list(?INNER),
	  Rules = ets:tab2list(?RULES),
	  Meta = ets:tab2list(?META),

          FactsCode = lists:foldl(fun( Tree, In  )->
				PrologCode =  erlog_io:write1(Tree),
				?DEBUG("  fact  ~p~n", [ PrologCode ]),
				V2 = list_to_binary( lists:flatten(PrologCode)++".<br/>" ),
				<<In/binary, V2/binary>>
			end, <<>>, LocalFacts  ),
	   MetaCode = lists:foldl(fun( {Name,Count,_Hash}, In  )->
				?DEBUG(" meta fact  ~p~n", [{Name,Count}]),

				LName = atom_to_list(Name),
				LCount = integer_to_list(Count),
				V2 = list_to_binary("<strong>"++LName++ "</strong> arity  - "++ LCount ++ ". <br/>") ,
				<<In/binary, V2/binary>>
			end, <<>>, Meta  ),

	
	    RulesCode = lists:foldl(fun( {Name, ProtoType ,Body }, In  )->
 				RestoreTree1 =  list_to_tuple( 
 						[Name|tuple_to_list(ProtoType) ] 
 					      ),
				RestoreTree = {':-', RestoreTree1, Body },
				PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
				?DEBUG(" restore code  ~p~n", [PrologCode]),
				V2 = list_to_binary( PrologCode++".<br/><br/><br/><strong>" ),
				<<In/binary, V2/binary>>
			end, <<>> , Rules  ),
	   FormatedCode1 = binary:replace(RulesCode,[<<" , ">>],<<"&nbsp;&nbsp;&nbsp;&nbsp;<strong>,</strong><br/>">>, [ global ] ),
	   FormatedCode = binary:replace(FormatedCode1,[<<":-">>],<<"</strong>:-<br/>">>, [ global ] ),
	   ResBin = << "<br/>", MetaCode/binary, FactsCode/binary,FormatedCode/binary >>,
	   ResBin.
	   
	   
time_string(FinishTime, StartTime)->
  io_lib:format("<br/><span class='time'> elapsed time ~p secs </span><br/>", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] )
.

 
