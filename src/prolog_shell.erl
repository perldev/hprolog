%%% File    : prolog_shell.erl
%%% Author  : Bogdan Chaicka
%%% Purpose : A simple  shell.
-module(prolog_shell).

-export([start/0,start/1,server/1]).

-import(lists, [foldl/3,foreach/2]).
-compile(export_all).
-include("prolog.hrl").


start() -> start("").

api_start_anon(Prefix)->
    prolog:create_inner_structs(Prefix)
.

api_start(Prefix)->
    prolog:create_inner_structs(Prefix),
   ?INCLUDE_HBASE( Prefix )
.

start(Prefix)-> 
    inets:start(),
    crypto:start(),
    ?LOG_APPLICATION,
    case lists:member(converter_monitor, global:registered_names() ) of
	  false -> converter_monitor:start_link();
	  true  -> do_nothing
    end,

    prolog:create_inner_structs(Prefix),
    case (catch ?INCLUDE_HBASE( Prefix ) ) of
        true-> io:format("~p namespace was loaded ~n",[Prefix]);
        Res ->io:format("unable to load namespace ~p  ~n",[Res])
    end,
    server(Prefix) 
.


server(Prefix) ->
    io:fwrite("Prolog Shell V~s (abort with ^G)\n",
	      [erlang:system_info(version)]),
    TreeEts = ets:new(tree_processes,[ public, set,named_table,{ keypos, 2 } ] ),   
    ets:insert(TreeEts, {system_record,?PREFIX, Prefix}),    
    server_loop(TreeEts, ?TRACE_OFF).

%% A simple Prolog shell similar to a "normal" Prolog shell. It allows
%% user to enter goals, see resulting bindings and request next
%% solution.

server_loop(TreeEts, TraceOn) ->
    process_flag(trap_exit, true),
    case erlog_io:read('| ?- ') of
	{ok,halt} -> ok;
	{ok, trace_on}->
	      io:fwrite("trace on Yes~n"),  
	      server_loop(TreeEts, ?TRACE_ON)
	;
	{ok, nl}->
	       io:fwrite("~n"),  
	       server_loop(TreeEts, TraceOn);
	{ok, stat}->
	       io:fwrite("todo this is ~n"),  
	       server_loop(TreeEts, TraceOn);
	{ok, listing}->
	      io:fwrite("listing is ~n"),  
	      Code = get_code_memory(TreeEts),
	      io:fwrite("~s ~n", [Code]),   
	      server_loop(TreeEts, TraceOn)
	;
	{ok, trace_off}->
	    io:fwrite("trace off Yes~n"),    
	    server_loop(TreeEts, ?TRACE_OFF)
	;
	
	{ok, debug_on}->
	    io:fwrite("debug on Yes~n"),  
	    server_loop(TreeEts, ?DEBUG_ON)
	;
	{ok, debug_off}->
	    io:fwrite("debug off Yes~n"),  

	    server_loop(TreeEts, ?DEBUG_OFF)
	;
	
	{ok,Files} when is_list(Files) ->
	    lists:foreach(fun(File)->
			      case catch(prolog:compile(TreeEts, File)) of
				  ok ->
				      io:fwrite("Yes~n"),    
				      server_loop(TreeEts, TraceOn);
				  Error ->
				      io:fwrite("Error: ~p~n", [Error]),
				      server_loop(TreeEts, TraceOn)
			      end
			    end, Files)
	 ;
	 {ok, Goal = {':-',_,_ } } ->
		io:fwrite("syntax error may be you want use assert ~p ~n",[Goal]),
		server_loop(TreeEts, TraceOn);

	{ok,Goal} ->
	      io:fwrite("Goal : ~p~n", [Goal]),
%%TODO  process body if we use  complain request
%% solution make temp aim with updated variables
	       {TempAim, _ShellContext } =  make_temp_aim(Goal), 
	       ?DEBUG("TempAim : ~p~n", [TempAim]),
	      
	       prolog_trace:trace_on(TraceOn, TreeEts ),              

	       ?DEBUG("~p make temp aim ~p ~n",[ {?MODULE,?LINE}, TempAim]),
	       StartTime = erlang:now(),
	       ?START_COUNTER(TreeEts),
	       Pid =  spawn( ?MODULE, aim_spawn,[start, self(), Goal, TreeEts] ),	      
               process_prove(Pid, TempAim, Goal,  StartTime ),
% 	       clean(tree_processes),%%%[this is very bad design or solution
	       prolog:clean_tree(TreeEts),
% 	       ets:insert(TreeEts, {system_record, ?AIM_COUNTER, 0}),
	       server_loop(TreeEts, TraceOn);

	{error,P = {_, Em, E }} ->
	    io:fwrite("Error: ~p~n", [P]),
	    server_loop(TreeEts, TraceOn);
	{error,P} ->
            io:fwrite("Error during parsing: ~p~n", [P]),
            server_loop(TreeEts, TraceOn)
    end.

    
aim_spawn(start, Pid, Goal, TreeEts  )->
                Res = ( catch prolog:aim( finish, ?ROOT, Goal,  dict:new(), 1, TreeEts, ?ROOT) ),
                Pid ! Res,
                receive 
                    {next, Prev} ->
                        aim_spawn(Prev, Pid, Res, TreeEts );
                    finish ->
                        exit(normal)    
                end;
aim_spawn(Prev, Pid, Goal, TreeEts  )->
                Res = (catch prolog:next_aim(Prev, TreeEts )),
                Pid ! Res,
                receive 
                    {next, Prev} ->
                        aim_spawn(Prev, Pid, Res, TreeEts );
                    finish ->
                        exit(normal)    
                        
                end
.
                
                
                
    
process_prove(Pid,   TempAim , Goal, StartTime)->
      receive  
	     {'EXIT', FromPid, Reason}->
		  ?DEBUG("~p exit aim ~p~n",[{?MODULE,?LINE}, {FromPid,Reason} ]),
		  io:fwrite("Error~n ~p",[{Reason,FromPid}]),
		  FinishTime = erlang:now(),
		  io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] );           
            {true, SomeContext, Prev} ->
                  ?DEBUG("~p got from prolog shell aim ~p~n",[?LINE ,{TempAim, Goal, dict:to_list(SomeContext)} ]),
                  FinishTime = erlang:now(),
                  New = prolog_matching:bound_body( 
                                        Goal, 
                                        SomeContext
                                         ),
                  ?DEBUG("~p temp  shell context ~p previouse key ~p ~n",[?LINE , New, Prev ]),

                  {true, NewLocalContext} = prolog_matching:var_match(Goal, New, dict:new()),                
                                        
                  lists:foreach(fun shell_var_match/1, dict:to_list(NewLocalContext) ),
                  ?SYSTEM_STAT(tree_processes, {0,0,0}),
                  Count = ?GET_AIM_COUNT(tree_processes),
                  io:fwrite(" elapsed time ~p next solution ~p process varients ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001,Prev, Count] ),
                  Line = io:get_line(': '),
                  case string:chr(Line, $;) of
                       0 ->
                          io:fwrite("Yes~n");
                       _ ->
                         ?DEBUG("~p send next to pid ~p",[{?MODULE,?LINE}, {Pid, Goal} ]),
                         ?START_COUNTER(tree_processes),
                         Pid ! {next, Prev},
                         process_prove(Pid,  TempAim , Goal,  erlang:now() )              
                  end;        
	    Res ->
	           
		   io:fwrite("No ~p~n",[Res]),FinishTime = erlang:now(),
    		   io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] )
    		   
     end,
     exit(Pid, finish)
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
make_temp_aim(Goal) when is_atom(Goal)->
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
	   Rules = ets:tab2list(common:get_logical_name(Prefix, ?RULES) ),
	   Meta = ets:tab2list( common:get_logical_name(Prefix, ?META) ),
% 	   add_new_rule(Tree = { ':-' ,ProtoType, BodyRule}, Pos )->
% 	   ?DEBUG("~p new rule to hbase  ~p ~n",[ {?MODULE,?LINE}, Tree ] ),
%          [ Name | _ProtoType ] = tuple_to_list(ProtoType),

	     MetaCode = lists:foldl(fun( {Name,Count,_Hash}, In  )->
				?DEBUG(" meta fact  ~p\n", [{Name,Count}]),

				LName = atom_to_list(Name),
				LCount = integer_to_list(Count),
				V2 = list_to_binary("% "++LName++ " arity  - "++ LCount ++ ". \n") ,
				<<In/binary, V2/binary>>
			end, <<>>, Meta  ),
	   RulesCode = lists:foldl(fun process_inner/2, <<>> , Rules  ),
	   FormatedCode1 = binary:replace(RulesCode,[<<" , ">>],<<"   ,        \n">>, [ global ] ),
	   FormatedCode = binary:replace(FormatedCode1,[<<":-">>],<<" :-\n">>, [ global ] ),
	   ResBin = << "\n", MetaCode/binary,FormatedCode/binary >>,
	   binary_to_list(ResBin).

process_inner({Name, ProtoType ,Body }, In)->

                                RestoreTree1 =  list_to_tuple( 
                                                [Name|tuple_to_list(ProtoType) ] 
                                              ),
                                RestoreTree = {':-', RestoreTree1, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p\n", [PrologCode]),
                                V2 = list_to_binary( PrologCode++".\n\n\n\n" ),
                                <<In/binary, V2/binary>>
;
process_inner({Name, Body }, In)->
                                RestoreTree = {':-', Name, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p\n", [PrologCode]),
                                V2 = list_to_binary( PrologCode++".\n\n\n\n" ),
                                <<In/binary, V2/binary>>
.
	
get_code_memory_html(Prefix)->
	   Rules = ets:tab2list(common:get_logical_name(Prefix, ?RULES) ),
           Meta = ets:tab2list( common:get_logical_name(Prefix, ?META) ),        
	   MetaCode = lists:foldl(fun( {Name,Count,_Hash}, In  )->
				?DEBUG(" meta fact  ~p~n", [{Name,Count}]),

				LName = atom_to_list(Name),
				LCount = integer_to_list(Count),
				V2 = list_to_binary("<strong>%"++LName++ "</strong> arity  - "++ LCount ++ ". <br/>") ,
				<<In/binary, V2/binary>>
			end, <<>>, Meta  ),

	
	    RulesCode = lists:foldl(fun process_inner_html/2, <<>> , Rules  ),
	   FormatedCode1 = binary:replace(RulesCode,[<<" , ">>],<<"&nbsp;&nbsp;&nbsp;&nbsp;<strong>,</strong><br/>">>, [ global ] ),
	   FormatedCode = binary:replace(FormatedCode1,[<<":-">>],<<"</strong>:-<br/>">>, [ global ] ),
	   ResBin = << "<br/>", MetaCode/binary, FormatedCode/binary >>,
	   ResBin.
	   
	   
process_inner_html( {Name, ProtoType ,Body }, In  )->
                RestoreTree1 =  list_to_tuple( 
                                     [Name|tuple_to_list(ProtoType) ] 
                                ),
                                RestoreTree = {':-', RestoreTree1, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p~n", [PrologCode]),
                                V2 = list_to_binary( PrologCode++".<br/><br/><br/><strong>" ),
                                <<In/binary, V2/binary>>;
              
process_inner_html( {Name, Body }, In  )->
            
                                RestoreTree = {':-', Name, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p~n", [PrologCode]),
                                V2 = list_to_binary( PrologCode++".<br/><br/><br/><strong>" ),
                                <<In/binary, V2/binary>>. 
	   
 
