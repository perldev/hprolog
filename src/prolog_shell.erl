%%% File    : prolog_shell.erl
%%% Author  : Bogdan Chaicka
%%% Purpose : A simple  shell.
-module(prolog_shell).

-export([start/0,start/1,server/1]).

-compile(export_all).
-include("prolog.hrl").


start() -> start("").



api_start_anon(Prefix, {heir, Heir})->
    prolog:create_inner_structs(Prefix, Heir)
;
api_start_anon(Prefix, FileName)->
    prolog:only_rules_create_inner(Prefix, FileName)
        
.


api_start(Prefix)->
    prolog:create_inner_structs(Prefix),
   ?INCLUDE_HBASE( Prefix )
.

start(Prefix)-> 
    case prolog:create_inner_structs(Prefix) of
         true->   case (catch ?INCLUDE_HBASE( Prefix ) ) of
                    true-> io:format("~p namespace was loaded ~n",[Prefix]);
                    Res ->io:format("unable to load namespace ~p  ~n",[Res])
                  end;
         already->
                    io:format("~p  names is already loaded ~n",[Prefix])
            
    end,
    server(Prefix) 
.

server(Prefix) ->
    io:fwrite("Prolog Shell V~s (abort with ^G)\n",
	      [erlang:system_info(version)]),
    server_loop(Prefix, ?TRACE_OFF).

%% A simple Prolog shell similar to a "normal" Prolog shell. It allows
%% user to enter goals, see resulting bindings and request next
%% solution.

server_loop(NameSpace, TraceOn) ->
    process_flag(trap_exit, true),
    case erlog_io:read('| ?- ') of
	{ok,halt} -> ok;
	{ok, trace_on}->
	      io:fwrite("trace on Yes~n"),  
	      server_loop(NameSpace, ?TRACE_ON);
	{ok, nl}->
	       io:fwrite("~n"),  
	       server_loop(NameSpace, TraceOn);
	{ok, stat}->
	       io:fwrite("todo this is ~n"),  
	       server_loop(NameSpace, TraceOn);
	{ok, listing}->
	      io:fwrite("listing is ~n"),  
	      Code = get_code_memory(NameSpace),
	      io:fwrite("~s ~n", [Code]),   
	      server_loop(NameSpace, TraceOn);
	{ok, trace_off}->
	    io:fwrite("trace off Yes~n"),    
	    server_loop(NameSpace, ?TRACE_OFF);
	{ok, debug_on}->
	    io:fwrite("debug on Yes~n"),  
	    server_loop(NameSpace, ?DEBUG_ON);
	{ok, debug_off}->
	    io:fwrite("debug off Yes~n"),  
	    server_loop(NameSpace, ?DEBUG_OFF);
	{ok,Files} when is_list(Files) ->
                lists:foreach(fun(File)->
                                        case catch(prolog:compile(NameSpace, File)) of
                                                ok ->
                                                io:fwrite(atom_to_list(File) ++ " Yes~n");
                                                Error ->
                                                io:fwrite(atom_to_list(File) ++ " Error: ~p~n", [Error])
                                        end
                                end, Files),
                server_loop(NameSpace, TraceOn);
	 {ok, Goal = {':-',_,_ } } ->
		io:fwrite("syntax error may be you want use assert ~p ~n",[Goal]),
		server_loop(NameSpace, TraceOn);
	{ok,Goal} ->
	       io:fwrite("Goal : ~p~n", [Goal]),                 
               StartTime = erlang:now(),
	       Pid =  prolog:call(Goal, TraceOn, NameSpace  ),      
               process_prove(Pid, Goal,  StartTime ),
% 	       clean(tree_processes),%%%[this is very bad design or solution
               Pid! finish,
	       server_loop(NameSpace, TraceOn);
	{error,P = {_, Em, E }} ->
	       io:fwrite("Error: ~p~n", [P]),
	       server_loop(NameSpace, TraceOn);
	{error,P} ->
               io:fwrite("Error during parsing: ~p~n", [P]),
               server_loop(NameSpace, TraceOn)
    end.

           
                
                
    
process_prove(Pid,    Goal, StartTime)->
      receive  
	    {'EXIT', FromPid, Reason}->
		  ?DEBUG("~p exit aim ~p~n",[{?MODULE,?LINE}, {FromPid,Reason} ]),
		  io:fwrite("Error~n ~p",[{Reason,FromPid}]),
		  FinishTime = erlang:now(),
		  io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] );           
            {true, SomeContext, Prev} ->
                  ?DEBUG("~p got from prolog shell aim ~p~n",[{?MODULE,?LINE} ,{Goal, dict:to_list(SomeContext)} ]),
                  FinishTime = erlang:now(),
                  New = prolog_matching:bound_body( Goal, SomeContext ),
                  ?DEBUG("~p temp  shell context ~p previouse key ~p ~n",[?LINE , New, Prev ]),
                  {true, NewLocalContext} = prolog_matching:var_match(Goal, New, dict:new()),                
                  lists:foreach(fun shell_var_match/1, dict:to_list(NewLocalContext) ),
                  ?SYSTEM_STAT(tree_processes, {0,0,0}),
                  io:fwrite(" elapsed time ~p next solution ~p process varients ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001,Prev] ),
                  Line = io:get_line(': '),
                  case string:chr(Line, $;) of
                       0 ->
                          io:fwrite("Yes~n");
                       _ ->
                         ?DEBUG("~p send next to pid ~p",[{?MODULE,?LINE}, {Pid, Goal, Prev} ]),
                         Pid ! {next, Prev},
                         process_prove(Pid,   Goal,  erlang:now() )              
                  end;        
	    Res ->
	           
		   io:fwrite("No ~p~n",[Res]),FinishTime = erlang:now(),
    		   io:fwrite(" elapsed time ~p ~n", [ timer:now_diff(FinishTime, StartTime)*0.000001 ] )
    		   
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

get_hbase_meta_code_html(Prefix)->

%          add_new_rule(Tree = { ':-' ,ProtoType, BodyRule}, Pos )->
%          ?DEBUG("~p new rule to hbase  ~p ~n",[ {?MODULE,?LINE}, Tree ] ),
%          [ Name | _ProtoType ] = tuple_to_list(ProtoType),
%          if non hbase it can be non existed
           case  catch ets:tab2list( common:get_logical_name(Prefix, ?META) ) of        
            {'EXIT', _ }-> <<"">>;
            Meta-> lists:foldl(fun( #meta_info{ name = Name, arity = Count,  
                                                cloud = Cloud,
                                                cloud_decomposition = Cloud_rule }, In  )->
                                ?DEBUG(" meta fact  ~p~n", [{Name, Count}]),

                                LName = atom_to_list(Name),
                                LCount = integer_to_list(Count),
                                V2 = case Cloud of
                                          ""->
                                                list_to_binary("<strong>%" ++ LName ++ "</strong>  arity  - "
                                                        ++ LCount ++ ". <br/>");
                                          _ ->
                                                list_to_binary("<strong>%"++LName++ "</strong>  arity  - "
                                                        ++ LCount ++ " existed cloud - "++ Cloud ++
                                                        ", decomoposition  " ++ 
                                                        Cloud_rule
                                                        ++" .<br/>")
                                               
                                     end,
                                <<In/binary, V2/binary>>
                        end, <<>>, Meta  )
           end
            

.

get_hbase_meta_code(Prefix)->

%          add_new_rule(Tree = { ':-' ,ProtoType, BodyRule}, Pos )->
%          ?DEBUG("~p new rule to hbase  ~p ~n",[ {?MODULE,?LINE}, Tree ] ),
%          [ Name | _ProtoType ] = tuple_to_list(ProtoType),
            %%if non hbase it can be non existed
            case catch
                ets:tab2list( common:get_logical_name(Prefix, ?META) ) of
            {'EXIT', _ }-> <<"">>;
            Meta ->
                lists:foldl(fun( #meta_info{ name = Name, arity = Count, 
                                             cloud = Cloud, 
                                             cloud_decomposition = Cloud_rule }, In  )->
                                ?DEBUG(" meta fact  ~p\n", [{Name,Count}]),

                                LName = atom_to_list(Name),
                                LCount = integer_to_list(Count),
                                V2 = case Cloud of
                                          ""->
                                                list_to_binary("% "++LName++ " arity  - "++ LCount ++ ". \n");
                                          _ ->
                                                list_to_binary("% " ++ LName ++ " arity  - " ++ 
                                                                LCount ++ " existed cloud - " ++ Cloud 
                                                                        ++", decomoposition  " ++ 
                                                                        Cloud_rule ++ "  . \n")
                                     end,
                                <<In/binary, V2/binary>>
                        end, <<>>, Meta  )
            end
            

.

% -record(tree, {operator, prototype, body } ).
get_code_memory(Prefix)->
	   Rules = ets:tab2list(common:get_logical_name(Prefix, ?RULES) ),
	   MetaCode = get_hbase_meta_code(Prefix),
	   RulesCode = lists:foldl(fun process_inner/2, <<>> , Rules  ),
	   %%simple formating
	   FormatedCode1 = binary:replace(RulesCode,[<<" , ">>],<<",\n">>, [ global ] ),
           FormatedCode2 = binary:replace(FormatedCode1,[<<",">>],<<", ">>, [ global ] ),
	   FormatedCode = binary:replace(FormatedCode2,[<<":-">>],<<":-\n">>, [ global ] ),
	   
	   ResBin = << "\n", MetaCode/binary,FormatedCode/binary >>,
	   binary_to_list(ResBin).

	   
	   
process_inner({Name, ProtoType ,Body }, In)->

                                RestoreTree1 =  list_to_tuple( 
                                                [Name|tuple_to_list(ProtoType) ] 
                                              ),
                                RestoreTree = {':-', RestoreTree1, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p\n", [PrologCode]),
                                V2 = list_to_binary( PrologCode++".\n\n" ),
                                <<In/binary, V2/binary>>
;
process_inner({Name, Body }, In)->
                                RestoreTree = {':-', Name, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p\n", [PrologCode]),
                                V2 = list_to_binary( PrologCode++".\n\n" ),
                                <<In/binary, V2/binary>>
.
	
get_code_memory_html(Prefix)->
	   Rules = ets:tab2list(common:get_logical_name(Prefix, ?RULES) ),
           MetaCode = get_hbase_meta_code_html(Prefix),
	   RulesCode = lists:foldl(fun process_inner_html/2, <<>> , Rules  ),
% 	   FormatedCode1 = binary:replace(RulesCode,[<<" , ">>],<<"&nbsp;&nbsp;&nbsp;&nbsp;<strong>,</strong><br/>">>, [ global ] ),
% 	   FormatedCode = binary:replace(FormatedCode1,[<<":-">>],<<"</strong>:-<br/>">>, [ global ] ),
	   
           %%a few formattings     
	   FormatedCode1 = binary:replace(RulesCode,[<<" , ">>],<<",<br/>">>, [ global ] ),
           FormatedCode2 = binary:replace(FormatedCode1,[<<",">>],<<", ">>, [ global ] ),
           FormatedCode = binary:replace(FormatedCode2,[<<":-">>],<<"</strong>:-<br/>">>, [ global ] ),
	   
	   ResBin = << "<br/>", MetaCode/binary,  FormatedCode/binary >>,
	   ResBin.
	   
	   
process_inner_html( {Name, ProtoType ,Body }, In  )->
                                RestoreTree1 =  list_to_tuple( 
                                     [Name|tuple_to_list(ProtoType) ] 
                                ),
                                RestoreTree = {':-', RestoreTree1, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p~n", [PrologCode]),
                                V2 = list_to_binary("<strong>" ++ PrologCode++".<br/><br/>" ),
                                <<In/binary, V2/binary>>;
              
process_inner_html( {Name, Body }, In  )->
                                RestoreTree = {':-', Name, Body },
                                PrologCode =  lists:flatten( erlog_io:write1( RestoreTree ) ),
                                ?DEBUG(" restore code  ~p~n", [PrologCode]),
                                V2 = list_to_binary( "<strong>" ++ PrologCode++".<br/><br/>" ),
                                <<In/binary, V2/binary>>. 
	   
 
