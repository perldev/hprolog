-module(prolog).
%%% File    : prolog.erl
%%% Author  : Bogdan Chaicka
%%% Purpose : A simple  shell.
%%This module provides prolog kernel work with parsed code before
%%% code is presented as a tree using tuples

-export([call/1,
          delete_structs/1,
          delete_inner_structs/1,
          compile/3,compile/2,
          create_inner_structs/1,
          call/2, call/3,
          start_aim_spawn/5,
          memory2hbase/2,
          memory2hbase/4,
          process_term/2,
          next_aim/2,
          next/1,
          finish/1,
	  operators/1,
	  proc_vars_hack/2,
	  proc_vars_hack/1,
	  bound_temp_and_original/2,
	  inner_change_namespace/3,
	  clean_tree/1,
	  create_inner_structs/2,
	  only_rules_create_inner/2,
          load_database_file/2,
          save_database_file/2
          ]).

%%deprecate this
-export([aim/8, aim/7]).

-include("prolog.hrl").

inner_meta_predicates(Name)->
      lists:member(Name, ?BUILTIN_PREDICATES)
.


operators( Op )->
  Table = get(?DYNAMIC_STRUCTS),
  case catch ets:lookup(Table, Op) of
      [ {Op, Status1, Status2 } ] -> 
          ?DEBUG("~p find custom operator ~p ",[{?MODULE,?LINE}, {Op, Status1, Status2 } ]),
         {yes,  Status1, Status2  };
      [ {Op, Status1, Status2, Status3 } ] -> 
          ?DEBUG("~p find custom operator ~p ",[{?MODULE,?LINE}, {Op, Status1, Status2, Status3 } ]),
          {yes,  Status1, Status2, Status3  };
      []-> no;
      _ -> no
  
  end
.


get_index(_Index)->
    erlang:make_ref().
    
clean_tree(TreeEts)->
    ets:foldl(fun(Elem, Acum)->
                case Elem of 
                      {system_record, _, _}-> Acum;
                      {system_record,_,_, _}-> Acum;
                      T = #aim_record{next = hbase}->
                          T#aim_record.solutions ! finish,
                          ets:delete(TreeEts, T#aim_record.id), Acum;
                      T = #aim_record{} ->
                          ets:delete(TreeEts, T#aim_record.id), Acum
                end
              end, true, TreeEts)
.     






        
  
    
%%compile inner memory
compile(Prefix, File)->
        compile(Prefix, File, 1).

        
%% do not delete meta info        
compile(Prefix, File, 0)->
    {ok, Terms } = erlog_io:read_file(File),
    only_rules_delete_inner(Prefix),
    lists:foldl(fun process_term_first/2, Prefix,  Terms ),
    %%TODO process aim :- 
    %%and repeat read_file
    RulesTable = common:get_logical_name(Prefix, ?RULES),
    lists:foldl(fun process_term/2, {RulesTable, 1 },  Terms ),
    ok
;

compile(Prefix, File, 1)->
    {ok, Terms } = erlog_io:read_file(File),
    delete_inner_structs(Prefix),
    lists:foldl(fun process_term_first/2, Prefix,  Terms ),
    %%TODO process aim :- 
    %%and repeat read_file
    
    RulesTable = common:get_logical_name(Prefix, ?RULES),
    lists:foldl(fun process_term/2, {RulesTable, 1 },  Terms ),
    ok
.
inner_change_namespace(false, _Name, _TreeEts)->
    false
;    
inner_change_namespace(true, Name, TreeEts)->
   %%TODO may be delete after a long time not using
   case ets:info(common:get_logical_name(Name, ?META) ) of
        undefined ->
            create_inner_structs(Name),
            ?INCLUDE_HBASE( Name );
        _ ->
            nothing
    end,
    ets:insert(TreeEts,{system_record, ?PREFIX, Name}),
    true
.
%%save namespace to file
save_database_file(Name, Path)->
        NormalName  = common:get_logical_name(Name, ?RULES),
        ets:tab2file(NormalName, Path)
.

%%load namespace from file
load_database_file(Path, Name)->
         create_inner_structs(Name),
         NormalName  = common:get_logical_name(Name, ?RULES),
         ( catch ets:delete( NormalName) ),
         Ets = only_rules_create_inner(Name, Path), %%% load from file
         ets:rename(Ets, NormalName).
          

%%delete tables
delete_structs(Prefix)->
      R1 = ( catch ets:delete(common:get_logical_name(Prefix, ?META_WEIGHTS) ) ),
      R2 = ( catch ets:delete(common:get_logical_name(Prefix, ?RULES) ) ),
      R3 = ( catch ets:delete(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS)) ),
      R4 = ( catch ets:delete(common:get_logical_name(Prefix, ?META) ) ),
      R5 = ( catch ets:delete(common:get_logical_name(Prefix, ?META_LINKS) ) ),
      R6 = ( catch ets:delete(common:get_logical_name(Prefix, ?HBASE_INDEX)) ),
      {R1,R2,R3,R4,R5,R6}
.

%%delete objects in tables
delete_inner_structs(Prefix)->
      ets:delete_all_objects(common:get_logical_name(Prefix, ?RULES) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS)),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META_WEIGHTS) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META_LINKS) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?HBASE_INDEX))
      
.

%for dynamic change cloud rules in web  console
only_rules_create_inner(Prefix)->
        Pid = erlang:whereis(auth_demon), %%in result all ets tables will be transfered to auth demon
        ets:new(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS),[named_table, set, public, { heir,Pid, Prefix } ]),
        ets:new(common:get_logical_name(Prefix, ?RULES),[named_table, bag, public, { heir,Pid, Prefix}])
.
% %%% USED
only_rules_create_inner(Prefix, FileName)->
      Pid = erlang:whereis(converter_monitor), %%in result all ets tables will be transfered to converter_monitor
      case catch ets:file2tab(FileName) of
             {ok, Ets } ->
                 ets:setopts(Ets, [ {heir, Pid, Prefix}]),
                 Ets;
             _->
                 ets:new(common:get_logical_name(Prefix, ?RULES),[named_table, bag, public, { heir,Pid, Prefix }])
     end.
    


only_rules_delete_inner(Prefix)->
        ets:delete_all_objects( common:get_logical_name(Prefix, ?RULES) ),
        ets:delete_all_objects( common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS) )
.



% Set a process as heir. The heir will inherit the table if the owner terminates. The message {'ETS-TRANSFER',tid(),FromPid,HeirData} will
%  | {heir,none}

create_inner_structs(Prefix, HeirPid)->
    put(?DYNAMIC_STRUCTS, common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS) ),
    case ets:info(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS)) of    
         undefined-> 
            ets:new(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS),[named_table, set, public, { heir,HeirPid, Prefix } ]),
            %TODO remove this
            ets:new(common:get_logical_name(Prefix, ?META_WEIGHTS),[named_table, set, public, { heir,HeirPid, Prefix }] ),
            ets:new(common:get_logical_name(Prefix, ?META),[named_table, set, public, { heir,HeirPid, Prefix }, {keypos, 2} ]),
            ets:new(common:get_logical_name(Prefix, ?RULES),[named_table, bag, public, { heir,HeirPid, Prefix }]),
            ets:new(common:get_logical_name(Prefix, ?META_LINKS),[named_table, bag, public, { heir,HeirPid, Prefix }]),
            %the hbase database is for everything
            ets:new(common:get_logical_name(Prefix, ?HBASE_INDEX), [named_table, bag, public, { heir,HeirPid, Prefix }]),
            true;
         _ -> already
         
    end
.

create_inner_structs(Prefix)->
    Pid = erlang:whereis(converter_monitor), %%in result all ets tables will be transfered to converter_monitor
    create_inner_structs(Prefix, Pid)
.

ets_code_record_process( {Name, ProtoType, Body}, In)->
       ?DEBUG("~p process ets code ~p~n",[{?MODULE,?LINE}, {Name, ProtoType, Body}  ]),  
       NormalProtoType  =  list_to_tuple( [ Name| tuple_to_list(ProtoType) ] ),
       PrologCode =  erlog_io:write1({':-',NormalProtoType, Body  }),      
       ?DEBUG("~p process ets code ~p~n",[{?MODULE,?LINE}, PrologCode  ]),  
       V2 = list_to_binary( lists:flatten(PrologCode)++ ?INNER_DELIMITER),
       <<In/binary, V2/binary >>
;
ets_code_record_process( { Name,  Body}, In)->
        ?DEBUG("~p process ets code ~p~n",[{?MODULE,?LINE}, { Name,  Body}  ]),  
       PrologCode =  erlog_io:write1({':-',Name, Body}),
       ?DEBUG("~p process ets code ~p~n",[{?MODULE,?LINE}, PrologCode  ]),  

       V2 = list_to_binary( lists:flatten(PrologCode)++ ?INNER_DELIMITER),
       <<In/binary, V2/binary >>
;
ets_code_record_process({Name}, In)->
       ?DEBUG("~p process ets code ~p~n",[{?MODULE,?LINE},{Name}  ]),  
       PrologCode =  erlog_io:write1({':-', Name, true }),
       ?DEBUG("~p process ets code ~p~n",[{?MODULE,?LINE}, PrologCode  ]),  
       V2 = list_to_binary( lists:flatten(PrologCode)++  ?INNER_DELIMITER),
       <<In/binary, V2/binary >>.



process_code_ets_key('$end_of_table', _RealRulesTable, _)->
    true;
process_code_ets_key(Key, RealRulesTable, RulesTable2)->
    List = ets:lookup(RealRulesTable, Key),
    Binary = lists:foldl(fun ets_code_record_process/2, <<>> , List),    
    NewKey = ets:next(RealRulesTable, Key),    
    ?DEBUG("~p put code  to ~p  ~p~n",[{?MODULE,?LINE}, Binary, {RulesTable2, Key}  ]),
    fact_hbase:hbase_low_put_key(RulesTable2, atom_to_list(Key),?FAMILY, ?CODE_COLUMN, binary_to_list(Binary)),
    process_code_ets_key(NewKey, RealRulesTable, RulesTable2 )
.


%%copy all from foreign namespace on foreign machine to 
%% the namespace on Host with Port and NameSpace
memory2hbase( HostFrom, Port,  NameSpace1, HostTo, ResPort, NameSpace2 )->
    throw(not_implemented)
.

%%copy all from current namespace on local machine to 
%% the namespace on Host with Port and NameSpace
memory2hbase( NameSpace1, Host, ResPort, NameSpace2 )->
         fact_hbase:delete_all_rules(NameSpace2, Host, ResPort),
         RealRulesTable = common:get_logical_name(NameSpace1, ?RULES),
         RulesTable2 = common:get_logical_name(NameSpace2, ?RULES_TABLE)
          .
%%%only rules
memory2hbase(NameSpace1, NameSpace2)->
        RealRulesTable = common:get_logical_name(NameSpace1, ?RULES),
        RulesTable2 = common:get_logical_name(NameSpace2, ?RULES_TABLE),
        fact_hbase:delete_all_rules(NameSpace2),
        FirstKey = ets:first(RealRulesTable),
        case catch  process_code_ets_key(FirstKey, RealRulesTable, RulesTable2 ) of
            {'EXIT', Reason}->
                throw(Reason);
            NewList ->
                NewList
        end        
.




process_term_first( Rule = {':-', Body }, Prefix ) ->


     ?DEBUG("~p got rule ~p~n",[{?MODULE,?LINE}, Rule  ]),    

     %     ets:insert(common:get_logical_name(Prefix, ?RULES), { Name, Body } ),

     Prefix
;
process_term_first( NothingRule, Prefix) ->

     ?DEBUG("~p unnessery rule during first step  ~p~n",[{?MODULE,?LINE}, NothingRule  ]),    
     
     Prefix

.

process_term(Rule  = {':-',Name, Body}, {RulesTable, Count}) when is_atom(Name)->
     ?DEBUG("~p got rule ~p~n",[{?MODULE,?LINE}, {Count, Rule } ]),    
     ets:insert(RulesTable, { Name, Body } ),
     {RulesTable, Count + 1}
;
process_term(Rule  = {':-',ProtoType, Body}, {RulesTable, Count})->
     ?DEBUG("~p got rule ~p~n",[{?MODULE,?LINE},  {Count, Rule }  ]),
     Name  = element(1, ProtoType),
     Args =  common:my_delete_element(1,ProtoType),
     ?DEBUG("~p compile rule ~p~n",[{?MODULE,?LINE}, Rule  ]),
     ets:insert(RulesTable, { Name, Args, Body } ),
     {RulesTable, Count +1}
;
process_term(Aim, {RulesTable, Count}) when is_atom(Aim)->
     ?DEBUG("~p compile fact ~p~n",[{?MODULE,?LINE},  {Count, Aim }  ]),
     ets:insert(RulesTable, { Aim, true } ),
     {RulesTable, Count +1}
;
process_term(Body, {RulesTable, Count} )->
     Name  = element(1, Body),
     Args =  common:my_delete_element(1,Body),
     ?DEBUG("~p compile fact ~p~n",[{?MODULE,?LINE},  {Count, Body }  ]),
     ets:insert(RulesTable, { Name, Args, true } ),
     {RulesTable, Count +1}
.



bound_list(List, Context)->
    bound_list(List, Context, [])
.
bound_list([], _Context, Res)->
	  %TODO a lot of operation you must avoid this
	    lists:reverse( Res )

;
bound_list([ Head | Tail ], Context, Res) when is_list(Tail) ->
      NewHead = prolog_matching:bound_body(Head, Context),
      bound_list(Tail , Context, [ NewHead| Res ])    
;
bound_list( [Head|Tail], Context, PreRes) -> %we know that it's not a list
	?DEV_DEBUG("~p  bounding ~p~n",[{?MODULE,?LINE}, [Head,Tail, PreRes]  ]),
	NewHead = prolog_matching:bound_body(Head, Context),
	
	NewTail = prolog_matching:bound_body(Tail, Context),
       ?DEV_DEBUG("~p finished bounding ~p~n",[{?MODULE,?LINE}, {PreRes,NewHead, NewTail }  ]),
	lists:reverse( PreRes ) ++ [ NewHead| NewTail ] 

.
hack_match_results_aim(Body, Result, Context)->
 	Name = element(1,Body ),
	NewResult = list_to_tuple( [Name |  tuple_to_list(Result) ] ),
        prolog_matching:var_match(  Body, NewResult,  Context)
.	
	

bound_vars(Search, Context) ->
%     ?DEV_DEBUG("~p bound ~p~n",[{?MODULE,?LINE}, {Search, dict:to_list(Context) }  ]),
    list_to_tuple(	lists:map( fun(E)->
			      prolog_matching:bound_body(E, Context)
			end, tuple_to_list(Search)
			)
		  )

.

%%TODO avoid this
var_generator_name(Name)->
       { list_to_atom( Name ) }
.

%% simple way for using prolog  in you application
-spec call(tuple())-> false | {true, tuple(), { pid(), reference() }  }.

call(Goal)->
   call(Goal, ?TRACE_OFF,  default).
   
-spec call(tuple(), atom() )-> false | {true, tuple(), { pid(), reference() }  }.
  
call(Goal, Tracing)->
        call(Goal, Tracing,  default).

-spec call(tuple(), atom(), atom()  )-> false | {true, tuple(), { pid(), reference() }  }.

call(Goal, Tracing,   NameSpace)->
        OperationLimit  =  application:get_env(eprolog, max_operations_limit),
        call(Goal, Tracing,   NameSpace, OperationLimit).
        
-spec call(tuple(), integer(), atom(), tuple()|undefined|integer()   )-> false | {true, tuple(), { pid(), reference() }  }.        

call(Goal, Tracing,   NameSpace, {ok, Count})->
        call(Goal, Tracing,   NameSpace, Count);
call(Goal, Tracing,   NameSpace, OperationLimit)->
          {ok, Child} =  proc_spawn(self(), Goal, Tracing,   NameSpace, OperationLimit ),
          receive 
                false ->
%                         finish({Child, undefined}), 
                        false;
                {true, Context, Prev} ->
                        ?DEBUG("~p result of  ~p ~n",[ {?MODULE,?LINE}, {Context, Prev} ]),
                        {true, Context, { erlang:pid_to_list(Child) , Prev} };
                Unexpected ->
                        finish({Child, undefined}), 
                        throw( Unexpected )          
          end
.
%%use list_to_pid for avoid problems with transport pids from one node to another
next({Child, Prev})->
         Pid = erlang:list_to_pid(Child),
         Pid ! {next, Prev},
         receive 
                false ->
                        false;
                {true, Context, Prev} ->
                        ?DEBUG("~p result of  ~p ~n",[ {?MODULE,?LINE}, {Context, Prev} ]),
                        {true, Context, {Child, Prev} };
                Unexpected ->
                        finish({Child, undefined}), 
                        throw( Unexpected )          
          end.
         

finish({Child, Prev})->
        Pid = list_to_pid(Child),
        Pid ! finish.
        


proc_spawn(Pid, Goal, Tracing,   NameSpace, Limit)->
       ?DEV_DEBUG("~p start child linked process ~p ~n",[{?MODULE,?LINE}, Goal ]),
       ChildSpec = { 
                make_ref(),
                {simple_background_process,
                 start_link, [temporary, 
                              ?MODULE,
                              start_aim_spawn,
                              [Pid, Goal, Tracing, NameSpace, Limit] ] },
                 temporary,
                 ?DEFAULT_TIMEOUT,
                 worker,
                [simple_background_process] },
       supervisor:start_child(eprolog_sup, ChildSpec)
.      


start_aim_spawn( BackPid, Goal, Tracing, NameSpace, Limit )->
         process_flag(trap_exit, true),
         ?DEBUG("~p start aim ~p~n",[{?MODULE,?LINE},Goal ]),
         TreeEts = ets:new(tree_processes,[ public, set, { keypos, 2 } ] ),   
         case NameSpace of
              default->
                 ets:insert(TreeEts, {system_record,?PREFIX, NameSpace});
              _Name ->
                 ets:insert(TreeEts, {system_record,?PREFIX, NameSpace}),
                 ets:insert(TreeEts, {system_record, hbase, NameSpace} )
         end,     
         prolog_trace:trace_on(Tracing, TreeEts),              
         aim_spawn(start, BackPid, Goal, TreeEts, Limit). 
%% must bee
aim_spawn(start, BackPid, Goal, TreeEts, Limit  )->
                Res = ( catch prolog:aim( finish, ?ROOT, Goal,  dict:new(), 1, TreeEts, ?ROOT,  Limit) ),
                BackPid ! Res,
                receive 
                    {next, NewPrev} ->
                        aim_spawn(NewPrev, BackPid, Res, TreeEts, Limit );
                    finish ->
                        ?DEBUG("~p finish signal ~n",[{?MODULE,?LINE} ]),
                        clean_tree(TreeEts), 
                        exit(normal);
                    Signal->
                          ?DEBUG("~p exit signal ~p~n",[{?MODULE,?LINE},Signal ]),
                          clean_tree(TreeEts), 
                          BackPid ! {unexpected, Signal},
                          exit(normal)
                end;
aim_spawn(Prev, BackPid, Goal, TreeEts, Limit  )->
                ?DEBUG("~p next aim ~p~n",[{?MODULE,?LINE}, {Prev,Goal} ]),
                Res = (catch prolog:next_aim(Prev, TreeEts )),
                BackPid ! Res,
                receive 
                    { next, NewPrev } ->
                        ?DEBUG("~p normal signal ~p~n",[{?MODULE,?LINE}, Prev ]),
                        aim_spawn(NewPrev, BackPid, Res, TreeEts, Limit );
                    finish ->
                        ?DEBUG("~p finish signal ~n",[{?MODULE,?LINE} ]),
                        clean_tree(TreeEts), 
                        exit(normal);
                    Signal ->
                        ?DEBUG("~p exit signal ~p~n",[{?MODULE,?LINE},Signal ]),
                        BackPid ! {unexpected, Signal},
                        clean_tree(TreeEts), 
                        exit(normal)
                end
.

%%TODO move clean tree to the supervisour
%%%just redesign to tail recursion    
aim(NextBody, PrevIndex ,  MainBody , Context, Index, TreeEts, Parent)->
    aim(NextBody, PrevIndex ,  MainBody , Context, Index, TreeEts, Parent, undefined).
    
aim(NextBody, PrevIndex ,  MainBody , Context, Index, TreeEts, Parent, undefined)->
    Call = [aim, default,  {NextBody, PrevIndex, MainBody, Context, Index, TreeEts, Parent }],
    loop(Call);
aim(NextBody, PrevIndex ,  MainBody , Context, Index, TreeEts, Parent, Limit)->    
    Call = [aim, default,  {NextBody, PrevIndex, MainBody, Context, Index, TreeEts, Parent }],
    MaxIndex = 0, 
    loop(Call, MaxIndex, Limit).

loop(Res,  Limit, Limit )->
    throw( {eprolog_exception, { max_operations_limit, Limit } });
loop([aim, Type, Params],  Index, Limit )->
    NewCall = aim( Type, Params  ),
    loop(NewCall,  Index+1, Limit);
loop(Res,  _Index, _Limit)->
     Res.
    
    
loop([aim, Type, Params] )->
    NewCall = aim( Type, Params  ),
    loop(NewCall);
loop(Res)->
     Res.
    

next_aim(Prev, TreeEts)->
   loop([ aim, next_aim, {Prev, TreeEts} ])
.
    
    
aim(finish, {Params} )->
        [ Res, true ] = Params,
        Res;
aim(conv3,  { 'finish', NewContext, PrevIndex, NewIndex, TreeEts, ?ROOT   } )->

        
        {true, NewContext, PrevIndex}
;        
aim(conv3,  { 'finish', NewContext, PrevIndex, NewIndex, TreeEts, ParentIndex   } )->

        NewParams = {ParentIndex, TreeEts, NewContext, PrevIndex},
        [aim, next_in_current_leap, NewParams]
;
aim(conv3, { { ',', Rule, Body }, Context, PrevIndex, Index, TreeEts, ParentIndex })->

        NewParams = { Body, PrevIndex , Rule, Context,  Index, TreeEts, ParentIndex},
        [aim, default, NewParams]
;
aim(conv3, { Body, Context, PrevIndex, Index, TreeEts, ParentIndex } )-> %%last rule in syntax tree 
        NewParams  = {'finish', PrevIndex, Body, Context,  Index, TreeEts, ParentIndex },
        [aim, default, NewParams ]
;       
aim(next_in_current_leap, { T , TreeEts, ThatLocalContext, Prev })->
%%we must check again cause ! inside in rule can cut another leaps
     ?DEV_DEBUG("~p process next_in_current_leap ~p",[{?MODULE,?LINE}, {T, ThatLocalContext}]),
     [NewT] = ets:lookup(TreeEts, T), 
     ets:insert(TreeEts, NewT#aim_record{ next = Prev}),
     NewIndex2 = get_index( Prev  ), 
     case  NewT of
          #aim_record{prototype = ?LAMBDA}->
                ?DEV_DEBUG("~p process next_in_current_leap lambda ~p",[{?MODULE,?LINE}, NewT]),
                NewParams = { NewT#aim_record.next_leap , ThatLocalContext, T,
                              NewIndex2, TreeEts, NewT#aim_record.parent },               
                [aim, conv3 , NewParams ];
          _->
               { NewLocalContext, BoundProtoType }  = bound_temp_and_original(ThatLocalContext, NewT),       
                ?TRACE2(NewT#aim_record.id, TreeEts, BoundProtoType,  NewLocalContext ),
                NewParams = { NewT#aim_record.next_leap , NewLocalContext, T,
                              NewIndex2, TreeEts, NewT#aim_record.parent },
                [aim, conv3 , NewParams ]  
     end     
;
              
aim(next_aim, { _ ,?ROOT, _ } )->
        false
;
aim(next_aim, {?ROOT, TreeEts})-> %% for console???
        false
; 
aim(next_aim,  {finish,_, TreeEts} )-> %%parent is not able be a finish
        throw({'EXIT',unexpected_finish,  ets:tab2list(TreeEts) } );     
        
aim(next_aim, {Index, TreeEts})-> %% for console???

         ?DEV_DEBUG("~p process aim ~p ~n",[{?MODULE,?LINE}, Index ]),
         [ Record ] = ets:lookup(TreeEts, Index),
         NewParams = {Record#aim_record.parent, Index, TreeEts},
         [aim, next_aim,  NewParams ]
;
aim(next_aim,  {Parent, Index, TreeEts} )->
      Res = ets:lookup(TreeEts, Index),  
      ?DEV_DEBUG("~p process aim ~p ~n",[{?MODULE,?LINE}, {Res, Parent, Index} ]),

      case Res of
   
        [ T = #aim_record{solutions = [], next = one }] ->%%one  it's pattern matching 
                ?DEV_DEBUG("~p got to prev aim  ~p ~n",[{?MODULE,?LINE}, T#aim_record.prev_id]),
                ets:delete(TreeEts, T#aim_record.id),%% it doesn't need us now
                NewParams = {T#aim_record.parent, T#aim_record.prev_id, TreeEts},
                [aim ,next_aim, NewParams]; %go to the prev aim
        %%TODO think a lot about overhead 
        [ T = #aim_record{next = hbase, solutions = Pid} ]-> %%one  it's pattern matching 
                 Pattern = fact_hbase:get_facts(Pid),
                 ?DEV_DEBUG("~p work pattern  ~p ~n",[{?MODULE,?LINE},  Pattern ]),
                 %%TODO you MUST AVOID THIS LINE
                 [aim, process_next_hbase, {Pattern, T, TreeEts, T#aim_record.parent}];


        [ T = #aim_record{next = one} ]-> %%one  it's pattern matching 
                Pattern =  aim_match(next_pattern, T#aim_record.solutions,  T#aim_record.temp_prototype, T#aim_record.context ),
                ?DEV_DEBUG("~p work pattern  ~p ~n",[{?MODULE,?LINE},  Pattern ]),
                %%TODO you MUST AVOID THIS LINE
                ?DEV_DEBUG("~p new head for stack pattern  ~n",[{?MODULE,?LINE} ]),
%                 T#aim_record.parent
                NewParams = { Pattern, T, TreeEts, T#aim_record.parent },
                [aim, process_next,  NewParams ];

         [ T = #aim_record{next = NextSolution} ]->
                     ?DEV_DEBUG("~p  go to  next solution  ~p ~n",[{?MODULE,?LINE},  NextSolution ]),

                     ets:insert(TreeEts, T#aim_record{ next = one}),
                     [aim, next_aim,  {T#aim_record.parent, NextSolution, TreeEts } ];  
         []->
              throw({'EXIT',unexpected_tree_leap, {[], TreeEts} } );
         Unexpected ->
              throw({'EXIT',unexpected_tree_leap, {Unexpected, TreeEts} } )
        end
;
aim(process_next_hbase, {[], T, TreeEts, Parent })->
        ets:delete( TreeEts, T#aim_record.id ),%% may be do it separate 
        NewParams = {Parent, T#aim_record.prev_id, TreeEts},
        [aim, next_aim, NewParams];
        
aim(process_next_hbase, { [Res], T, TreeEts, Parent})->
        %    Res = [{}]
        %%TODO remove this reorganization 
        NewIndex =  get_index(T#aim_record.id), % now(), %T#aim_record.id + 1,
        Name =  erlang:element(1, T#aim_record.prototype ),
        BoundProtoType = list_to_tuple( [Name|tuple_to_list( Res )] ),
        %%MUST BE TRUE
        ?DEBUG("~p bound temp aim  ~p ~n",[{?MODULE,?LINE},
                                           {Res, T#aim_record.prototype, T#aim_record.temp_prototype, BoundProtoType }]),
                                  
        %TODO mistakes there                                    
        { true, NewLocalContext } = prolog_matching:var_match( T#aim_record.prototype, BoundProtoType, T#aim_record.context),
        %%TODO remove all logs with dict:to_list functions
        ?DEBUG("~p process FACT ~p ~n",[{?MODULE,?LINE},  {
                                                            T#aim_record.prototype,
                                                            T#aim_record.temp_prototype,
                                                            T#aim_record.next_leap} ]),
        %%TODO change all conv3е to aim procedure
        %% this case is needed for saving original context of current tree leap
        ?TRACE(T#aim_record.id, TreeEts, T#aim_record.temp_prototype, NewLocalContext),
        ?TRACE2(T#aim_record.id, TreeEts, BoundProtoType, NewLocalContext),
        NewParams = { T#aim_record.next_leap , NewLocalContext, T#aim_record.id, NewIndex, TreeEts, Parent },
        [aim, conv3 , NewParams];
aim(process_next_hbase, { Unexpected, _T, TreeEts, _Parent})->
        throw({'EXIT',unexpected_tree_leap, {Unexpected, TreeEts} } );

aim(process_next, { false, T, TreeEts, Parent } )->        
        ets:delete( TreeEts, T#aim_record.id ),%% may be do it separate 
        NewParams = {Parent, T#aim_record.prev_id, TreeEts},
        [aim, next_aim, NewParams]
;
aim(process_next, { {true, NewContext, true, Tail}, T, TreeEts, Parent })->
        ets:insert(TreeEts, T#aim_record{solutions = Tail, next = one}),
        NewIndex =  get_index(T#aim_record.id), % now(), %T#aim_record.id + 1,
        { NewLocalContext, BoundProtoType } = bound_temp_and_original(NewContext, T),                                                  
        %%TODO remove all logs with dict:to_list functions
        ?DEBUG("~p process FACT ~p ~n",[{?MODULE,?LINE},  {
                                                            T#aim_record.prototype,
                                                            T#aim_record.temp_prototype,
                                                            T#aim_record.next_leap,
                                                            NewLocalContext,
                                                            BoundProtoType} ]),
        %% this case is needed for saving original context of current tree leap
        ?TRACE(T#aim_record.id, TreeEts, T#aim_record.temp_prototype, NewLocalContext),
        ?TRACE2(T#aim_record.id, TreeEts, BoundProtoType, NewLocalContext),
        NewParams = { T#aim_record.next_leap , NewLocalContext, T#aim_record.id, NewIndex, TreeEts, Parent }, 
        [aim, conv3, NewParams]       
;



aim(process_next, { { true, NewContext, NextBody, Tail }, T, TreeEts, Parent })->
         ?DEBUG("~p matching is rule ~p ~n",[{?MODULE,?LINE}, T ]),
         NewIndex = get_index(T#aim_record.id), % now() ,%T#aim_record.id + 1,         
%          ?DEBUG("~p process RULE ~p ~n",[{?MODULE,?LINE},  {dict:to_list(NewContext), NextBody} ]),
         %%save another pattterns
         ets:insert(TreeEts, T#aim_record{solutions = Tail, next = one}),
         ?TRACE(T#aim_record.id, TreeEts, T#aim_record.temp_prototype, T#aim_record.context),
         
         
         ?DEV_DEBUG("~p new head for stack pattern  ~n",[{?MODULE,?LINE} ]),
         NewParams =  { NextBody, NewContext, T#aim_record.id , NewIndex, TreeEts,  T#aim_record.id },
         [aim, conv3, NewParams]
;
aim(default, {NextBody, PrevIndex ,{ 'retract', FirstFact, SecondFact } , Context, Index, TreeEts, Parent  } ) when is_atom(FirstFact)->
        prolog_built_in:dynamic_del_link( FirstFact, SecondFact, TreeEts  ),
        ?LOG("~p del ~p  yes ~n",[ {?MODULE,?LINE}, {  FirstFact, SecondFact } ] ), 
        %%all this Previndex just for retract and !
        NewParams = {NextBody, Context, PrevIndex,  Index, TreeEts, Parent },
        [aim, conv3 , NewParams ]
;
aim(default, {NextBody, PrevIndex,  Body = {to_var, _, _}, Context, Index, TreeEts, Parent } ) ->
    %%converting atom to variable
% inner_defined_aim(NextBody, PrevIndex , Body  = { to_var, X, _  }, Context, _Index, TreeEts  ) ->
      {_, X1, X2 } = prolog_matching:bound_body( Body, Context), 
      case  prolog_built_in:inner_to_var(X1,  X2, Context ) of
         {true, NewContext } ->
             NewParams = { NextBody, NewContext, PrevIndex,  Index, TreeEts, Parent },
             [ aim, conv3, NewParams];
         false ->         
            NewParams = {Parent, PrevIndex, TreeEts },
            [ aim, next_aim, NewParams ]
      end       
;
aim(default, {NextBody, PrevIndex , {'retract', {':-', Obj,'true' }  }, Context, Index, TreeEts, Parent}) when is_tuple(Obj) ->
  NewParams =  { NextBody, PrevIndex , {'retract', Obj }, Context, Index, TreeEts, Parent },
  [aim, default, NewParams]
;
aim(default, {NextBody, PrevIndex , {'retract', {':-', Obj, Body }  }, Context, Index, TreeEts, Parent}) when is_tuple(Obj) ->
%%% TODO retraction clause rules
        true
;

%%del only link

aim(default, {NextBody, PrevIndex , {'retract', Body }, Context, Index, TreeEts, Parent})->
       %%we make just to temp aims with two patterns 
         BodyBounded = prolog_matching:bound_body(Body, Context),
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = { retract,  BodyBounded },
                      temp_prototype = { retract, BodyBounded  }, 
                      solutions = [ { retract, { {'_X_RETRACTING'} } , {',',{ 'call', {'_X_RETRACTING'} }, {'__inner_retract', {'_X_RETRACTING'}  }  } } ],
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }), 
         NewParams = { Parent, Index, TreeEts },
         [aim, next_aim, NewParams]
;
aim(default, { _NextBody, PrevIndex ,'fail', Context, Index, TreeEts, Parent  }) ->
        ?DEBUG("~p tree contain ~p ",[ {?MODULE,?LINE},{PrevIndex, Parent}]),
        ?TRACE(Index, TreeEts,  'false', Context),
       NewParams = {Parent, PrevIndex, TreeEts },
       [aim, next_aim, NewParams] %%go back
;
aim(default, { _NextBody, PrevIndex ,'false', Context, Index, TreeEts, Parent }) ->
       ?DEBUG("~p tree contain ~p ",[ {?MODULE,?LINE},{PrevIndex, Parent}]),

      ?TRACE(Index, TreeEts,  'false', Context),
      
       NewParams = {Parent, PrevIndex, TreeEts },
       
       [aim, next_aim, NewParams ]
;  
aim(default, { NextBody, PrevIndex ,'nl', Context, Index, TreeEts, Parent } ) ->
        ?NL(TreeEts),
        NewParams = { NextBody, Context, PrevIndex,  Index, TreeEts, Parent },
        [aim,conv3, NewParams]
;  
aim(default, { NextBody, PrevIndex ,'true', Context,  Index, TreeEts, Parent }) ->
       ?DEBUG("~p tree contain ~p ",[ {?MODULE,?LINE},{PrevIndex, Parent}]),

        ?TRACE(Index, TreeEts,  'true', Context),
         NewParams = {NextBody, Context, PrevIndex,  Index, TreeEts, Parent },
         [aim, conv3 , NewParams ]
;  
aim(default, { NextBody, PrevIndex ,system_stat, Context, Index, TreeEts, Parent } ) ->
        ?SYSTEM_STAT(TreeEts, {Index, PrevIndex, Parent}),
        NewParams = { NextBody, Context, PrevIndex,  Index, TreeEts, Parent },
        [aim, conv3, NewParams]
; 
aim(default, { NextBody, PrevIndex ,fact_statistic, Context, Index, TreeEts, Parent }) ->
        ?FACT_STAT(?STAT),%%TODO for web
         NewParams = { NextBody, Context, PrevIndex,  Index, TreeEts, Parent }, 
         [aim, conv3 , NewParams]
;   
aim(default, { NextBody, PrevIndex, '!', Context, Index , TreeEts, Parent })->
         ?DEBUG("~p  parent cut  ~p", [{?MODULE,?LINE}, {NextBody, PrevIndex,'!', Context, Index , TreeEts, Parent} ]),
%        hardworking procedure of cutting another  solution
         ?TRACE(Index, TreeEts, '!', Context),
         cut_parent(Parent, TreeEts),
         spawn(?MODULE, cut_all_solutions, [TreeEts, PrevIndex, Parent ]),
         ?TRACE2(Index, TreeEts, '!', Context),
         NewParams = { NextBody, Context, Parent, Index , TreeEts, Parent }, 
         [aim, conv3 , NewParams ]
;
aim(default, { NextBody, PrevIndex,  {'call', ProtoType = {':', _Module, _Call }, ApiResult},
               Context, Index , TreeEts, Parent })->
             {_, Module, CallFunc} = prolog_matching:bound_body(ProtoType, Context),      
%              ?TRACE(Index, TreeEts, {call, Module, CallFunc },  Context),
              case prolog_built_in:api_call(Module, CallFunc, ApiResult, Context) of
                {true, NewContext} ->
%              ?TRACE2(Index, TreeEts, ApiResult, NewContext),             
                        ?DEBUG("~p next body ~p ~n",[{?MODULE,?LINE},
                                        { NextBody, NewContext, PrevIndex,  Index, TreeEts, Parent } ]),
                        NewParams = { NextBody, NewContext, PrevIndex,  Index, TreeEts, Parent },
                        [aim, conv3, NewParams];
                _->
                      ?DEV_DEBUG("~p return false  ~n",[{?MODULE,?LINE}  ]),
                      aim(next_aim, {Parent, PrevIndex, TreeEts})
             end
;
% it is 'not' !!!
aim(default,{ NextBody, PrevIndex, {'\\+', X}, Context, Index , TreeEts, Parent })->
      BodyBounded = prolog_matching:bound_body(X, Context),
      ?DEV_DEBUG("~p bound aim  for \\+ from ~p ~n",[{?MODULE,?LINE}, {BodyBounded, Index, PrevIndex} ]),
       %%TODO you MUST AVOID THIS LINE
          
      case  aim(conv3 ,{BodyBounded, Context, finish, Index, TreeEts, Parent}) of
             {true, NewLocalContext, NewPrev} ->
                       ?DEV_DEBUG("~p return  true \\+ from ~p ~n",[{?MODULE,?LINE}, {BodyBounded, Index, PrevIndex} ]),
                       aim(next_aim, {Parent, PrevIndex, TreeEts});
              Res -> %%means false
                     ?DEV_DEBUG("~p return false from ~p ~n",[{?MODULE,?LINE}, {BodyBounded, Res} ]),
                       aim(conv3 ,{ NextBody, Context, PrevIndex, Index, TreeEts, Parent } )
      end 
;
%%%for such lines  Body =~  fact(2),(fact(x), (fact(x1);fact(x3)) ) 
aim(default,{ NextBody, PrevIndex, Body = {',', _ProtoType, _Second }, Context, Index, TreeEts, Parent} )->
         
        ets:insert(TreeEts,
                #aim_record{ id = Index,
                             prototype = ?LAMBDA,
                             temp_prototype = ?LAMBDA, 
                             solutions = [ {?LAMBDA, Body }],
                             next = one,
                             prev_id = PrevIndex,
                             context = Context,
                             next_leap = NextBody,
                             parent = Parent 
                      }),                      
         NewParams = { Parent, Index, TreeEts },
         [aim, next_aim, NewParams] 
;
aim(next_aim_fork_conv,{ [  {true, NewLocalContext, NewPrev}, NextBody,  TreeEts, Parent, Index] } )->
        NewIndex = get_index(Index),
        NewParams =  { NextBody, NewLocalContext, NewPrev, NewIndex, TreeEts, Parent },
        [aim, conv3 , NewParams]
;
%  {':-',
%                            {min,{'List1'},{'List2'},{'Output'}},
%                            {',',
%                             {length,{'List1'},{'N'}},
%                             {',',
%                              {length,{'List2'},{'M'}},
%                              {';',
%                               {'->',{'<',{'N'},{'M'}},{'=',{'Output'},true}},
%                               {';',
%                                {'->',
%                                 {'=:=',{'N'},{'M'}},
%                                 {'=',{'Output'},equal}},
%                                {'=',{'Output'},other}}}}}}

aim(default, {NextBody, PrevIndex , Body = { ';', {'->', Objection, SecondBody }, Other }, Context, Index, TreeEts, Parent})->
        %%we make just to temp aims with two patterns 
       
        ?TRACE(Index, TreeEts, {'->',Objection}, Context),
       
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = ?LAMBDA,
                      temp_prototype = ?LAMBDA, 
                      solutions = [ {
                                    ?LAMBDA,{',',{ once, Objection},
                                                 {',',{'__cut_solution', Index},
                                                   SecondBody }  }
                                    },
                                    {?LAMBDA, Other }
                                  ],
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),
         
         
         
%          [aim, default,{NextBody, PrevIndex , Body = { ';', {'->', Objection, SecondBody }, Other }, Context, Index, TreeEts, Parent}]
         NewParams = { Parent, Index, TreeEts},
         
         [aim, next_aim, NewParams ]
;
aim(default, {NextBody, PrevIndex , Body = {'->', Objection, SecondBody }, Context, Index, TreeEts, Parent})->
        %%we make just to temp aims with two patterns 
        %%TODO rewrite it without '!' 
        ?TRACE(Index, TreeEts, {'->',Objection}, Context),
       
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = ?LAMBDA,
                      temp_prototype = ?LAMBDA, 
                      solutions = [ {
                                    ?LAMBDA,{',',{ once, Objection},
                                                   SecondBody }  
                                    }
                                  ],
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),
         NewParams = { Parent, Index, TreeEts},
         [aim, next_aim, NewParams ]
;
aim(default, {NextBody, PrevIndex, {'__cut_solution', Head }, Context, Index, TreeEts, Parent})->
        [AimRecord] = ets:lookup(TreeEts, Head),
        ets:insert(TreeEts, AimRecord#aim_record{next = one, solutions = [] }),
        NewParams = { NextBody, Context, PrevIndex,  Index, TreeEts, Parent },
        [aim, conv3, NewParams]
        
;
aim(default, {NextBody, PrevIndex, {'__inner_retract', Body }, Context, Index, TreeEts, Parent})->
        prolog_built_in:delete_fact(TreeEts, Body, Context,?SIMPLE_HBASE_ASSERT),
        NewParams = { NextBody, Context, PrevIndex,  Index, TreeEts, Parent },
        [aim, conv3, NewParams]
;
aim(default, {NextBody, PrevIndex ,  MainBody = {'clause', Head_, Body_ }, Context, Index, TreeEts, Parent})->
        
        
         Head = prolog_matching:bound_body(Head_, Context),
         Body = prolog_matching:bound_body(Body_, Context),
         ?DEV_DEBUG("~p  make clause  ~p ~n",[{?MODULE,?LINE},  {Head, Body} ]),
         Name = element(1, Head),%%from the syntax tree get name of fact
         {TempSearch, _NewContext} = prolog_shell:make_temp_aim(MainBody),%% think about it
         RulesTable = common:get_logical_name(TreeEts, ?RULES), 
         RulesList = ets:lookup(RulesTable, Name), 
         
         ?DEV_DEBUG("~p  possible clause  ~p ~n",[{?MODULE,?LINE}, RulesList ]),

         Solutions = clause_aim_match(Head,  Body,  RulesList),
         ?DEV_DEBUG("~p  temp clauses  ~p ~n",[{?MODULE,?LINE}, Solutions ]),

         %%we make just to temp aims with two patterns 
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = MainBody,
                      temp_prototype = TempSearch, 
                      solutions = Solutions,
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),
         NewParams = {Parent, Index, TreeEts},
         [aim, next_aim,  NewParams ]
;

%%%OPTIMIZE IT  %%TODO
aim(default, {NextBody, PrevIndex , Body = {'once', ProtoType }, Context, Index, TreeEts, Parent})->
          BodyBounded = bound_aim(ProtoType, Context),
         %%we make just to temp aims with two patterns 

        ?TRACE(Index, TreeEts, Body, Context),
        ets:insert(TreeEts,
        #aim_record{ id = Index,
                      prototype = {once, BodyBounded },
                      temp_prototype = {once, BodyBounded }, 
                      solutions = [ {once, { {'_X_ONCE'} }, {',', {'call', {'_X_ONCE'} }, '!' } } ],
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),  
         NewParams = { Parent, Index, TreeEts },
         [aim, next_aim, NewParams]
;
aim(default, {NextBody, PrevIndex , Body = {';', FirstBody, SecondBody }, Context, Index, TreeEts, Parent})->
       %%we make just to temp aims with two patterns 
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = ?LAMBDA,
                      temp_prototype = ?LAMBDA, 
                      solutions = [ {?LAMBDA, FirstBody }, {?LAMBDA, SecondBody }],
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),                      
         NewParams = { Parent, Index, TreeEts },
         [aim, next_aim, NewParams] 
;

%%YES im very lazy ;)
aim(default, { NextBody, PrevIndex, {'call', ProtoType }, Context, Index, TreeEts, Parent })->    
    ?TRACE(Index, TreeEts,  {'call', ProtoType }, Context),
    BodyBounded = bound_aim(ProtoType, Context),
    ?TRACE2(Index, TreeEts,  {'call', BodyBounded }, Context),
    
    NewParams = { NextBody, PrevIndex, BodyBounded, Context, Index, TreeEts, Parent},
    [aim, default, NewParams]
;
aim(default, {NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent }) when is_atom(ProtoType)->
    
    NewParams = {NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent, ?SIMPLE_HBASE_ASSERT},
    
    [aim, user_defined_aim, NewParams ]
;
aim(default, {NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent })->
    Name = element(1,ProtoType),
    %%TODO rewrite using ets
    ?DEBUG("~p inner aim ~p ~n",[{?MODULE,?LINE}, {ProtoType, Context}  ]),
    case inner_meta_predicates(Name) of
          true ->
            ?TRACE(Index, TreeEts, bound_aim(ProtoType, Context), Context),
             Res = prolog_built_in:inner_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts),             
             NewParams = { Res, NextBody,  PrevIndex, Index, TreeEts, Parent },
             [aim, process_builtin_pred,  NewParams];
          false ->
             NewParams = { NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent, ?SIMPLE_HBASE_ASSERT},
             [aim, user_defined_aim,  NewParams ]
    end
;    
aim(process_builtin_pred,{ {false, _Context}, _NextBody, PrevIndex, _NewIndex2, TreeEts, Parent })->
          NewParams = {Parent, PrevIndex, TreeEts},
          [aim, next_aim,  NewParams ]
;
%%TODO about NewIndex we are able to get old NewIndex2, if have no such predicate like retract
%%%%TODO remove this
aim(process_builtin_pred, { {true, Context}, NextBody, PrevIndex, NewIndex2, TreeEts, Parent })->               
         %%just for calculation
        NewParams = { NextBody, Context, PrevIndex, NewIndex2, TreeEts, Parent }, 
        [aim, conv3, NewParams ];
%%WITH HBASE we do not work with facts without prototype
%%Do not allow to make rule and facts

aim(user_defined_aim, {NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent, 1}) when is_atom(ProtoType)->
           
         %% at first we will looking in inner database
         RulesTable = common:get_logical_name(TreeEts, ?RULES), 
         RuleList = ets:lookup(RulesTable, ProtoType),        
         %% pattern matching like one aim
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = ProtoType,
                      temp_prototype = ProtoType, 
                      solutions = RuleList,
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),                
         NewParams = {Parent, Index, TreeEts},
         [aim, next_aim, NewParams ]
;
aim(user_defined_aim,{NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent, 0}) when is_atom(ProtoType)->
         
         %% at first we will looking in inner database
         RulesTable = common:get_logical_name(TreeEts, ?RULES), 
         RuleList = ets:lookup(RulesTable, ProtoType),        
         %% pattern matching like one aim
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = ProtoType,
                      temp_prototype = ProtoType, 
                      solutions = RuleList,
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),
         NewParams = {Parent, Index, TreeEts},
         [aim, next_aim, NewParams ]
;
aim(user_defined_aim,{ NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent, 0} )->
         Name = element(1, ProtoType),%%from the syntax tree get name of fact
         RulesTable = common:get_logical_name(TreeEts, ?RULES), 
         RuleList = ets:lookup(RulesTable, Name),        
         BoundProtoType = bound_aim(ProtoType, Context),
         {TempSearch, _NewContext} = prolog_shell:make_temp_aim(BoundProtoType),%% think about it
 
        %%pattern matching like one aim
         ?DEBUG("~p user defined aim ~p ~n",[{?MODULE,?LINE}, { BoundProtoType, TempSearch,  NextBody } ]),
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = ProtoType,
                      temp_prototype = TempSearch, 
                      solutions = RuleList,
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),
         NewParams = {Parent, Index, TreeEts},
         [aim, next_aim,  NewParams  ]
;
aim(user_defined_aim, {NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent, 1}) ->
        Name = element(1, ProtoType),%%from the syntax tree get name of fact
        RulesTable = common:get_logical_name(TreeEts, ?RULES),         
        RuleList = ets:lookup(RulesTable, Name),
        NewParams =  {RuleList, NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent},
        [aim, hbase_user_defined_aim, NewParams]
;
aim(hbase_user_defined_aim,{[], NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent})->
        %%search fact in hbase
         BoundProtoType = bound_aim(ProtoType, Context),
         {TempSearch, _NewContext} = prolog_shell:make_temp_aim(BoundProtoType),%% think about it
         HbasePid = fact_hbase:start_fact_process( BoundProtoType, TreeEts, self() ),
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = BoundProtoType,
                      temp_prototype = TempSearch, 
                      solutions = HbasePid,
                      next = hbase,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),             
         NewParams =   {Parent, Index, TreeEts},           
         [aim, next_aim,  NewParams ]

;
aim(hbase_user_defined_aim, {RuleList, NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent} )->
         BoundProtoType = bound_aim(ProtoType, Context),
         {TempSearch, _NewContext} = prolog_shell:make_temp_aim(BoundProtoType),%% think about it
         %%pattern matching like one aim
         ?DEBUG("~p user defined aim ~p ~n",[{?MODULE,?LINE}, { BoundProtoType, TempSearch,  NextBody } ]),
         converter_monitor:stat('search',  
                                 erlang:element(1,ProtoType),
                                 common:get_logical_name( TreeEts ),
                                 ProtoType, true ),
         
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = ProtoType,
                      temp_prototype = TempSearch, 
                      solutions = RuleList,
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),   
         NewParams = {Parent, Index, TreeEts },
         [aim, next_aim,  NewParams]
;
aim(Type, Params)->
    throw({unexpected_tree_leap, { Type, Params} }).






clause_aim_match(Head, Body,  [])->
     []   
;
clause_aim_match(Head,  Body,  RulesList)->
     clause_aim_match(Head,  Body, RulesList , [] )   
.

clause_aim_match(_Head, _Body,  [], Acum )->
        lists:reverse(Acum);
clause_aim_match(Head, Body,  [ {Name, BodyRule} |RulesList], Acum )->
        % [ {?LAMBDA, FirstBody }, {?LAMBDA, SecondBody }]
       NewBody =  { clause, { Head, Body },  {',', {'=', Head, Name }, { '=', Body, BodyRule } } },
       clause_aim_match(Head, Body, RulesList, [ NewBody |Acum ] )
;
clause_aim_match(Head,  Body,  [ {Name, ProtoType, BodyRule} |RulesList], Acum )->
        NormalProtoType = list_to_tuple( [ Name|tuple_to_list(ProtoType) ] ),
        NewBody = { clause, { Head, Body }, {',', {'=', Head, NormalProtoType }, { '=', Body, BodyRule } } },
%        NewBody = { Name, ProtoType ,{ '=', Body, BodyRule } },
       clause_aim_match(Head,  Body,  RulesList, [NewBody |Acum] )    
.


aim_match({ {false, _}, _ }, [], ProtoType, _ )->
    false
;
aim_match({ {?EMPTY, _}, _ }, [], ProtoType, _ )->
    false
;
aim_match({ { false, _ }, _ }, Tail, ProtoType, Context )->
    aim_match(next_pattern, Tail, ProtoType, Context)
;
aim_match({ {Res, NewContext}, NextBody }, Tail, _ProtoType, _ )->%%finish
    {Res, NewContext, NextBody, Tail}
;
aim_match(_, [ {?LAMBDA, Body }  | Tail], ProtoType, Context )-> %%for ';' statemant
    aim_match({  {true, Context}, Body }, Tail, ProtoType, Context )
;
%%for predicates without arguments
aim_match(next_pattern, [ Aim = { Name,  Body} | Tail], Name, Context ) when is_atom(Name)->       
         aim_match( { { true, dict:new() }, Body }, Tail, Name, Context );
aim_match(next_pattern, [ Aim = { Name, Args, Body} | Tail], Name, Context ) when is_atom(Name)->       
         aim_match( { { false, 1 }, 2 }, Tail, Name, Context );         
aim_match(next_pattern, [ Aim = { Name, Body } | Tail], ProtoType, Context ) when is_tuple(ProtoType)->       
         aim_match( { { false, 1 }, 2 }, Tail, ProtoType, Context );     
aim_match(next_pattern, [ Aim = { Name, Args, Body} | Tail], ProtoType,Context )->       
         CompareProtoType = list_to_tuple( [ Name | tuple_to_list(Args) ] ), %%HACK avoid this 
         ?DEV_DEBUG("~p match ~p ~n",[{?MODULE,?LINE}, { CompareProtoType, ProtoType } ]),
         Res =  prolog_matching:var_match(CompareProtoType, ProtoType, dict:new() ),
         ?DEV_DEBUG("~p result of matching  ~p ~n",[{?MODULE,?LINE}, {Res, CompareProtoType, ProtoType } ]),
         aim_match({ Res, Body }, Tail, ProtoType, Context ).%%finish


% timer:tc(?MODULE, 'next', [ Rule, Context , Index, TreeEts ] ),

bound_temp_and_original(Context, T)->
      ?DEBUG("~p bound temp and original aims  ~p ~n",[{?MODULE,?LINE},  T  ]),
      BoundProtoType =  bound_aim(T#aim_record.temp_prototype, Context),
      ?DEBUG("~p bound temp aim  ~p ~n",[{?MODULE,?LINE},
                {T#aim_record.prototype, T#aim_record.temp_prototype, BoundProtoType }]),
      { true, NewContext} = prolog_matching:var_match(T#aim_record.prototype, BoundProtoType,T#aim_record.context),
      { NewContext, BoundProtoType}  
.


        









% {':-',
%                           {maxvector6,{'M'},{'V'}},
%                           {',',
%                            {mas,{'M'},{'Ii'},{'V'}},
%                            {',',
%                             {retract,{m,{'X'}}},
%                             {',',
%                              {';',
%                               {',',{'>',{'V'},{'X'}},{assert,{m,{'V'}}}},
%                               {',',{'=<',{'V'},{'X'}},{assert,{m,{'X'}}}} },
%
%                              false
%                              }
%                             }  
%                           }
%               }

%%working with our tree
% Goal complex: {',',{fact1,2,4,5},
%                    {',','!',
%                         {',',{'not',{add,{'X'},{'Y'}}},{pro,{'T'},{'Y'}}}}}


% {';',{',',{fact,1},
%      {',',{fact1,1},{is,{'X'},{'+',1,1} } } },
%      {';',{',',{fact2,{'X'}},
%                       {',',{is,{'X'},{'*',{'X'},{'Y'}}},{fact1,{'Y'}} } },
%                  {fact,6} }
% }

% Goal : {';',{',',{fact,1},{',',{fact1,1},{is,{'X'},{'+',1,1}}}},'!'}
% {';',{fact,1},
%      {';',{',',{ fact2,{'X'} },
%           {',',{is,{'X'},{'*',{'X'},{'Y'}}},{fact1,{'Y'}}}},
%
%                  {fact,6}}}




 
%%% make temp aims for prolog shell

bound_list_hack(List, Context)->
    bound_list_hack(List, Context, [])
.
bound_list_hack([], Context, Res)->
    {  lists:reverse( Res ) , Context };
    
bound_list_hack([ Head | Tail ], Context, Res) when is_list(Tail)->
     case prolog_matching:is_var(Head) of
	  positive ->	
		      case  prolog_matching:find_var(Context, Head) of
			nothing ->   
				NewSearchVar = process_var_hack(Head),
				bound_list_hack(Tail , 
						prolog_matching:store_var( {Head, NewSearchVar } , Context ),
						[ NewSearchVar| Res] 
						);
		       Var ->   bound_list_hack(Tail ,  Context , [ Var| Res] )
		     end;
	  _ ->
	  
		      {NewHead, NewContext} = bound_struct_hack(Head, Context),
		      bound_list_hack(Tail , NewContext, [ NewHead  | Res ])
    end    
;
bound_list_hack([ Head | Tail ], Context, Res) ->
	{NewHead , NewContext} =
	case  prolog_matching:find_var(Context, Head) of
	    nothing ->
		NewVar = process_var_hack(Head),
		{NewVar, prolog_matching:store_var( {Head, NewVar }, Context ) };
		  %%%in  case like this [1|2] flatten will fall
	    Var ->  
		{ Var , Context   }
	end,	
	{NewTail , NewContext2} = 
	case  prolog_matching:find_var(NewContext, Tail) of
	    nothing ->
		NewVar1 = process_var_hack(Tail),
		%%%in  case like this [2|2] flatten will fall
		{ NewVar1, prolog_matching:store_var( {Tail, NewVar1 }, NewContext ) };
	    Var1 ->  
		{ Var1 , NewContext   }
	end,
	{   lists:reverse(  Res  ) ++ [NewHead | NewTail ], NewContext2  } %%%%BLYAAAA	
.


proc_vars_hack(Search, Context )->
       bound_vars_hack( tuple_to_list(Search), [], Context )
.

proc_vars_hack(Search )->
       bound_vars_hack( tuple_to_list(Search), [], dict:new() )
.


bound_vars_hack([], Res, Context )->
    { list_to_tuple( lists:reverse( Res) ), Context }
;
bound_vars_hack([{'_'}| List], Res, Context )->
    {NewHead, NewContext} = bound_struct_hack({'_'}, Context),
    bound_vars_hack(List, [ NewHead| Res ] , NewContext  )
;
bound_vars_hack([Some| List], Res, Context ) when is_atom(Some)->
    bound_vars_hack(List, [Some| Res ] , Context  )
;
bound_vars_hack([Search| List], Res, Context )->
      case prolog_matching:is_var(Search) of
	    positive -> 
		    case prolog_matching:find_var(Context, Search) of %%val in search wasn't bounded
			  nothing ->  	NewSearchVar = process_var_hack(Search),
				        bound_vars_hack(List, [ NewSearchVar| Res ], 
					prolog_matching:store_var( { Search,NewSearchVar } ,Context)   );
			  Head ->       bound_vars_hack(List, [ Head| Res ] , Context  )
		    end;
	    Head ->   case is_list(Head) of %%bound list
			   true ->  { NewHead, NewContext } = bound_list_hack(Head, Context),
				    bound_vars_hack(List, [ NewHead| Res ], NewContext );
			   false ->
				    {NewHead, NewContext} = bound_struct_hack(Head, Context),
				    bound_vars_hack(List, [ NewHead| Res ] , NewContext  )
		      end
      end.
      
bound_struct_hack(Var = {'_'}, Context)->
	NewSearchVar = process_var_hack(Var),
	{NewSearchVar, Context }
;
bound_struct_hack(Var = { _ }, Context)->
	 case prolog_matching:find_var(Context, Var) of
	      nothing -> 
			NewSearchVar = process_var_hack(Var),
			NewContext =  prolog_matching:store_var( { Var, NewSearchVar } ,Context),
			{NewSearchVar, NewContext };
	      Some -> {Some, Context }
	end
;
bound_struct_hack({Operator, Var1, Var2}, Context)->
         { BoundVar2, NewContext  } = bound_struct_hack(Var2, Context),
         { BoundVar1, NewContext1 } = bound_struct_hack(Var1, NewContext),
         { {Operator, BoundVar1, BoundVar2}, NewContext1}
;
bound_struct_hack(Var, Context) when is_tuple(Var)->
	 
	 { BoundVar, NewContext }= 
	      lists:mapfoldl( fun(E, Dict)->
				    bound_struct_hack(E, Dict)
			      end , Context, 
			      tuple_to_list(Var) ),
	{ list_to_tuple(BoundVar), NewContext }
;
bound_struct_hack(Var, Context)->
	 {Var, Context} 
.


process_var_hack({Var})->
    {_Me,_Sec,Mic} = erlang:now(),
    %%TODO you must avoid this
    { list_to_atom( "_" ++ integer_to_list(Mic) ) }
.




bound_aim(ProtoType, Context)->
	BoundedSearch = prolog_matching:bound_body(ProtoType, Context ),
	BoundedSearch
.

%%%HARD solution, that hase scense only with hbase
cut_all_solutions(TreeEts, Parent, Parent )->
    true;
cut_all_solutions(TreeEts, 1, Parent )->
    true;
cut_all_solutions(TreeEts, PrevIndex, Parent )->

         %%TODO delete there delete hbase pids
%       io:format("~p delete another solution ~p~n",[{?MODULE,?LINE}, AimRecord]),
      case catch ets:lookup(TreeEts, PrevIndex) of
         [AimRecord = #aim_record{  next = hbase, solutions = HbasePid } ]->
                 exit(HbasePid, cut_solution),
                 ets:delete(TreeEts, AimRecord#aim_record.id  )
                 ;
         [AimRecord = #aim_record{  next = one } ]->
             ets:delete(TreeEts, AimRecord#aim_record.id  );
         {'EXIT',Reason }->
            ?CUT_LOG("empty tree situation during cutting solutin  ",[  ]);      
         []->
            ?CUT_LOG("STRANGE situation during cutting solutin ~p ",[ ets:tab2list(TreeEts) ]);         
         [Record] ->
             cut_all_solutions(TreeEts, Record#aim_record.next, Record#aim_record.id ),
             ets:delete(TreeEts, Record#aim_record.id),
             cut_all_solutions(TreeEts, Record#aim_record.prev_id, Parent )
      end.


      
      
cut_parent(Parent, TreeEts)->
    ?DEBUG("~p  delete ~p",[{?MODULE,?LINE}, Parent]),
    case  ets:lookup(TreeEts, Parent) of
        [AimRecord = #aim_record{prototype = ?LAMBDA } ]->
                ets:insert(TreeEts, AimRecord#aim_record{  next = one, solutions = [] }),
                cut_parent(AimRecord#aim_record.parent, TreeEts)                
                ;
        [ AimRecord ] ->       
                ets:insert(TreeEts, AimRecord#aim_record{  next = one, solutions = [] }),                
                case  ets:lookup(TreeEts, AimRecord#aim_record.parent) of
                    []-> ?DEBUG("~p nothing to delete ~p",[{?MODULE,?LINE}, AimRecord#aim_record.parent]);
                    [AimRecord1]->
                        ets:insert(TreeEts, AimRecord1#aim_record{  next = one  })                
                end;       
                
         []->
            ?DEBUG("~p nothing to delete ~p",[{?MODULE,?LINE},Parent])
    end
.

