-module(prolog).
-compile(export_all).
%%% File    : prolog_shell.erl
%%% Author  : Bogdan Chaicka
%%% Purpose : A simple  shell.
%%This module provides prolog kernel work with parsed code before
%%% code is presented as a tree using tuples

-record(aim_record,
{
id, 
prototype,
temp_prototype,
solutions,
next,%%%
prev_id,
context,
next_leap,
parent

}

).

%   ets:insert(TreeEts,{ Index, BoundProtoType, TempSearch, RuleList, one, PrevIndex, Context, NextBody  }),
-include("prolog.hrl").

inner_meta_predicates(Name)->
      lists:member(Name, ?BUILTIN_PREDICATES)
.
get_index(_Index)->
    now().
    
-ifdef(USE_HBASE).
    
clean_tree(TreeEts)->
    ets:foldl(fun(Elem, Acum)->
                case Elem of 
                      {system_record, _, _}-> Acum;
                      T = #aim_record{next = hbase}->
                          T#aim_record.solutions ! finish,
                          ets:delete(TreeEts, T), Acum;
                      T = #aim_record{} ->
                        ets:delete(TreeEts, T), Acum
                end
              end, true, TreeEts)
.     

-else.

clean_tree(TreeEts)->
         ets:foldl(fun(Elem, Acum)->
 			 case Elem of
                            {system_record, _, _}-> Acum;
                            T = #aim_record{} ->
                                ets:delete(TreeEts, T), Acum
                        end
              end, true, TreeEts).
              

-endif.
        

    
    
%%compile inner memory
compile(Prefix, File)->
    {ok, Terms } = erlog_io:read_file(File),
    delete_inner_structs(Prefix),
    lists:foldl(fun process_term/2, Prefix,  Terms ),
    ok
.
inner_change_namespace(false, _Name, _TreeEts)->
    false
;    
inner_change_namespace(true, Name, TreeEts)->
   %%TODO may be delete after a long time not using
   case ets:info(common:get_logical_name(Name, ?META) ) of
        undefined ->
%             delete_structs(TreeEts),
            create_inner_structs(Name),
            ?INCLUDE_HBASE( Name );
        _ ->
           nothing_do
    end,
    ets:insert(TreeEts,{system_record, ?PREFIX, Name})
.

delete_structs(Prefix)->
      ets:delete(common:get_logical_name(Prefix, ?META_WEIGHTS) ),
      ets:delete(common:get_logical_name(Prefix, ?RULES) ),
      ets:delete(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS)),
      ets:delete(common:get_logical_name(Prefix, ?META) ),
      ets:delete(common:get_logical_name(Prefix, ?META_LINKS) ),
      ets:delete(common:get_logical_name(Prefix, ?HBASE_INDEX))
.

delete_inner_structs(Prefix)->
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META_WEIGHTS) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?RULES) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS)),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META_LINKS) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?HBASE_INDEX))
.

%  | {heir,none}
% Set a process as heir. The heir will inherit the table if the owner terminates. The message {'ETS-TRANSFER',tid(),FromPid,HeirData} will


create_inner_structs(Prefix)->
    Pid = erlang:whereis(converter_monitor), %%in result all ets tables will be transfered to converter_monitor
    ets:new(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS),[named_table, set, public, { heir,Pid, some_data } ]),
    %TODO remove this
    put(?DYNAMIC_STRUCTS, common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS) ),
    ets:new(common:get_logical_name(Prefix, ?META_WEIGHTS),[named_table, set, public, { heir,Pid, some_data }] ),
    ets:new(common:get_logical_name(Prefix, ?META),[named_table, set, public, { heir,Pid, some_data }]),
    ets:new(common:get_logical_name(Prefix, ?RULES),[named_table, bag, public, { heir,Pid, some_data }]),
    ets:new(common:get_logical_name(Prefix, ?META_LINKS),[named_table, bag, public, { heir,Pid, some_data }]),
    %the hbase database is for everything
    ets:new(common:get_logical_name(Prefix, ?HBASE_INDEX), [named_table, bag, public, { heir,Pid, some_data }])
    %%statistic is common
.



process_term(Rule  = {':-',Name, Body}, Prefix) when is_atom(Name)->
     ?DEBUG("~p got rule ~p~n",[{?MODULE,?LINE}, Rule  ]),    
     ets:insert(common:get_logical_name(Prefix, ?RULES), { Name, Body } ),
     Prefix
;
process_term(Rule  = {':-',ProtoType, Body}, Prefix)->
     ?DEBUG("~p got rule ~p~n",[{?MODULE,?LINE}, Rule  ]),
     Name  = element(1, ProtoType),
     Args =  common:my_delete_element(1,ProtoType),
     ?DEBUG("~p compile rule ~p~n",[{?MODULE,?LINE}, Rule  ]),
     ets:insert(common:get_logical_name(Prefix,?RULES), { Name, Args, Body } ),
     Prefix
;
process_term(Aim, Prefix) when is_atom(Aim)->
     ?DEBUG("~p compile fact ~p~n",[{?MODULE,?LINE}, Aim  ]),
     ets:insert(common:get_logical_name(Prefix,?RULES), { Aim, true } ),
     Prefix
;
process_term(Body, Prefix )->
     Name  = element(1, Body),
     Args =  common:my_delete_element(1,Body),
     ?DEBUG("~p compile fact ~p~n",[{?MODULE,?LINE}, Body  ]),
     ets:insert(common:get_logical_name(Prefix,?RULES), { Name, Args, true } ),
     Prefix
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%TODO avoid this
var_generator_name(Name)->
       { list_to_atom( Name ) }
.




%%this is used for prolog predicat retract
dynamic_del_link(Fact1, Fact2, TreeEts) ->
    ?WAIT("~p regis  wait del link   ~p ~n",[{?MODULE,?LINE},{Fact1, Fact2} ]),
    TableName = common:get_logical_name( TreeEts, ?META_LINKS),
    ?WAIT("~p GOT  wait del link   ~p ~n",[{?MODULE,?LINE},{Fact1, Fact2} ]),
		List = ets:lookup(TableName, Fact1 ),
		ets:delete(TableName, Fact1 ), %delete links in memory
		lists:foreach(fun(  Val = { _Fact, Elem, _Val1 } )->
				  case Elem of
				      Fact2 -> next;
				      _ ->  ets:insert(TableName, Val)    
				  end
			      end,  List  ),	  
		fact_hbase:del_link_fact(Fact1, Fact2, TreeEts)
.
dynamic_del_fact(B, TreeEts)->
      ?WAIT("~p regis  wait del    ~p ~n",[{?MODULE,?LINE},B ]),
       fact_hbase:del_fact(B, TreeEts)
.

dynamic_del_rule(Search, TreeEts)->
              %%TODO process case when after delete we have empty list of rules
              %%we must delete table and meta  from hbase 
              ?WAIT("~p regis  wait del rule    ~p ~n",[{?MODULE,?LINE},Search ]),
              RulesTable = common:get_logical_name(TreeEts, ?RULES),
	      ?WAIT("~p GOT  wait del rule    ~p ~n",[{?MODULE,?LINE},Search ]),
	      Name  = element(1, Search),
	      ProtoType  = common:my_delete_element(1, Search),
	      RuleList = ets:lookup(RulesTable, Name),
	      %%get all prototypes
	      ?DEBUG("~p got rules by ~p~n",[{?MODULE,?LINE}, {Name, RuleList, ProtoType} ]), %%find matching rules
	      {NewRuleList, _StatusOut}  = 
				lists:foldr(fun( SrcE, { In, Status } )->  
                                                    Pat = erlang:element(2, SrcE  ) ,
                                                    ?DEBUG("~p pattern compare ~p ~n",[{?MODULE,?LINE}, {Pat,ProtoType}]),
                                                    case prolog_matching:match( tuple_to_list(Pat), tuple_to_list(ProtoType),[]) of
                                                        []-> { [SrcE| In], Status};
                                                        [ Var ]-> 
                                                            case Status of
                                                                ok-> { In, deleted };
                                                                _ -> { [SrcE| In], Status }
                                                            end  
                                                    end
                                            end, {[], ok }, RuleList),
	      ets:delete(RulesTable, Name),
	      ?DEBUG("~p new rule list ~p~n",[{?MODULE,?LINE}, NewRuleList ]), %%find matching rules
	      fact_hbase:del_rule(Name, TreeEts),
	      ets:insert(RulesTable, NewRuleList),
	      lists:foreach(fun( { NameRule, Args, Body } )->
				  
				  ResProtoType = list_to_tuple( [ NameRule | tuple_to_list(Args) ] ) ,
				  Tree = {':-',ResProtoType, Body },
				  fact_hbase:add_new_rule(Tree, first, TreeEts )
			    end, NewRuleList )
.
%%used by prolog operator assert
%%TODO adding rules like this test:-write() - without arguments
dynamic_new_rule(Tree = {':-',ProtoType, Body} , first, TreeEts )->
     Name  = element(1, ProtoType),
     Args =  common:my_delete_element(1,ProtoType),
     ?LOG("~n compile new rule first ~p ~n",[ Name  ]),
     New = {Name, Args, Body },
     fact_hbase:add_new_rule(Tree, first, TreeEts),
     RulesName = common:get_logical_name( TreeEts, ?RULES),
     case ets:lookup(RulesName, Name  )  of
	  []-> ets:insert(RulesName, New );
	  List ->
	       ets:delete(RulesName, Name),%%TODO replace it
	       ets:insert(RulesName,  [ New  | List]  )
	      
      end
;
dynamic_new_rule(Tree = {':-',ProtoType, Body} , last, TreeEts )->
     Name  = element(1, ProtoType),
     Args =  common:my_delete_element(1,ProtoType),
     ?LOG("~n compile new rule last ~p ~n",[ Name  ]),
     New =  { Name, Args, Body },
     fact_hbase:add_new_rule(Tree, last, TreeEts),
     ets:insert(common:get_logical_name( TreeEts, ?RULES),  New )
;
dynamic_new_rule(SomeThing ,_ ,_)->
	io:format("error ~p  ~n",[SomeThing]).

%%operators op
add_operator({op, OrderStatus, Type, Name }, TreeEts) when is_integer(OrderStatus)->
    case OrderStatus>1200 of 
	true -> false;
	false -> case OrderStatus<0 of
		    true-> false;
		    false->
			  add_operator(OrderStatus,Type, Name, TreeEts)
		end
    end
;
add_operator(_, _)->
    false
.

add_operator(OrderStatus, yfx, Name, TreeEts  ) ->
     ets:insert(common:get_logical_name(TreeEts, ?DYNAMIC_STRUCTS), {Name, OrderStatus ,OrderStatus, OrderStatus-1  }  ),    
     true
;   
add_operator(OrderStatus, xfy, Name, TreeEts   )->  
     ets:insert(common:get_logical_name(TreeEts, ?DYNAMIC_STRUCTS), {Name, OrderStatus - 1 ,OrderStatus, OrderStatus  }  ),
     true
;
add_operator(OrderStatus, xfx, Name, TreeEts )->
     ets:insert(common:get_logical_name(TreeEts, ?DYNAMIC_STRUCTS), {Name, OrderStatus - 1 ,OrderStatus, OrderStatus-1  }  ),
     true
;
%prefix operators
add_operator(OrderStatus, fy, Name, TreeEts  )->
     ets:insert(common:get_logical_name(TreeEts,?DYNAMIC_STRUCTS), {Name, OrderStatus, OrderStatus   }  ),
     true
;
add_operator(OrderStatus, fx, Name, TreeEts ) ->
     ets:insert(common:get_logical_name(TreeEts,?DYNAMIC_STRUCTS), {Name, OrderStatus, OrderStatus - 1   }  ),
     true
;
% postfix
add_operator(OrderStatus, yf, Name, TreeEts ) ->
     ets:insert(common:get_logical_name(TreeEts,?DYNAMIC_STRUCTS), {Name, OrderStatus ,OrderStatus  }  ),
     true
;
add_operator(OrderStatus, xf, Name, TreeEts  )->
     ets:insert(common:get_logical_name(TreeEts,?DYNAMIC_STRUCTS), {Name, OrderStatus ,OrderStatus  }  ),
     true
;
add_operator(_OrderStatus, _, _Name, _TreeEts  )->
     false
.

% functors
% arg 
% {',',{fact,{'X'}},{',',{functor,{fgg,5,t},f,6},{fact,x1}}}
%%TODO 
check_functor(Some, Name, Count, Context)->
      true
.
% {',',{fact,{'X'}},{',',{arg,5, {fgg,5,t},Val},{fact,x1}}}
%%TODO 
check_arg(Some, Count, Value, Context ) ->
      true     
.

cut_all_solutions(TreeEts, Parent, Parent )->
    true;
cut_all_solutions(TreeEts, finish, Parent )->
    true;
cut_all_solutions(TreeEts, 1, Parent )->
    true;
cut_all_solutions(TreeEts, PrevIndex, Parent )->
    ?DEBUG("~p delete ~p",[{?MODULE,?LINE},PrevIndex]),
    [ AimRecord ] = ets:lookup(TreeEts, PrevIndex),
     ets:insert(TreeEts, AimRecord#aim_record{solutions=[], next=one} ),
     cut_all_solutions(TreeEts, AimRecord#aim_record.prev_id, Parent ).



%%%usual assert
inner_defined_aim(NextBody, PrevIndex ,{ 'assertz', Body = {':-', _ProtoType, _Body1 }  }, Context, Index, TreeEts  ) ->
  inner_defined_aim(NextBody, PrevIndex , { 'assert', Body   }, Context, Index, TreeEts   )
;
inner_defined_aim(NextBody, PrevIndex ,{ 'assert', Body = {':-', _ProtoType, _Body1 }  }, Context, _Index, TreeEts ) ->
      dynamic_new_rule(Body, last, TreeEts),
      {true, Context}

;
inner_defined_aim(NextBody, PrevIndex ,{ 'asserta', Body = {':-', _ProtoType, _Body1 }   }, Context, _Index, TreeEts  )->
     dynamic_new_rule(Body, first, TreeEts),
     {true,Context}
    
;


inner_defined_aim(NextBody, PrevIndex ,{ 'assertz', Body   }, Context, Index, TreeEts  ) when is_tuple(Body)->
      inner_defined_aim(NextBody, PrevIndex , { 'assert', Body   }, Context, Index, TreeEts   )
;
inner_defined_aim(NextBody, PrevIndex ,{ 'assert', Body   }, Context, _Index, TreeEts  ) when is_tuple(Body)->     
      add_fact(Context, Body, last, TreeEts, ?SIMPLE_HBASE_ASSERT)     
;
inner_defined_aim(NextBody, PrevIndex ,{ 'asserta', Body   }, Context, _Index, TreeEts  ) when is_tuple(Body)->
      add_fact(Context, Body, first, TreeEts, ?SIMPLE_HBASE_ASSERT)
;
inner_defined_aim(NextBody, PrevIndex, {'inner_retract___', Body }, Context, _index, TreeEts)->
%%only facts
%%TODO rules
     delete_fact(TreeEts, Body, Context,?SIMPLE_HBASE_ASSERT),
     %%it's alway true cause this system predicat  must be used before call(X)
     {true, Context}
; 
inner_defined_aim(NextBody, PrevIndex, Body = { 'meta', _FactName, _ParamName, Val }, Context, _Index, TreeEts  )->
    NewBody = prolog_matching:bound_body(Body, Context),
    Res = fact_hbase:meta_info(NewBody,  TreeEts),     
    {MainRes ,NewContext} = prolog_matching:var_match(Res, Val, Context),
    {MainRes ,NewContext}
;
inner_defined_aim(NextBody, PrevIndex ,{ 'functor', Body, Name, Count }, Context, _Index, _  )->
      Res  =
	  case Body of
	      {Key}->  %means var
		  case prolog_matching:find_var(Context, Body)  of
		    nothing -> false;
		    Some  when is_tuple( Some )->  %%means functor
			   check_functor(Some, Name, Count, Context );
		    _ -> false
		 end;
	      Some when is_tuple(Some)->
		      check_functor(Some, Name, Count, Context ); 
	      _->
		  false
	end,
    {Res,Context}
;
inner_defined_aim(NextBody, PrevIndex, { 'arg', Count, Body, Value }, Context, _Index, _  )->
	Res  =
	  case Body of
	      {Key}->  %means var
		  case prolog_matching:find_var(Context, Body)  of
		    nothing -> false;
		    Some  when is_tuple( Some )->  %%means functor
			   check_arg(Some, Count, Value, Context );
		    _ -> false
		 end;
	      Some when is_tuple(Some)->
		      check_arg(Some, Count, Value, Context ); 
	      _->
		  false
	end,
	{Res,Context}
	
    
    
;
inner_defined_aim(NextBody, PrevIndex ,{ atom, Body  }, Context, _Index, _  ) ->

    Res =
	case Body of
	      Body when is_atom(Body)->
		  true;
              {Key} ->
		  case prolog_matching:find_var(Context, Body)  of
		     nothing -> false;
		     Some when is_atom(Some) -> true;
		     _ ->false
		  end;
	      _ -> false
	end,
    {Res, Context} 
;
inner_defined_aim(NextBody, PrevIndex ,{ use_namespace, Name  }, Context, _Index, TreeEts  ) ->
     Body = prolog_matching:bound_body( Name, Context),
     Res  = case is_list(Body) of
                true ->   
                    TableName = Name++?META_FACTS,
                    inner_change_namespace( fact_hbase:check_exist_table(TableName), Name, TreeEts ),
                    true;
                false -> false
            end,
     {Res, Context}   
;
%%сonvert to float
inner_defined_aim(NextBody, PrevIndex ,Body = { to_float, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     case common:inner_to_float(X1B) of
                false -> {false,Context};
                R ->  
                    prolog_matching:var_match(X2B, R, Context)
     end
;
%%сonvert to integer
inner_defined_aim(NextBody, PrevIndex ,Body = { to_integer, _X1,_X2  }, Context, _Index, TreeEts  ) ->

     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     case common:inner_to_int(X1B) of
                false -> {false,Context};
                R ->  
                    prolog_matching:var_match(X2B, R, Context)
     end
;
%%сonvert to string
inner_defined_aim(NextBody, PrevIndex , Body  = { to_list, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     case common:inner_to_list(X1B) of
                false -> {false,Context};
                R ->  
                    prolog_matching:var_match(X2B, R, Context)
     end
;

inner_defined_aim(NextBody, PrevIndex ,{ create_namespace, Name  }, Context, _Index, _  ) ->
    Res = fact_hbase:create_new_namespace(Name),
    {Res, Context}
;

inner_defined_aim(NextBody, PrevIndex ,{ integer, X  }, Context, _Index, _  ) ->
    Body = prolog_matching:bound_body( X, Context),
    Res =
	case Body of
	      Body when is_integer(Body)->
		  true;
              {Key} ->
		  case prolog_matching:find_var(Context, Body)  of
		     nothing -> false;
		     Some when is_integer(Some) -> true;
		     _ ->false
		  end;
	      _ -> false
	end,
    {Res, Context}   
;
inner_defined_aim(NextBody, PrevIndex ,{ 'list', X  }, Context, _Index, _  ) ->
     Body = prolog_matching:bound_body( X, Context),

    Res =
	case Body of
	      Body when is_list(Body)->
		  true;
              {Key} ->
		  case prolog_matching:find_var(Context, Body)  of
		     nothing -> false;
		     Some when is_list(Some) -> true;
		     _ ->false
		  end;
	      _ -> false 
	end,
     {Res, Context}     
;
inner_defined_aim(NextBody, PrevIndex ,{ atomic, X  }, Context, _Index, _  ) ->
   
   Body = prolog_matching:bound_body( X, Context),
   Res =
	case Body of
	      Body when is_atom(Body)->
		  true;
	      Body when is_number(Body)->
		  true;	  
              {Key} ->
		  case prolog_matching:find_var(Context, Body)  of
		     nothing -> false;
		     Some when is_atom(Some) -> true;
    		     Some when is_integer(Some) -> true;
		     _ -> false
		  end;
	      _ -> false 
	end,
      {Res, Context}      
;
inner_defined_aim(NextBody, PrevIndex ,{ 'float', X  }, Context, _Index, _  ) ->
        Body = prolog_matching:bound_body( X, Context),
        Res =
	case Body of
	      Body when is_float(Body)->
		  true;
              {Key} ->
		  case prolog_matching:find_var(Context, Body)  of
		     nothing -> false;
		     Some when is_float(Some) -> true;
		     _ ->false
		  end;
	      _ -> false 
	end,
	{Res, Context}

    
;
inner_defined_aim(NextBody, PrevIndex ,{ 'var', X  }, Context, _Index, _  ) ->
        Body = prolog_matching:bound_body( X, Context),
        Res =
	case Body of
	  {Key} when is_atom(Key)->
% 	        ?DEBUG("~p aim var is ~p~n",[{?MODULE,?LINE}, {Body,dict:to_list(Context) } ]),

		case prolog_matching:find_var(Context, Body)  of
		     nothing -> true;
		     _ -> false
		end;
	  _ -> 
	      false
	end,
    {Res, Context}
;
inner_defined_aim(NextBody, PrevIndex ,{ 'nonvar', Body  }, Context, _Index, _  ) ->
      
      Res =
	case Body of
              {Key} ->
		  case prolog_matching:find_var(Context, Body)  of
		       nothing -> false;
		       _ -> true
		  end;
	      _ -> true
	end,
	{Res, Context}
;



%%TODO for assert adding result not only true  think about it

%%this is an asserting of links between two facts and describe its connection through prolog rules
inner_defined_aim(NextBody, PrevIndex ,{ 'assertz', SourceFact, ForeignFact,  Body = {':-', _ProtoType, _Body1 }  }, Context, Index, TreeEts  ) ->
  inner_defined_aim(NextBody, PrevIndex , { 'assert',  SourceFact, ForeignFact,  Body   }, Context, Index, TreeEts  )
;
inner_defined_aim(NextBody, PrevIndex ,{ 'assert', SourceFact,  ForeignFact, Body = {':-', ProtoType, _Body1 }  }, Context, _Index, TreeEts ) ->
      dynamic_new_rule(Body, last, TreeEts),
      RuleName = erlang:element(1, ProtoType),
      fact_hbase:add_link(SourceFact, ForeignFact, RuleName, TreeEts ),
      ets:insert(common:get_logical_name(TreeEts, ?META_LINKS), {SourceFact, ForeignFact, RuleName  }),
      {true, Context} 
;

inner_defined_aim(NextBody, PrevIndex ,{ 'assertz', SourceFact, ForeignFact,  RuleName  }, Context, Index, TreeEts  ) ->
  inner_defined_aim(NextBody, PrevIndex , { 'assert',  SourceFact, ForeignFact,  RuleName   }, Context, Index, TreeEts   )
;

inner_defined_aim(NextBody, PrevIndex ,{ 'assert', SourceFact,  ForeignFact, RuleName }, Context, _Index, TreeEts ) ->
      fact_hbase:add_link(SourceFact, ForeignFact, RuleName, TreeEts ),
      ets:insert( common:get_logical_name(TreeEts, ?META_LINKS), 
		  {SourceFact, ForeignFact, RuleName  }),
      {true, Context}
;
%%del only link
inner_defined_aim(NextBody, PrevIndex ,{ 'retract', FirstFact, SecondFact } , Context, _Index, TreeEts  ) when is_atom(FirstFact)->
	erlang:apply(?MODULE, dynamic_del_link, [ FirstFact, SecondFact, TreeEts ] ),
        ?LOG("~p del ~p  yes ~n",[ {?MODULE,?LINE}, {  FirstFact, SecondFact } ] ), 
        %%all this Previndex just for retract and !
        {true, Context}
;
%%%inner predicates of our system
inner_defined_aim(NextBody, PrevIndex ,Body = {get_char, X }, Context, _Index, TreeEts)-> 
    TempX = ?GET_CHAR(TreeEts), %%only one character
    {MainRes, NewContext} = prolog_matching:var_match( common:inner_to_atom(TempX), X, Context ),
    {MainRes, NewContext}
;
inner_defined_aim(NextBody, PrevIndex ,Body = {read, X }, Context, _Index, TreeEts)->
    TempX = ?READ(TreeEts), %%read prolog term
    case TempX of
	{ ok, Term } ->
	      Res = prolog_matching:var_match( Term, X, Context ),
	      Res;
	_ ->
	    ?WRITELN(TreeEts, "i can parse input"),
	      {false, Context}
    end
;
inner_defined_aim(NextBody, PrevIndex ,Body = { 'write', X }, Context, _Index,  TreeEts  ) ->
    X1 = prolog_matching:bound_body( X, Context),
%     ?DEBUG("~p write ~p ~n",[X, dict:to_list(Context)]),
    ?WRITE(TreeEts, X1),
    {true, Context}
;
inner_defined_aim(NextBody, PrevIndex ,Body = { 'write_unicode', X }, Context, _Index,  TreeEts  ) ->
    X1 = prolog_matching:bound_body( X, Context),
%     ?DEBUG("~p write ~p ~n",[X, dict:to_list(Context)]),
    ?WRITE_UNICODE(TreeEts, X1), %%only for local console
    {true, Context}
;

inner_defined_aim(NextBody, PrevIndex ,Body = { 'writeln', X }, Context, _Index, TreeEts  ) ->
    X1 = prolog_matching:bound_body(X, Context),
%     ?DEBUG("~p write ~p ~n",[X, dict:to_list(Context)]),
    ?WRITELN(TreeEts, X1),
    {true, Context}
;

%%new binary operator via  ор( 600, хfу, &)  :- ор( 500, fy, ~).

%%infix
    

inner_defined_aim(NextBody, PrevIndex ,Body = { op, _OrderStatus, _, Name }, Context, _Index, TreeEts  ) when is_atom(Name) ->
    Res = add_operator( Body, TreeEts ),
   {Res, Context}
;   

inner_defined_aim(NextBody, PrevIndex ,{ op, _OrderStatus, _, _Name }, Context, _Index, _TreeEts  ) ->
    {false, Context}
;  
% tree represantation  = {'==',
%                                     {'_A__459399'},
%                                     {'+',{'+',{'X'},{'Y'}},{'U'}}}
%%Arithmetic comprasion

%% X1 + Y3 is Y + X
%%{',',{ is, {'+', {'X1'},{'Y3'}} , {'+',{'Y'},{'X'} } }
%%{Accum} it means that we work only with one X = X1 + XY, but not  X+C1 = X1 + XY!!

inner_defined_aim(NextBody, PrevIndex , Body = {'date', _Ini, _Typei, _Accumi },  Context, Index, TreeEts)->
	

      {In, Type, Accum} = bound_vars( common:my_delete_element(1, Body) , Context ),
      ?DEV_DEBUG("~p call date function with ~p",[{?MODULE,?LINE},{In, Type, Accum}]),
      Res =
	case common:get_date(In,Type) of		
	  false ->
	      false;
	  Var -> 
	      case  prolog_matching:is_var(Accum )  of%%simlpe pattern matching
		      true -> { In, Type, Var };
		      Var  ->   { In, Type, Var };
		      _Nothing->  false   
	       end
	end,
      {Res, Context}      
;
inner_defined_aim(NextBody, PrevIndex , {'is',  NewVarName = { _Accum }, Expr },
		  Context, _Index , _TreeEts )->
	CalcRes = common_process_expr(Expr, Context),
	?DEBUG("~p calc expression ~p ~n",[{?MODULE,?LINE},{ NewVarName, Expr, CalcRes  } ]),
	 prolog_matching:var_match(NewVarName, CalcRes, Context)

;
% L =.. [ F | Args ],

inner_defined_aim(NextBody, PrevIndex ,{ '=..', First, Second }, Context, _Index, _TreeEts  )->
    Second = prolog_matching:bound_body(Second, Context),
    ToTerm = list_to_tuple(Second),
    prolog_matching:var_match(First, ToTerm, Context)
   
;
inner_defined_aim(NextBody, PrevIndex ,{ '=:=', First, Second }, Context, _Index, _TreeEts  )->
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res ={ compare(One,Two), Context },
    Res
;
inner_defined_aim(NextBody, PrevIndex ,{ '=', First, Second }, Context, _Index, _TreeEts  )->
    ?DEBUG("~p aim = ~p",[{?MODULE,?LINE}, { '=', First, Second } ]),
    Res = prolog_matching:var_match(First, Second, Context ),
    ?DEV_DEBUG("~p aim = ~p",[{?MODULE,?LINE}, Res ]),
    Res
;
inner_defined_aim(NextBody, PrevIndex ,{ '\\=', First, Second }, Context, _Index, _TreeEts  )->
    ?DEBUG("~p aim = ~p",[{?MODULE,?LINE}, { '\\=', First, Second } ]),
    Res = case prolog_matching:var_match(First, Second, Context ) of
	{false, _ } -> {true, Context};
	_->{false,Context}
    end,
    Res
;
inner_defined_aim(NextBody, PrevIndex ,{ '==', First, Second }, Context, _Index, _TreeEts  )->
    { Res, _ } = prolog_matching:var_match(First, Second, Context ),
    { Res, Context}
;
inner_defined_aim(NextBody, PrevIndex ,{ '>=', First, Second }, Context, _Index, _TreeEts  )->
    
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { One >= Two, Context },
    Res
;
inner_defined_aim(NextBody, PrevIndex ,{'=<', First, Second }, Context, _Index, _TreeEts  )->
	    
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { One =< Two, Context }, 
    Res
;
inner_defined_aim(NextBody, PrevIndex ,{ '>', First, Second }, Context, _Index, _TreeEts  )->
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { One > Two, Context }, 
    Res
;
inner_defined_aim(NextBody, PrevIndex ,{'<', First, Second }, Context, _Index, _TreeEts  )->
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { One < Two, Context },
    Res
;
inner_defined_aim(NextBody, PrevIndex , {'=\\=', First, Second }, Context, _Index, _TreeEts )->
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { not_compare(One ,Two), Context },
    Res.

aim(NextBody, PrevIndex , {'retract', Body }, Context, Index, TreeEts, Parent)->
       %%we make just to temp aims with two patterns 
         BodyBounded = prolog_matching:bound_body(Body, Context),
         ets:insert(TreeEts,
         #aim_record{ id = Index,
                      prototype = { retract,  BodyBounded },
                      temp_prototype = { retract, BodyBounded  }, 
                      solutions = [ { retract, { {'_X_RETRACTING'} } , {',',{call,{'_X_RETRACTING'} }, {'inner_retract___', {'_X_RETRACTING'}  }   } } ],
                      next = one,
                      prev_id = PrevIndex,
                      context = Context,
                      next_leap = NextBody,
                      parent = Parent 
                      }),                      
         next_aim(Parent, Index, TreeEts ) 
;
aim(_NextBody, _PrevIndex ,'fail', Context, _Index, TreeEts, Parent  ) ->
        false
;
aim(_NextBody, _PrevIndex ,'false', Context, _Index, TreeEts, Parent ) ->
        false
;  
aim(NextBody, PrevIndex ,'nl', Context, Index, TreeEts, Parent  ) ->
        ?NL(TreeEts),
        conv3( NextBody, Context, PrevIndex,  Index, TreeEts, Parent )
;  
aim(NextBody, PrevIndex ,'true', Context,  Index, TreeEts, Parent  ) ->
        conv3( NextBody, Context, PrevIndex,  Index, TreeEts, Parent )
;  
aim(NextBody, PrevIndex ,system_stat, Context, Index, TreeEts, Parent  ) ->
        ?SYSTEM_STAT(TreeEts, {Index, PrevIndex, Parent}),
        conv3( NextBody, Context, PrevIndex,  Index, TreeEts, Parent )
; 
aim(NextBody, PrevIndex ,fact_statistic, Context, Index, TreeEts, Parent  ) ->
        ?FACT_STAT(?STAT),%%TODO for web
        conv3( NextBody, Context, PrevIndex,  Index, TreeEts, Parent )
;    
aim(NextBody, PrevIndex,'!', Context, Index , TreeEts, Parent )->
         ?DEBUG("~p  parent cut  ~p", [{?MODULE,?LINE}, Parent ]),
         [AimRecord] = ets:lookup(TreeEts, Parent),  
         ?DEBUG("~p  cut  ~p", [{?MODULE,?LINE}, AimRecord ]),         
         ets:insert(TreeEts, AimRecord#aim_record{next = one, solutions=[] } ), 
         cut_all_solutions(TreeEts, PrevIndex, Parent ),
         %CUT all solution and current leap tell that there is a finish
         conv3( NextBody, Context, finish,  get_index(Index), TreeEts, Parent )
;
aim(NextBody, PrevIndex, Body = {',', ProtoType, Second }, Context, Index, TreeEts, Parent)->
        case aim(Second, PrevIndex, ProtoType, Context, Index, TreeEts, Parent) of
            {true, NewLocalContext, NewPrev} ->
                    conv3( NextBody, NewLocalContext, NewPrev,  get_index(Index), TreeEts, Parent );
             Res -> %%means false
                    ?DEV_DEBUG("~p return false from ~p ~n",[{?MODULE,?LINE}, {ProtoType, Index, PrevIndex, Parent} ]),
                    Res
        end
;
aim(NextBody, PrevIndex , Body = {';', FirstBody, SecondBody }, Context, Index, TreeEts, Parent)->
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
         next_aim(Parent, Index, TreeEts ) 
;
%%YES im very lazy ;)
aim(NextBody, PrevIndex, {'call', ProtoType }, Context, Index, TreeEts, Parent )->    
    BodyBounded = bound_aim(ProtoType, Context),
    aim(NextBody, PrevIndex, BodyBounded, Context, Index, TreeEts, Parent)
;
aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent ) when is_atom(ProtoType)->
    user_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent)
;
aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent )->
    Name = element(1,ProtoType),
    case inner_meta_predicates(Name) of
          true -> 
            ?TRACE(Index, TreeEts, bound_aim(ProtoType, Context), Context),
             Res = inner_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts),             
             process_builtin_pred(Res, NextBody,  PrevIndex, Index, TreeEts, Parent);
          false ->
             user_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent)
    end
.
        
process_builtin_pred({false, _Context}, _NextBody, PrevIndex, _NewIndex2, TreeEts, Parent )->
          next_aim(Parent, PrevIndex, TreeEts )
;
%%TODO about NewIndex we are able to get old NewIndex2, if have no such predicate like retract
%%TODO Index must be rewrited to auto increment
process_builtin_pred({true, Context}, NextBody, PrevIndex, NewIndex2, TreeEts, Parent )->               
        case  conv3( NextBody, Context, PrevIndex, NewIndex2, TreeEts, Parent ) of
            ProcessRes = {true, _Context, _Prev} -> 
                            ProcessRes;
                        false ->
                            ?DEV_DEBUG("~p work to previous  ~p ~n",[{?MODULE,?LINE},  PrevIndex ]),
                            next_aim(Parent, PrevIndex, TreeEts );
                         Unexpected ->
                            throw({'EXIT',unexpected_return_value_builtin_pred, {Unexpected,  TreeEts} } )
        end
.

%%WITH HBASE we do not work with facts without prototype
%%Do not allow to make rule and facts
-ifdef(USE_HBASE).


user_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent) when is_atom(ProtoType)->
         
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
         next_aim(Parent, Index, TreeEts )
;
user_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent) ->
        Name = element(1, ProtoType),%%from the syntax tree get name of fact
        RulesTable = common:get_logical_name(TreeEts, ?RULES),         
        RuleList = ets:lookup(RulesTable, Name),
        hbase_user_defined_aim(RuleList, NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent)
.
hbase_user_defined_aim([], NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent)->
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
         next_aim(Parent, Index, TreeEts )

;
hbase_user_defined_aim(RuleList, NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent)->
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
         next_aim(Parent, Index, TreeEts )
.




-else.

user_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent) when is_atom(ProtoType)->
         
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
         next_aim(Parent, Index, TreeEts )
;
user_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent )->
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
         next_aim(Parent, Index, TreeEts )
.



-endif.

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
next_aim(Index ,TreeEts)->
    [ Record ] = ets:lookup(TreeEts, Index),
    next_aim(Record#aim_record.parent, Index, TreeEts )
.
next_aim( _, finish,_  )->
        false;
next_aim( finish,_, TreeEts  )-> %%parent is not able be a finish
        throw({'EXIT',unexpected_finish,  TreeEts } );
next_aim( _,?ROOT, _  )->
        false;
next_aim( Parent, Index, TreeEts )->
      Res = ets:lookup(TreeEts, Index),  
      ?DEV_DEBUG("~p process aim ~p ~n",[{?MODULE,?LINE}, {Res, Parent, Index} ]),
%       ?PAUSE,
      case Res of
        [ T = #aim_record{solutions = [], next = one }] ->%%one  it's pattern matching 
                ?DEV_DEBUG("~p got to prev aim  ~p ~n",[{?MODULE,?LINE}, T#aim_record.prev_id]),
                ets:delete(TreeEts, T#aim_record.id),%% it doesn't need us now
                next_aim(Parent, T#aim_record.prev_id, TreeEts); %go to the prev aim
        %%TODO think a lot about overhead 
         [ T = #aim_record{next = hbase, solutions = Pid} ]-> %%one  it's pattern matching 
                Pattern = fact_hbase:get_facts(Pid),
                ?DEV_DEBUG("~p work pattern  ~p ~n",[{?MODULE,?LINE},  Pattern ]),
                case process_next_hbase(Pattern, T, TreeEts, T#aim_record.parent) of
                        ProcessRes = {true, _Context, _Prev} -> 
                            ProcessRes;
                        false ->
                            ?DEV_DEBUG("~p work to previouse  ~p ~n",[{?MODULE,?LINE},  Index ]),
                            next_aim(Parent, Index, TreeEts );
                         Unexpected ->
                            throw({'EXIT',unexpected_return_value, {Unexpected, T, TreeEts} } )
                end;
        [ T = #aim_record{next = one} ]-> %%one  it's pattern matching 
                Pattern =  aim_match(next_pattern, T#aim_record.solutions,  T#aim_record.temp_prototype, T#aim_record.context ),
                ?DEV_DEBUG("~p work pattern  ~p ~n",[{?MODULE,?LINE},  Pattern ]),
                case process_next(Pattern, T, TreeEts, T#aim_record.parent) of
                        ProcessRes = {true, _Context, _Prev} -> 
                            ProcessRes;
                        false ->
                            ?DEV_DEBUG("~p work to previouse  ~p ~n",[{?MODULE,?LINE},  Index ]),
                            next_aim(Parent, Index, TreeEts );
                         Unexpected ->
                            throw({'EXIT',unexpected_return_value, {Unexpected, T, TreeEts} } )
                end;
         [ T = #aim_record{next = NextSolution} ]->
                    ?DEV_DEBUG("~p  go to  next solution  ~p ~n",[{?MODULE,?LINE},  NextSolution ]),
%                      %%???
                     NextSolutionRes = next_aim(#aim_record.id, NextSolution, TreeEts ),
                     ?DEV_DEBUG("~p   next solution  ~p ~n",[{?MODULE,?LINE},  NextSolutionRes ]),
                     
                     case  NextSolutionRes  of
                           {true, ThatLocalContext, Prev} -> 
                                  next_in_current_leap(T, TreeEts, ThatLocalContext, 
                                                       Prev, T#aim_record.solutions, T#aim_record.parent  );
                           false ->
                                  ?DEBUG("~p return false from ~p ~n",[{?MODULE,?LINE}, T ]),

                                  ets:insert( TreeEts, T#aim_record{ next = one }),%%FINISH bu try another patterns  
                                  next_aim( Parent, Index, TreeEts ); %% it will be  turn to previous index cause one
                            Unexpected ->
                                  throw({'EXIT',unexpected_return_value, {Unexpected, T, TreeEts} } )
                     end;         
         []->
            false;
         Unexpected ->
             throw({'EXIT',unexpected_tree_leap, {Unexpected, TreeEts} } )
        end
.
bound_temp_and_original(Context, T)->
      ?DEBUG("~p bound temp and original aims  ~p ~n",[{?MODULE,?LINE},  T  ]),
      BoundProtoType =  bound_aim(T#aim_record.temp_prototype, Context),
      ?DEBUG("~p bound temp aim  ~p ~n",[{?MODULE,?LINE},
                {T#aim_record.prototype, T#aim_record.temp_prototype, BoundProtoType }]),
      { true, NewContext} = prolog_matching:var_match(T#aim_record.prototype, BoundProtoType,T#aim_record.context),
      { NewContext, BoundProtoType}  
.

process_next_hbase([], T, TreeEts, Parent)->
        ets:insert(TreeEts, T#aim_record{solutions = [], next = one } ), %%tell finish everything
        false;
process_next_hbase( [Res], T, TreeEts, Parent)->
        %    Res = [{}]
        %%TODO remove this to fact_hbase module
        NewIndex =  get_index(T#aim_record.id), % now(), %T#aim_record.id + 1,
        Name =  erlang:element(1, T#aim_record.prototype ),
        BoundProtoType = list_to_tuple( [Name|tuple_to_list( Res )] ),
        %%MUST BE TRUE
        ?DEBUG("~p bound temp aim  ~p ~n",[{?MODULE,?LINE},
                                           {T#aim_record.prototype, T#aim_record.temp_prototype, BoundProtoType }]),
                                  
                                           
        { true, NewLocalContext } = prolog_matching:var_match(BoundProtoType, T#aim_record.prototype, T#aim_record.context),
        %%TODO remove all logs with dict:to_list functions
        ?DEBUG("~p process FACT ~p ~n",[{?MODULE,?LINE},  {
                                                            T#aim_record.prototype,
                                                            T#aim_record.temp_prototype,
                                                            T#aim_record.next_leap} ]),
        %%TODO change all conv3 to aim procedure
        %% this case is needed for saving original context of current tree leap
        ?TRACE(T#aim_record.id, TreeEts, T#aim_record.temp_prototype, NewLocalContext),
        ?TRACE2(T#aim_record.id, TreeEts, BoundProtoType, NewLocalContext),
        
        conv3( T#aim_record.next_leap , NewLocalContext, T#aim_record.id, NewIndex, TreeEts, Parent );
process_next_hbase( Unexpected, _T, TreeEts, _Parent)->
        throw({'EXIT',unexpected_tree_leap, {Unexpected, TreeEts} } ).

        
        
process_next(false, T, TreeEts, Parent)->        
        ets:insert(TreeEts, T#aim_record{solutions = [] } ),
        false;
process_next({true, NewContext, true, Tail}, T, TreeEts, Parent)->
        ets:insert(TreeEts, T#aim_record{solutions = Tail, next = one}),
        NewIndex =  get_index(T#aim_record.id), % now(), %T#aim_record.id + 1,
        { NewLocalContext, BoundProtoType } = bound_temp_and_original(NewContext, T),                                                  
        %%TODO remove all logs with dict:to_list functions
        ?DEBUG("~p process FACT ~p ~n",[{?MODULE,?LINE},  {
                                                            T#aim_record.prototype,
                                                            T#aim_record.temp_prototype,
                                                            T#aim_record.next_leap} ]),
        %%TODO change all conv3 to aim procedure
        %% this case is needed for saving original context of current tree leap
        ?TRACE(T#aim_record.id, TreeEts, T#aim_record.temp_prototype, NewLocalContext),
        ?TRACE2(T#aim_record.id, TreeEts, BoundProtoType, NewLocalContext),
        conv3( T#aim_record.next_leap , NewLocalContext, T#aim_record.id, NewIndex, TreeEts, Parent )       
;
process_next({ true, NewContext, NextBody, Tail }, T, TreeEts, Parent)->
         ?DEBUG("~p matching is rule ~p ~n",[{?MODULE,?LINE}, T ]),
         NewIndex = get_index(T#aim_record.id), % now() ,%T#aim_record.id + 1,         
%          ?DEBUG("~p process RULE ~p ~n",[{?MODULE,?LINE},  {dict:to_list(NewContext), NextBody} ]),
         %%save another pattterns
         ets:insert(TreeEts, T#aim_record{solutions = Tail, next = one}),
         ?TRACE(T#aim_record.id, TreeEts, T#aim_record.temp_prototype, T#aim_record.context),
         
         case conv3( NextBody, NewContext, finish, NewIndex, TreeEts, T#aim_record.id  ) of 
                {true, ThatLocalContext, Prev}->
                        next_in_current_leap(T, TreeEts,ThatLocalContext, Prev, Tail, Parent );
                Res ->  %%means false 
                        ?DEBUG("~p return false from ~p ~n",[{?MODULE,?LINE}, NextBody ]),
                        ?TRACE2(T#aim_record.id, TreeEts, {T#aim_record.temp_prototype, Res }, T#aim_record.context ),
                        Res
                
         end
;
process_next(Unexpected, T, TreeEts, Parent)->
         throw({'EXIT',unexpected_next_aim, {Unexpected, T, TreeEts, Parent} } )
 
.
next_in_current_leap(T = #aim_record{prototype = ?LAMBDA}, TreeEts, ThatLocalContext, Prev, Tail, Parent )->
%%we must check again cause ! inside in rule can cut another leaps
     [NewT] = ets:lookup(TreeEts, T#aim_record.id), %%Think a lot how avoid this
     ets:insert(TreeEts, NewT#aim_record{ next = Prev}),
     NewIndex2 = get_index( Prev  ), 
     conv3( T#aim_record.next_leap , ThatLocalContext, T#aim_record.id, NewIndex2, TreeEts, Parent )  
;
next_in_current_leap(T, TreeEts, ThatLocalContext, Prev, Tail, Parent )->
    %%we must check again cause ! inside in rule can cut another leaps
    [NewT] = ets:lookup(TreeEts, T#aim_record.id), %%Think a lot how avoid this
    
     ets:insert(TreeEts, NewT#aim_record{next = Prev}),
     
     { NewLocalContext, BoundProtoType }  = bound_temp_and_original(ThatLocalContext, T),       

     NewIndex2 = get_index( Prev  ), %now(), % Prev + 1,
     ?TRACE2(T#aim_record.id, TreeEts, BoundProtoType,  NewLocalContext ),
     conv3( T#aim_record.next_leap , NewLocalContext, T#aim_record.id, NewIndex2, TreeEts, Parent )  
.






%%working with linked facts this is the first step for implementation expert dynamic  system
worker_linked_rules(Body, OutTree)->
      ?WAIT("~p out table  ~p ~n",[{?MODULE,?LINE},  ets:tab2list(OutTree) ]),
      TreeEts = ets:new(some_name ,[set, public, { keypos, 2 } ]),
      [Prefix] = ets:lookup(OutTree, ?PREFIX),
      ets:insert(TreeEts, Prefix),
      ets:insert(TreeEts, {system_record, ?DEBUG_STATUS, false}), %%turn off debugging      
      

      FactName = erlang:element(1,Body ),
      
      ProtoType = tuple_to_list( common:my_delete_element(1, Body) ),
      List = ets:lookup( common:get_logical_name(TreeEts, ?META_LINKS) , FactName),
      ?WAIT("~p temp table add  ~p ~n",[{?MODULE,?LINE},   List ]),
      
      Res = foldl_linked_rules(FactName, List, TreeEts, ProtoType),
      ets:delete(TreeEts),
      Res
.

%  Res = (catch prolog:aim( finish, ?ROOT, Goal,  dict:new(), 
%                                                 1, tree_processes, ?ROOT) ),
foldl_linked_rules(FactName, [], _TreeEts, _ProtoType)->
    ?WAIT("~p finish add  ~p ~p ~n",[{?MODULE,?LINE},  FactName, true]),
    true
;
foldl_linked_rules(FactName, [Head| Tail], TreeEts, ProtoType)->
    {_, _NameFact, Rule   } = Head,
    ?LOG("~p try do rule ~p for ~p ~n",[{?MODULE,?LINE}, Rule, FactName]),
    clean_tree(TreeEts),
    RuleCall = list_to_tuple([Rule| ProtoType]),
    ?WAIT("~p start next rule ~p ~p ~n",[{?MODULE,?LINE}, Rule, FactName]),
    
    Res = repeat(3, ?MODULE, aim, [finish, ?ROOT, RuleCall, dict:new(), 1, TreeEts, ?ROOT ]),
    
    
    ?COUNT("~p finish rule ~p result ~p ~n",[FactName, Rule, Res]),
    ?WAIT("~p finish rule ~p ~p ~n",[{?MODULE,?LINE}, Rule, {FactName,Res}]),
    foldl_linked_rules(FactName, Tail, TreeEts, ProtoType)
.




%TODO process the Hbase mistakes and timeouts
repeat(Count, Module ,Func, Params )->
  repeat(0,Count, Module ,Func, Params )

.

repeat(0, infinity, Module ,Func, Params )->

    ?LOG("~p try do infinity function ~p ~n",[{?MODULE,?LINE}, {Module,Func,Params }]), 
    case erlang:apply(Module, Func, Params) of
	{true, _ ,_}-> 
	      true;
	false-> repeat(0, infinity, Module ,Func, Params )
    end
;
repeat(Count, Count, Module ,Func, Params )->
      erlang:apply(Module, Func, Params)
;
repeat(Index, Count, Module ,Func, Params )->
    ?LOG("~p try do infinity function ~p count ~p ~n",[{?MODULE,?LINE}, {Module,Func,Params },Index]), 
    
    case erlang:apply(Module, Func, Params) of
	{true, _ ,_} -> true ;
	false ->
		timer:sleep(1500),%TODO move it to settings
		repeat(Index+1, Count, Module ,Func, Params )
    end
.



delete_about_me(Index,TreeEts)->
    ets:delete(TreeEts, Index).



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



conv3(  'finish', NewContext, PrevIndex, NewIndex, TreeEts, ParentIndex )->
        {true, NewContext, PrevIndex}
;
conv3({ ',', Rule, Body }, Context, PrevIndex, Index, TreeEts, ParentIndex )->
	%start process of fact and rule calculation
% 	?DEBUG("~p  process  ~p ~n",[{?MODULE,?LINE}, {Rule, Body, dict:to_list(Context) } ]),
	{Time , Res} =   timer:tc(?MODULE, 'aim' ,[ Body, PrevIndex , Rule, Context,  Index, TreeEts, ParentIndex] ),
	?TC("~p conv3 process in  ~p ~n",[{?MODULE,?LINE}, {Time,Res}  ]),
	%%%form params for 
	Res
;
conv3( Body, Context, PrevIndex, Index, TreeEts, ParentIndex )-> %%last rule in syntax tree 
	%%Search bounded need for hbase
	{Time , Res} =   timer:tc(?MODULE, 'aim', [ 'finish', PrevIndex, Body, Context,  Index, TreeEts, ParentIndex]),
        Res
.

 
%%% make temp aims for prolog shell

bound_list_hack(List, Context)->
    bound_list_hack(List, Context, [])
.
bound_list_hack([], Context, Res)->
    {  lists:reverse( Res ) , Context };
    
bound_list_hack([ Head | Tail ], Context, Res) when is_list(Tail)->
     case prolog_matching:is_var(Head) of
	  true ->	
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
	    true -> 
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
	 
	 {BoundVar, NewContext }= 
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
    {_Me,_,Mic} = erlang:now(),
    %%TODO you must avoid this
    { list_to_atom( "_" ++ integer_to_list(Mic) ) }
.



is_rule({':-',_Proto, _Name })->
    true
;	
is_rule(ProtoType) when is_tuple(ProtoType)->
    NewProto =   common:my_delete_element(1,ProtoType  ),
    is_rule( tuple_to_list(NewProto) )
;	
is_rule([])->
    false
;
is_rule([Head|Tail])->
      %TODO
      case prolog_matching:is_var(Head) of
	    true -> true;
	    _ -> 
		  is_rule(Tail)
      end
.

common_process_expr( Body = {'mod', _Var1, _Var2  } , Context )->
    arithmetic_process(Body,Context );
common_process_expr( Body = {'+', _Var1, _Var2  } , Context )->
    arithmetic_process(Body,Context );
common_process_expr( Body = {'/', _Var1, _Var2  } , Context )->
    arithmetic_process(Body,Context );
common_process_expr( Body = {'*', _Var1, _Var2  } , Context )->
    arithmetic_process(Body,Context );
common_process_expr( Body = {'-', _Var1, _Var2  } , Context )->
    arithmetic_process(Body,Context );
common_process_expr( Var, _Context ) when is_float(Var)->
     Var
;
common_process_expr( Var, _Context ) when is_integer(Var)->
     Var
;
common_process_expr( Body, Context )->
      case prolog_matching:find_var(Context, Body ) of 
	    nothing -> false;
	    Var -> Var
      end
      .

 
 
% {'+',{'+',{'R1'},{'R'}},{'R4'}}
arithmetic_process(nothing, _Context)->
    false;
arithmetic_process( {Operator, Var1, Var2  } , Context )->	
     NewVar1 =  arithmetic_process(Var1, Context),
     NewVar2 =  arithmetic_process(Var2, Context),
     ?DEBUG(" test  - ~p, ~p", [NewVar1,NewVar2] ),
     a_result(Operator, NewVar1, NewVar2)
;
arithmetic_process( Name = {'-',Var}, Context )->	
	NewVar1 =  prolog_matching:bound_body(Var, Context),
	NewVar = case NewVar1 of
		    {_}-> false;
		    _ -> NewVar1
		  end,
	
       case arithmetic_process(NewVar, Context) of 
	    false -> false;
	    
	    X when is_float(X) -> -1*X;
	    X when is_integer(X) -> -1*X;
	    X when is_atom(X) -> false;
	    X when is_binary(X)->
			{First, _Second} = string:to_float("-"++binary_to_list(X)++".0" ),
			my_float( First );	  
	    X ->
			{First, _Second} = string:to_float("-"++ X ++".0" ),
			my_float( First )
	end;
arithmetic_process( Name = {'+',Var}, Context )->	
	NewVar1 = prolog_matching:bound_body(Var, Context),
	NewVar = case NewVar1 of
		    {_}-> false;
		    _ -> NewVar1
		  end,
       case arithmetic_process(NewVar, Context) of 
	    false -> false;
	    X when is_float(X) -> X;
	    X when is_integer(X) -> X;
	    X when is_atom(X) -> false;
	    X when is_binary(X)->
			{First, _Second} = string:to_float(binary_to_list(X)++".0" ),
			my_float( First );	  
	    X ->
			{First, _Second} = string:to_float( X ++".0" ),
			my_float( First )
	end;	
arithmetic_process( Name = {_Var}, Context )->	
	NewVar1 = prolog_matching:bound_body(Name, Context),
	NewVar = case NewVar1 of
		    {_}-> false;
		    _ -> NewVar1
		  end,
	%cause the hbase store only strings
	%%in normal case all non-number it must fail
       case arithmetic_process(NewVar, Context) of 
	    nothing -> false;
	    X when is_float(X) -> X;
	    X when is_integer(X) -> X;
	    X when is_atom(X) -> false;
	    X when is_binary(X)-> 
		{First, _Second} = string:to_float( binary_to_list(X)++".0" ),
		  my_float( First );	  
	    X ->
		  {First, _Second} = string:to_float( X++".0" ),
		  my_float( First )
	end;
	
arithmetic_process(SomeThing, _Context )->	
       case SomeThing of 
	    X when is_float(X) -> X;
	    X when is_integer(X) -> X;
	    X when is_atom(X) -> false;
	    X when is_binary(X)->
		 {First, _Second} = string:to_float( binary_to_list(X)++".0" ),
		  my_float( First );
	    X ->
		  {First,_Second} = string:to_float( X++".0" ),
		  my_float( First )
	end
.

a_result(_Operator, _NewVar1, false)->
    false;
a_result(_Operator, false, _)->
    false;
a_result('+', NewVar1, NewVar2)->
    NewVar1 + NewVar2;
a_result('mod', NewVar1, NewVar2)->
    NewVar1 rem NewVar2;
a_result('-', NewVar1, NewVar2)->
     NewVar1 - NewVar2;
a_result('/', NewVar1, NewVar2)->
    NewVar1/NewVar2;
a_result('*', NewVar1, NewVar2)->
    NewVar1*NewVar2.

my_float(error)->
  false;
my_float(Float)->
  Float.
 
 
 
 
compare(One, One) ->
    true
;
compare(_One, _Second)->
    false
.

not_compare(One, One) ->
    false
;
not_compare(_One, _Two) ->
    true
.



bound_aim(ProtoType, Context)->
	BoundedSearch = prolog_matching:bound_body(ProtoType, Context ),
	BoundedSearch
.

%%HACK avoid this 
-ifdef(USE_HBASE).

operators( Op )->
  no
.

-else.


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
-endif.

%TODO avoid this by saving facts and rules in one table
%%comments we are not able avoid with using HBASE
is_deep_rule(Name, TreeEts)->
    FactTable = common:get_logical_name(TreeEts,?RULES ),
    case ets:lookup(FactTable, Name) of
	  [] -> false;
	  _ ->true
    end
.

delete_fact(TreeEts, Body, Context ,0)->
        NewBody = prolog_matching:bound_body(Body, Context),   
        Name = erlang:element(1, NewBody),
        ProtoType = common:my_delete_element(1, NewBody),
        ?DEV_DEBUG("~p retracting fact ~p",[{?MODULE,?LINE},  NewBody]),
        %%TODO deleting matching rules !!! now we delete only facts
        ets:delete_object(common:get_logical_name(TreeEts, ?RULES), {Name, ProtoType, true } );
delete_fact(TreeEts, Body, Context ,1)->
        BodyBounded = prolog_matching:bound_body(Body, Context),   
        Name = erlang:element(1, BodyBounded),
        case  is_deep_rule(Name, TreeEts) of
                     true ->
                        %%TODO BOUNDING of CONSTANS
                        ?DEV_DEBUG("~p delete rule ~p ~n", [{?MODULE,?LINE},BodyBounded]),
                        erlang:apply(?MODULE, dynamic_del_rule, [BodyBounded, TreeEts]);
%                         unlink(Pid);
                        
                     false ->
                        ?DEV_DEBUG("~p delete fact ", [{?MODULE,?LINE}]),
                         dynamic_del_fact(BodyBounded, TreeEts)
        end.


%%%TODO test various variants wheather worker_linked_rules is failed or dynamic_new_rule/add_new_fact was failed
add_fact(Context, Body, last, TreeEts,  1)->
      BodyBounded = bound_aim(Body, Context),
      Name = erlang:element(1, Body),
      CheckResult = {is_deep_rule(Name, TreeEts), is_rule( BodyBounded )},
      ?DEV_DEBUG("~p add fact ~p", [{?MODULE,?LINE}, {CheckResult,BodyBounded}]),
      case CheckResult of 
            {true,_} -> 
                    dynamic_new_rule(  {':-', Body, true }, last, TreeEts );
            {false, true} -> 
                    dynamic_new_rule(  {':-', Body, true }, last, TreeEts );       
            {false, false  }->
%                   link_fact( BodyBounded, TreeEts ),
                    fact_hbase:add_new_fact( BodyBounded, last, TreeEts)
      end,
      ?DEBUG(" add ~p  yes ~n",[BodyBounded ]),
      
      Res = worker_linked_rules(BodyBounded, TreeEts),
      ?WAIT(" add ~p  result is ~p ~n",[Body, Res ]),
      {true,Context}
;
add_fact(Context, Body, first,TreeEts, 1)->
      BodyBounded = bound_aim(Body, Context),
      Name = erlang:element(1, Body), 
      CheckResult = {is_deep_rule(Name, TreeEts), is_rule( BodyBounded )},
      ?DEV_DEBUG("~p add fact ~p", [{?MODULE,?LINE}, {CheckResult,BodyBounded}]),
      case CheckResult of 
            {true,_} -> 
                    dynamic_new_rule(  {':-', BodyBounded, true }, first, TreeEts);
            {false, true } ->
                    dynamic_new_rule(  {':-', BodyBounded, true }, first, TreeEts);
            {false, _}->
                    fact_hbase:add_new_fact(BodyBounded, first, TreeEts)
      end,
      ?LOG(" add ~p  yes ~n",[BodyBounded ]),
      Res = worker_linked_rules(BodyBounded, TreeEts),
      ?WAIT(" add ~p  result is ~p ~n",[BodyBounded, Res ]),
      {true, Context}
;      
add_fact(Context, Body, last, TreeEts, 0)->
      BodyBounded = bound_aim(Body, Context),
      dynamic_new_rule(  {':-', BodyBounded, true }, last, TreeEts ),
      {true, Context}
;
add_fact(Context, Body, first, TreeEts, 0)->
      BodyBounded = bound_aim(Body, Context),
      dynamic_new_rule(  {':-', BodyBounded, true }, first, TreeEts ),
      {true, Context}
. 
 
      

