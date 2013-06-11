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
get_index(_)->
    now().
    

%%compile inner memory
compile(Prefix, File)->
    {ok, Terms } = erlog_io:read_file(File),
    delete_inner_structs(Prefix),
    lists:foldl(fun process_term/2, Prefix,  Terms ),
    ok
.

delete_inner_structs(Prefix)->

      ets:delete_all_objects(common:get_logical_name(Prefix, ?RULES) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS)),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?INNER) ) ,
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?META_LINKS) ),
      ets:delete_all_objects(common:get_logical_name(Prefix, ?HBASE_INDEX))
.

create_inner_structs(Prefix)->
  
    ets:new(common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS),[named_table, set, public]),
    %TODO remove this
    put(?DYNAMIC_STRUCTS, common:get_logical_name(Prefix, ?DYNAMIC_STRUCTS) ),
    
    ets:new(common:get_logical_name(Prefix, ?META),[named_table, set, public]),
    ets:new(common:get_logical_name(Prefix, ?INNER),[named_table, bag, public]),
    ets:new(common:get_logical_name(Prefix, ?RULES),[named_table, bag, public]),
    ets:new(common:get_logical_name(Prefix, ?META_LINKS),[named_table, bag, public]),
    %the hbase database is for everything
    ets:new(common:get_logical_name(Prefix, ?HBASE_INDEX), [named_table, bag, public])
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
        prolog_matching:var_match(  Body, NewResult,  Context).	
	

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
    receive 
	  start ->
	       ?WAIT("~p GOT  wait del link   ~p ~n",[{?MODULE,?LINE},{Fact1, Fact2} ]),
		List = ets:lookup(TableName, Fact1 ),
		ets:delete(TableName, Fact1 ), %delete links in memory
		lists:foreach(fun(  Val = { _Fact, Elem, _Val1 } )->
				  case Elem of
				      Fact2 -> next;
				      _ ->  ets:insert(TableName, Val)    
				  end
			      end,  List  ),	  
		fact_hbase:del_link_fact(Fact1, Fact2, TableName),
		exit(normal)
      end

.
dynamic_del_fact(B, TreeEts)->
      ?WAIT("~p regis  wait del    ~p ~n",[{?MODULE,?LINE},{B} ]),
       fact_hbase:del_fact(B, TreeEts)
.

dynamic_del_rule(Search, TreeEts)->

      %%TODO process case when after delete we have empty list of rules
      %%we must delete table and meta  from hbase 
       ?WAIT("~p regis  wait del rule    ~p ~n",[{?MODULE,?LINE},Search ]),
      RulesTable = common:get_logical_name(TreeEts, ?RULES),
      receive  
	 start->
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
      end
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
     add_fact(Context, Body, first, TreeEts, ?SIMPLE_HBASE_ASSERT);

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
%%TODO 
inner_defined_aim(NextBody, PrevIndex ,{ use_namespace, Name  }, Context, _Index, _  ) ->
%     Res = fact_hbase:create_new_namespace(Name),
     {true, Context}   
;

inner_defined_aim(NextBody, PrevIndex ,{ create_namespace, Name  }, Context, _Index, _  ) ->
    Res = fact_hbase:create_new_namespace(Name),
    {Res, Context}
;

inner_defined_aim(NextBody, PrevIndex ,{ integer, Body  }, Context, _Index, _  ) ->
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
inner_defined_aim(NextBody, PrevIndex ,{ 'list', Body  }, Context, _Index, _  ) ->
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
inner_defined_aim(NextBody, PrevIndex ,{ atomic, Body  }, Context, _Index, _  ) ->
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
inner_defined_aim(NextBody, PrevIndex ,{ 'float', Body  }, Context, _Index, _  ) ->
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
inner_defined_aim(NextBody, PrevIndex ,{ 'var', Body  }, Context, _Index, _  ) ->
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


inner_defined_aim(NextBody, PrevIndex ,{ 'retract', FirstFact, SecondFact } , Context, _Index, TreeEts  ) when is_atom(FirstFact)->
	Pid = spawn(?MODULE, dynamic_del_link, [ FirstFact, SecondFact, TreeEts ] ),
	unlink(Pid),
	Pid ! start,
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
inner_defined_aim(NextBody, PrevIndex ,Body = { 'writeln', X }, Context, _Index, TreeEts  ) ->
    X1 = prolog_matching:bound_body(X, Context),
%     ?DEBUG("~p write ~p ~n",[X, dict:to_list(Context)]),
    ?WRITELN(TreeEts, X1),
    {true, Context}
;
inner_defined_aim(NextBody, PrevIndex ,Body = {nl,true}, Context, _Index, TreeEts  ) ->
    ?NL(TreeEts),
   {true, Context}
;  

inner_defined_aim(NextBody, PrevIndex ,Body = {system_stat,true}, Context, _Index, TreeEts  ) ->
    ?SYSTEM_STAT(TreeEts),
     {true, Context}
; 
inner_defined_aim(NextBody, PrevIndex ,Body = {fact_statistic, true}, Context, _Index, TreeEts  ) ->
    ?FACT_STAT(?STAT),%%TODO for web
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
inner_defined_aim(NextBody, PrevIndex ,{false,false}, Context, _Index, _TreeEts  )->
    {false,Context}
;
inner_defined_aim(NextBody, PrevIndex ,false, Context, _Index, _TreeEts  )->
    {false,Context}
;
inner_defined_aim(NextBody, PrevIndex ,fail, Context, _Index, _TreeEts  )->
    {?EMPTY,Context}
;
inner_defined_aim(NextBody, PrevIndex ,{true, true}, Context, _Index, _TreeEts  )->
    {true, Context}
;
inner_defined_aim(NextBody, PrevIndex ,true, Context, _Index, _TreeEts  )->
    {true, Context}
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
   {Res, _ } = prolog_matching:var_match(First, Second, Context ),
    {Res, Context}
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


aim(NextBody, _PrevIndex,'!', Context, Index , TreeEts, Parent )->
         
         [AimRecord] = ets:lookup(TreeEts, Parent),  
         ?DEBUG("~p  cut  ~p", [{?MODULE,?LINE}, AimRecord ]),
         ets:insert(TreeEts, AimRecord#aim_record{next = one, solutions=[]} ), 
         %CUT all solution and current leap tell that there is a finish
         conv3( NextBody, Context, finish,  get_index(Index), TreeEts, Parent )
;
aim(NextBody, PrevIndex, Body = {',', ProtoType, Second }, Context, Index, TreeEts, Parent)->
       
        case aim(Second, PrevIndex, ProtoType, Context, Index, TreeEts, Parent) of
            {true, NewLocalContext, NewPrev} ->
                    conv3( NextBody, NewLocalContext, NewPrev,  get_index(Index), TreeEts, Parent );
             Res -> %%means false
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
        
process_builtin_pred({false, _Context}, _NextBody, _PrevIndex, _NewIndex2, _TreeEts, Parent )->
    false
;
%%TODO about NewIndex we are able to get old NewIndex2, if have no such predicate like retract
%%TODO Index must be rewrited to auto increment
process_builtin_pred({true, Context}, NextBody, PrevIndex, NewIndex2, TreeEts, Parent )->
         conv3( NextBody, Context, PrevIndex,  
         get_index(NewIndex2),
         TreeEts, Parent )
.

user_defined_aim(NextBody, PrevIndex, ProtoType, Context, Index, TreeEts, Parent ) when is_atom(ProtoType)->
         RulesTable = common:get_logical_name(TreeEts, ?RULES), 
         RuleList = ets:lookup(RulesTable, ProtoType),        
 
        %%pattern matching like one aim
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
         Name = element(1,ProtoType),%%from the syntax tree get name of fact
         Search =  common:my_delete_element(1, ProtoType),%%get prototype
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
        [ T = #aim_record{next = one} ]-> %%one  it's pattern matching 
                Pattern =  aim_match(next_pattern, T#aim_record.solutions,  T#aim_record.temp_prototype, T#aim_record.context ),
                ?DEV_DEBUG("~p work pattern  ~p ~n",[{?MODULE,?LINE},  Pattern ]),
                case process_next(Pattern, T, TreeEts, T#aim_record.parent) of
                        ProcessRes = {true, _Context, _Prev} -> 
                            ProcessRes;
                        false ->
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
                                  ets:insert( TreeEts, T#aim_record{ next = one }),  
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
      ?DEBUG("~p bound temp aim  ~p ~n",[{?MODULE,?LINE}, {T#aim_record.prototype, T#aim_record.temp_prototype, BoundProtoType }]),

      {true, NewContext} = prolog_matching:var_match(T#aim_record.prototype, BoundProtoType,T#aim_record.context),
%       ?DEBUG("~p new context ~p ~n",[{?MODULE,?LINE}, dict:to_list(NewContext) ]),
      {NewContext, BoundProtoType}  
.



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
                        ?TRACE2(T#aim_record.id, TreeEts, {T#aim_record.temp_prototype, Res }, T#aim_record.context ),
                        Res
                
         end
;
process_next(Unexpected, T, TreeEts, Parent)->
         throw({'EXIT',unexpected_next_aim, {Unexpected, T, TreeEts, Parent} } )
 
.
next_in_current_leap(T = #aim_record{prototype = ?LAMBDA}, TreeEts, ThatLocalContext, Prev, Tail, Parent )->

     [NewT] = ets:lookup(TreeEts, T#aim_record.id), %%Think a lot how avoid this
     ets:insert(TreeEts, NewT#aim_record{ next = Prev}),
     
     NewIndex2 = get_index( Prev  ), 
     conv3( T#aim_record.next_leap , ThatLocalContext, T#aim_record.id, NewIndex2, TreeEts, Parent )  
;
next_in_current_leap(T, TreeEts, ThatLocalContext, Prev, Tail, Parent )->

    [NewT] = ets:lookup(TreeEts, T#aim_record.id), %%Think a lot how avoid this
     ets:insert(TreeEts, NewT#aim_record{next = Prev}),
     
     { NewLocalContext, BoundProtoType }  = bound_temp_and_original(ThatLocalContext, T),       

     NewIndex2 = get_index( Prev  ), %now(), % Prev + 1,
     ?TRACE2(T#aim_record.id, TreeEts, BoundProtoType,  NewLocalContext ),
     conv3( T#aim_record.next_leap , NewLocalContext, T#aim_record.id, NewIndex2, TreeEts, Parent )  
.






%%working with linked facts this is the first step for implementation expert dynamic  system
worker_linked_rules(Body, Prefix)->
      TreeEts = ets:new(some_name ,[set,public]),
      
      ets:insert(TreeEts, {system_record, ?PREFIX, Prefix }),
      
      FactName = erlang:element(1,Body ),
      
      ProtoType = tuple_to_list( common:my_delete_element(1, Body) ),
      
      ets:insert(TreeEts, {system_record, ?DEBUG_STATUS, false}), %%turn off debugging
      
      List = ets:lookup( common:get_logical_name(Prefix,?META_LINKS) , FactName),
%       foldl4(FactName, List, TreeEts, ProtoType),
      ets:delete(TreeEts)

.



%TODO process the Hbase mistakes and timeouts
repeat(Count, Module ,Func, Params )->
  repeat(0,Count, Module ,Func, Params )

.

repeat(0, infinity, Module ,Func, Params )->

    ?LOG("~p try do infinity function ~p ~n",[{?MODULE,?LINE}, {Module,Func,Params }]), 
    case erlang:apply(Module, Func, Params) of
	true-> 
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
	true-> true;
	false->
		timer:sleep(1500),%TODO make it setting
		repeat(Index+1, Count, Module ,Func, Params )
    end
.



delete_about_me(Index,TreeEts)->
    ets:delete(TreeEts, Index).
    
%%function process the last aim in the tree

aim_loop([{false, _ }],  _, Index, _, _, Context)->
      {false, Context,Index }
;
aim_loop({false, _ },  _, Index, _, _, Context)->
      {false, Context ,Index}
;
aim_loop([{?EMPTY, _Some }], _, Index, _, _, Context)->
	{?EMPTY, Context, Index}
;
aim_loop({?EMPTY, _Some }, _, Index, _, _, Context)->
	{?EMPTY, Context, Index}
;
aim_loop({Result, _SomeContext},  ParentPid,  Index, _TreeEts, true, LocalContext)->
	?DEBUG("~p true process result on one leap ~p ~n",[{?MODULE,?LINE}, Result ]),
        {true, LocalContext, Index}
;
aim_loop({Result, NewLocalContext},  ParentPid,  Index, _TreeEts, Body, LocalContext)->
	{true, NewLocalContext, Index}	  
	
;

%%%TODO may be change it to record ??!!!
aim_loop(Result,  ParentPid,  
	Index, TreeEts, Body, LocalContext)->
	?DEBUG("~p process result on one leap ~p ~n",[{?MODULE,?LINE}, {Result, Body} ]),
        Result
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



conv3(  'finish', NewContext, PrevIndex, NewIndex, TreeEts, ParentIndex )->
        {true, NewContext, PrevIndex}
;
conv3({ ',', Rule, Body }, Context, PrevIndex, Index, TreeEts, ParentIndex )->
	%start process of fact and rule calculation
% 	?DEBUG("~p  process  ~p ~n",[{?MODULE,?LINE}, {Rule, Body, dict:to_list(Context) } ]),
	{Time , Res} =   timer:tc(?MODULE, 'aim' ,[ Body, PrevIndex , Rule, Context,  Index, TreeEts, ParentIndex] ),
	?TC("~p conv3 process in  ~p ~n",[{?MODULE,?LINE}, Time  ]),
	%%%form params for 
	Res
;

conv3( Body, Context, PrevIndex, Index, TreeEts, ParentIndex )-> %%last rule in syntax tree 
	%%Search bounded need for hbase
% 	?DEBUG("~p  process  ~p ~n",[{?MODULE,?LINE}, { Body, dict:to_list(Context) } ]),
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
	    true-> true;
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


%%for using by super visor
assert(Body)->
	  %%TODO add buffer 
	  Pid = spawn_link(?MODULE, worker_linked_rules_conv, [Body] ),
	  {ok, Pid }
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

%%%HACK rewrite to tail recursion
start_retract_process([], _BodyBounded, Context, _Index, _TreeEts, ParentPid)->
    ?DEBUG("~p wait for delete ~p ",[{?MODULE,?LINE}, have_no_other ]),
    receive 
	next ->
	      ParentPid ! {result, {false, Context} }
    end;
    
     
%TODO rewrite this !!
start_retract_process(FactList, BodyBounded, Context, Index, TreeEts, ParentPid) when is_list(BodyBounded)->
      [Head| _Tail] = BodyBounded,
      Name = erlang:element(1, Head),
      ?DEBUG("~p wait for delete ~p ",[{?MODULE,?LINE}, BodyBounded ]),
      FactTable = common:get_logical_name(TreeEts,?INNER ),
      receive 
	  next ->
		Res = retract_fold(false, BodyBounded, FactList, Context, [], FactTable ),
		
		case Res of
		    {false, _ }->  ParentPid ! {result, Res };
		    {true, _} ->
			ParentPid ! {result, Res},
			NewFactList = ets:lookup(FactTable, Name),
			start_retract_process(NewFactList, BodyBounded,  Context, Index, TreeEts, ParentPid)
		end
      end
.
%TODO rewrite this !!
start_retract_process(BodyBounded, Context, Index, TreeEts, ParentPid) when is_tuple(BodyBounded)->
    Name = erlang:element(1, BodyBounded),
    ?DEBUG("~p wait for delete ~p ",[{?MODULE,?LINE}, Name ]),
    FactTable = common:get_logical_name(TreeEts,?INNER ),
    OutFacts = common:get_logical_name(TreeEts, ?META),
    receive 
	next ->
		?DEBUG("~p retract got next  ~n",[{?MODULE,?LINE}]),

		case is_deep_rule( Name, TreeEts ) of
		     true ->
			%%TODO BOUNDING of CONSTANS
			Pid = spawn(?MODULE, dynamic_del_rule, [BodyBounded, TreeEts]),
 		    	unlink(Pid),
		        Pid ! start,
		        ParentPid !  {result, {true, Context} },
		        start_retract_process(BodyBounded, Context, Index, TreeEts, ParentPid);
		      
		     false->
			DelRes =
			    case ets:lookup(OutFacts, Name ) of 
			      [_] ->
				  Res = dynamic_del_fact(BodyBounded, TreeEts),
				  ?DEBUG("~p delete fact ~p ",[{?MODULE,?LINE}, {Name, Res} ]),
				  {Res, Context };
			      [] ->    
				      case ets:lookup(FactTable, Name)  of
				      [] ->
					    ParentPid ! {result, {false, Context} };
				      FactList ->
					  ?DEV_DEBUG("~p got list ~p ",[{?MODULE,?LINE},FactList ]),
					  Res = retract_fold(false, BodyBounded, FactList, Context, [], FactTable ),
					  ?DEV_DEBUG("~p delete  res ~p ",[{?MODULE,?LINE},Res ]),
					  Res
					
				      end
			    end,
			    case DelRes of
					      {false, _ }->  ParentPid ! {result, DelRes };
					      {true, _} ->
						  ParentPid ! {result, DelRes},
						  start_retract_process( BodyBounded, Context, Index, TreeEts, ParentPid)
			    end
		end
    end
 
.
%TODO avoid this by saving facts and rules in one table

is_deep_rule(Name, TreeEts)->
    FactTable = common:get_logical_name(TreeEts,?RULES ),
    case ets:lookup(FactTable, Name) of
	  [] -> false;
	  _ ->true
    end
.

retract_fold({true,NewContext}, Match, [],_Context, NewList, Table)->
    ReverS = lists:reverse(NewList),
    Name = erlang:element(1, Match),
    ets:delete(Table, Name),
    ets:insert(Table, ReverS),
    {true, NewContext};
    
retract_fold(false ,_Match, [],Context, List, _Table)->
   {false, Context};    
retract_fold(Res = {true, _}, Match, [Head|Tail], Context, List, Table)->
       retract_fold(Res, Match, Tail, Context, [Head|List], Table) 
;    
retract_fold(Res, Match, [Head|Tail], Context, List, Table)->
	  
    case  prolog_matching:var_match(Match, Head, Context)  of
	  {true, NewContext}->
	    
	    retract_fold(
		      {true,NewContext}, Match, Tail, Context, List, Table
		
		);
	 { false, _}->
		 retract_fold(Res, Match, Tail, Context, [Head|List], Table) 
	 
    end    
.

add_fact(Context, Body, last, TreeEts,  1)->
      BodyBounded = bound_aim(Body, Context),
      case is_rule(BodyBounded) of 
            true -> 
                    dynamic_new_rule(  {':-', Body, true }, last, TreeEts );
            false ->
%                   link_fact( BodyBounded, TreeEts ),
                    fact_hbase:add_new_fact( BodyBounded, last, TreeEts)
      end,
      ?DEBUG(" add ~p  yes ~n",[Body ]),
      {true,Context};
add_fact(Context, Body, first,TreeEts, 1)->
      BodyBounded = bound_aim(Body, Context),
      case is_rule( BodyBounded ) of 
            true -> 
                    dynamic_new_rule(  {':-', BodyBounded, true }, first, TreeEts);
            false ->
%                   link_fact( BodyBounded, TreeEts ),
                    fact_hbase:add_new_fact(BodyBounded, first, TreeEts)
      end,
      ?LOG(" add ~p  yes ~n",[Body ]),
      {true, Context};
add_fact(Context, Body, last, TreeEts, 0)->
      dynamic_new_rule(  {':-', Body, true }, last, TreeEts ),
      {true, Context};
add_fact(Context, Body, first, TreeEts, 0)->
      dynamic_new_rule(  {':-', Body, true }, first, TreeEts ),
      {true, Context}. 
 
      

