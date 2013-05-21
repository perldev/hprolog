-module(prolog).
-compile(export_all).
%%% File    : prolog_shell.erl
%%% Author  : Bogdan Chaicka
%%% Purpose : A simple  shell.
%%This module provides prolog kernel work with parsed code before
%%% code is presented as a tree using tuples




-include("prolog.hrl").

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
     ets:insert(common:get_logical_name(Prefix, ?RULES), { Name, { true }, Body } ),
     Prefix
;
process_term(Rule  = {':-',ProtoType, Body}, Prefix)->
     ?DEBUG("~p got rule ~p~n",[{?MODULE,?LINE}, Rule  ]),
     Name  = element(1, ProtoType),
     Args =  common:my_delete_element(1,ProtoType),
     ?DEBUG("~p compile rule ~p~n",[{?MODULE,?LINE}, Rule  ]),
     ets:insert(common:get_logical_name(Prefix,?RULES),  { Name, Args, Body } ),
     Prefix
;
process_term(Aim, Prefix) when is_atom(Aim)->
     ?DEBUG("~p compile fact ~p~n",[{?MODULE,?LINE}, Aim  ]),
     ets:insert(common:get_logical_name(Prefix,?INNER), {Aim, true} ),
     Prefix
;
process_term(Body, Prefix )->
     ?DEBUG("~p compile fact ~p~n",[{?MODULE,?LINE}, Body  ]),
     ets:insert(common:get_logical_name(Prefix,?INNER), Body ),
     Prefix
.

%%%calculate all with patterns
recr([], _SearchQ)->
	[];
recr(Whole, SearchQ)->
	recr(Whole, tuple_to_list(SearchQ), []).

recr([], _ , Res)->
	lists:reverse(Res)
;
recr( [Head| Whole], Head,  Res)->
	NewRes = [Head| Res],  
	recr(Whole, Head, NewRes)
;
%% search and head must be the same size
recr( [Head| Whole], SearchQ,  Res)->
	%% in fact we have vars
	NewRes = prolog_matching:match( tuple_to_list(Head), SearchQ, Res),  
	recr(Whole, SearchQ, NewRes)
.



%%bound vars to Context

fill_context_from_list([], [], Context)->
    Context 
;
fill_context_from_list([], [Search], Context)->
    prolog_matching:store_var({Search, [] }, Context )
;
fill_context_from_list([Head| Not], [ Search | Tail], Context)->

    %%MUST PASS!!
    {true, NewContext} = prolog_matching:var_match(Search, Head, Context),
    fill_context_from_list(Not, Tail, NewContext )
    

;
fill_context_from_list(List, Search, Context)->
    prolog_matching:store_var({Search, List }, Context )
.

%%this operation don't bound vars
fill_context(true, _, Context )->
    Context
;
fill_context(false, _, Context )->
    Context
;
fill_context([], [], Context )->
    Context
;
fill_context(_, ['=:='|_Tail], Context )->
    Context
;
fill_context(_, ['=='|_Tail], Context )->
    Context
;
fill_context(_, ['='|_Tail], Context )->
    Context
;
fill_context(_, ['<'|_Tail], Context )->
    Context
;
fill_context(_, ['>'|_Tail], Context )->
    Context
;
fill_context(_, ['>='|_Tail], Context )->
    Context
;
fill_context(_, ['=<'|_Tail], Context )->
    Context
;
fill_context(_, ['\\='|_Tail], Context )->
    Context
;
fill_context(_, ['=\\='|_Tail], Context )->
    Context
;
fill_context(Head, Search, Context)  when is_tuple(Search)->
    fill_context(Head, tuple_to_list(Search), Context )
;
fill_context(Head, Search, Context )  when is_tuple(Head)->
    fill_context(tuple_to_list(Head), Search, Context );
fill_context([{'_'}| List], [_| Tail], Context )  ->
    fill_context(List, Tail, Context );    
fill_context([_| List], [{'_'}| Tail], Context )  ->
    fill_context(List, Tail, Context );   
%%TODO rewrite it, this function is too big
fill_context([Head| List], [Search| Tail], Context )->
      ?DEV_DEBUG("~p fill context  ~p~n",[{?MODULE,?LINE}, {Head, Search, dict:to_list(Context ) } ]),
      case prolog_matching:is_var(Search) of
	    true -> 
		    case prolog_matching:find_var(Context, Search) of %%val in search wasn't bounded
			  nothing -> 
   				    ?DEV_DEBUG("~p store var ~p~n",[{?MODULE,?LINE}, {Search, Head} ]),
				    NewDict = prolog_matching:store_var({Search, Head}, Context), 
				    fill_context(List, Tail, NewDict  );
			  {AnotherVar} -> 
				    ?DEV_DEBUG("~p store var ~p~n",[{?MODULE,?LINE}, {Search, Head} ]),
				    NewDict = prolog_matching:store_var({Search, Head}, Context), 
				    NewDict1 = prolog_matching:store_var({{AnotherVar}, Head}, NewDict), 
				    fill_context(List, Tail, NewDict1  );		     
			  Head ->   
				    fill_context(List, Tail, Context  );
			  Not -> 
				    ?DEV_DEBUG("~p  struct ~p~n",
						[{?MODULE,?LINE}, {Search, Not, Head} ]),	   
				    case prolog_matching:var_match(Not, Head, Context) of
						    {true, NewLocalContext}->
							  NewVar = prolog_matching:bound_body(Not ,NewLocalContext),
							  NewLocalContext1 = 
								  prolog_matching:store_var({Search, NewVar}, NewLocalContext), 
							  fill_context( List, Tail, NewLocalContext1  );
						    _->
						      ?DEV_DEBUG("~p impossible situation this is not matched ~p~n",
							  [ {?MODULE,?LINE}, {Search, Not,dict:to_list(Context)} ] ),
							  exit(unexpected_error),
						      []
				    end			    
		    end;
% 		     Head ->  fill_context(List, Tail, Context  );
	    Not ->  
		    case is_list(Not) of
			  true ->
				  ?DEV_DEBUG("~p fill, matched list ~p~n",[{?MODULE,?LINE}, { Head, Search} ]),  
				  NewContext = fill_context_from_list(Head, Search, Context  ),
				  fill_context(List, Tail, NewContext  ); 
			  false-> case Not of 
				       Head ->
					  fill_context( List, Tail, Context  );
				       _ ->  %%%not impossible
 					  ?DEV_DEBUG("~p  got struct situation  ~p~n",
							[ {?MODULE,?LINE}, {Search, Head} ]),
					  case prolog_matching:var_match(Search, Head, Context) of
					      {true, NewLocalContext}->
						     fill_context( List, Tail, NewLocalContext  );
					      _->
						?DEV_DEBUG("~p impossible situation this is not matched ~p~n",
						    [ {?MODULE,?LINE}, {Search, Not, dict:to_list(Context)} ] ),
						    exit(unexpected_error),
						[]
					  end
				  end
				       
		    end
		  
      end.
%%%bound vars     

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
    ?DEV_DEBUG("~p bound ~p~n",[{?MODULE,?LINE}, {Search, dict:to_list(Context) }  ]),
    list_to_tuple(	lists:map( fun(E)->
			      prolog_matching:bound_body(E, Context)
			end, tuple_to_list(Search)
			)
		  )

.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




var_generator_name(Name)->
       { list_to_atom( Name ) }
.





%%prototype
%% name vars
bound_results( Vars, Elem, LocalContext )->

			NewLocalContext = fill_context(
						    Vars,
						    Elem,
						    LocalContext),
						    
			bound_vars(Elem, NewLocalContext )		  

.


%%standard operations and functions
% {ok,Term} ->
% 		    [Term|read_stream(Fd, L1)];
% 		{error,What} -> throw({error,What})

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

%%%inner predicates of our system
aim(Body = {get_char, X }, Context, _Index, TreeEts)-> 
    TempX = ?GET_CHAR, %%only one character
    {MainRes, NewContext} = prolog_matching:var_match( list_to_atom(TempX), X, Context ),
    {MainRes, NewContext}
;
aim(Body = {read, X }, Context, _Index, TreeEts)->
    TempX = ?READ, %%read prolog term
    case TempX of
	{ ok, Term } ->
	      Res = prolog_matching:var_match( Term, X, Context ),
	      Res
	{} ->
	    ?WRITELN("i can parse input"),
	    {false, Context}
    end
;
aim(Body = { 'meta', _FactName, _ParamName, Val }, Context, _Index, TreeEts  )->
    NewBody = prolog_matching:bound_body(Body, Context),
    Res = fact_hbase:meta_info(NewBody,  TreeEts),     
    {MainRes ,NewContext} = prolog_matching:var_match(Res, Val, Context),
    {MainRes ,NewContext}
;
aim({ functor, Body, Name, Count }, Context, _Index, _  )->
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
aim({ arg, Count, Body, Value }, Context, _Index, _  )->
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
aim({ atom, Body  }, Context, _Index, _  ) ->

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
aim({ change_namespace, Name  }, Context, _Index, _  ) ->
%     Res = fact_hbase:create_new_namespace(Name),
    {true, Context}   
;

aim({ create_namespace, Name  }, Context, _Index, _  ) ->
    Res = fact_hbase:create_new_namespace(Name),
    {Res, Context}   
;

aim({ integer, Body  }, Context, _Index, _  ) ->
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
aim({ 'list', Body  }, Context, _Index, _  ) ->
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
aim({ atomic, Body  }, Context, _Index, _  ) ->
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
aim({ 'float', Body  }, Context, _Index, _  ) ->
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
aim({ 'var', Body  }, Context, _Index, _  ) ->
      Res =
	case Body of
	  {Key} when is_atom(Key)->
	        ?DEBUG("~p aim var is ~p~n",[{?MODULE,?LINE}, {Body,dict:to_list(Context) } ]),

		case prolog_matching:find_var(Context, Body)  of
		     nothing -> true;
		     _ -> false
		end;
	  _ -> 
	      false
	end,
    {Res, Context}
;
aim({ 'nonvar', Body  }, Context, _Index, _  ) ->
      
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
aim({ 'assertz', SourceFact, ForeignFact,  Body = {':-', _ProtoType, _Body1 }  }, Context, Index, TreeEts  ) ->
  aim( { 'assert',  SourceFact, ForeignFact,  Body   }, Context, Index, TreeEts  )
;
aim({ 'assert', SourceFact,  ForeignFact, Body = {':-', ProtoType, _Body1 }  }, Context, _Index, TreeEts ) ->
      dynamic_new_rule(Body, last, TreeEts),
      RuleName = erlang:element(1, ProtoType),
      fact_hbase:add_link(SourceFact, ForeignFact, RuleName, TreeEts ),
      ets:insert(common:get_logical_name(TreeEts, ?META_LINKS), {SourceFact, ForeignFact, RuleName  }),
      {true, Context} 
;

aim({ 'assertz', SourceFact, ForeignFact,  RuleName  }, Context, Index, TreeEts  ) ->
  aim( { 'assert',  SourceFact, ForeignFact,  RuleName   }, Context, Index, TreeEts   )
;

aim({ 'assert', SourceFact,  ForeignFact, RuleName }, Context, _Index, TreeEts ) ->
      fact_hbase:add_link(SourceFact, ForeignFact, RuleName, TreeEts ),
      ets:insert( common:get_logical_name(TreeEts, ?META_LINKS), 
		  {SourceFact, ForeignFact, RuleName  }),
      {true, Context}
;
%%%usual assert
aim({ 'assertz', Body = {':-', _ProtoType, _Body1 }  }, Context, Index, TreeEts  ) ->
  aim( { 'assert', Body   }, Context, Index, TreeEts   )
;
aim({ 'assert', Body = {':-', _ProtoType, _Body1 }  }, Context, _Index, TreeEts ) ->
      dynamic_new_rule(Body, last, TreeEts),
      {true, Context}

;
aim({ 'asserta', Body = {':-', _ProtoType, _Body1 }   }, Context, _Index, TreeEts  )->
     dynamic_new_rule(Body, first, TreeEts),
     {true,Context}
    
;
aim({ 'assertz', Body   }, Context, Index, TreeEts  ) when is_tuple(Body)->
    aim( { 'assert', Body   }, Context, Index, TreeEts   )
;
aim({ 'assert', Body   }, Context, _Index, TreeEts  ) when is_tuple(Body)->
%       Name = element(1,Body),
      BodyBounded = bound_aim(Body, Context),
      case is_rule(BodyBounded) of 
	    true -> 
		    dynamic_new_rule(  {':-', BodyBounded, true }, last, TreeEts );
	    false ->
		    link_fact( BodyBounded, TreeEts ),
		    fact_hbase:add_new_fact( BodyBounded, last, TreeEts)
      end,
      ?DEBUG(" add ~p  yes ~n",[Body ]),
      {true,Context}
;
aim({ 'asserta', Body   }, Context, _Index, TreeEts  ) when is_tuple(Body)->
%       Name = element(1,Body),
      BodyBounded = bound_aim(Body, Context),
      case is_rule( BodyBounded ) of 
	    true -> 
		    dynamic_new_rule(  {':-', BodyBounded, true }, first, TreeEts);
	    false ->
		    link_fact( BodyBounded, TreeEts ),
		    fact_hbase:add_new_fact(BodyBounded, first, TreeEts)
      end,
      ?LOG(" add ~p  yes ~n",[Body ]),
      {true, Context}
;
aim({ 'retract', FirstFact, SecondFact } , Context, _Index, TreeEts  ) when is_atom(FirstFact)->
	Pid = spawn(?MODULE, dynamic_del_link, [ FirstFact, SecondFact, TreeEts ] ),
	unlink(Pid),
	Pid ! start,
        ?LOG("~p del ~p  yes ~n",[ {?MODULE,?LINE}, {  FirstFact, SecondFact } ] ), 
        {true, Context}
;
aim(Body = { 'write', X }, Context, _Index, _TreeEts  ) ->
    X1 = prolog_matching:bound_body( X, Context),
    ?DEBUG("~p write ~p ~n",[X, dict:to_list(Context)]),
    ?WRITE(X1),
    {true, Context}
;
aim(Body = { 'writeln', X }, Context, _Index, _TreeEts  ) ->
    X1 = prolog_matching:bound_body(X, Context),
    ?DEBUG("~p write ~p ~n",[X, dict:to_list(Context)]),
    ?WRITELN(X1),
    {true, Context}
;
aim(Body = {nl,true}, Context, _Index, _TreeEts  ) ->
    ?NL,
    {true, Context}
;  

aim(Body = {system_stat,true}, Context, _Index, TreeEts  ) ->
    ?SYSTEM_STAT(TreeEts),
    {true, Context}
; 
aim(Body = {fact_statistic, true}, Context, _Index, TreeEts  ) ->
    ?FACT_STAT(?STAT),
    {true, Context}
; 

%%new binary operator via  ор( 600, хfу, &)  :- ор( 500, fy, ~).

%%infix
    

aim(Body = { op, _OrderStatus, _, Name }, Context, _Index, TreeEts  ) when is_atom(Name) ->
    Res = add_operator( Body, TreeEts ),
    {Res, Context}
;   

aim({ op, _OrderStatus, _, _Name }, Context, _Index, _TreeEts  ) ->
    {false, Context}
;  
%%TODO make operators of == > < \\=
% false, true, fail standart prolog operators

aim({false,false}, Context, _Index, _TreeEts  )->
    {?EMPTY,Context}
;
aim(false, Context, _Index, _TreeEts  )->
    {?EMPTY,Context}
;
aim(fail, Context, _Index, _TreeEts  )->
    {?EMPTY,Context}
;
aim({true, true}, Context, _Index, _TreeEts  )->
    {true, Context}
;
aim(true, Context, _Index, _TreeEts  )->
    {true, Context}
;

% tree represantation  = {'==',
%                                     {'_A__459399'},
%                                     {'+',{'+',{'X'},{'Y'}},{'U'}}}
%%Arithmetic comprasion

%%{',',{is,{'+',{'X1'},{'Y3'}},{'+',{'Y'},{'X'}}}
%%{Accum} it means that we work only with one X = X1 + XY, but not  X+C1 = X1 + XY!!

aim( Body = {'date', _Ini, _Typei, _Accumi },  Context, Index, TreeEts)->
	

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
aim( {'is',  NewVarName = { _Accum }, Expr },
		  Context, _Index , _TreeEts )->
	CalcRes = common_process_expr(Expr, Context),
	?DEBUG("~p calc expression ~p ~n",[{?MODULE,?LINE},{ NewVarName, Expr, CalcRes  } ]),
	prolog_matching:var_match(NewVarName, CalcRes, Context)

;
aim({ '=:=', First, Second }, Context, _Index, _TreeEts  )->
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res ={ compare(One,Two), Context },
    Res
    
;
aim({ '=', First, Second }, Context, _Index, _TreeEts  )->
    ?DEBUG("~p aim = ~p",[{?MODULE,?LINE}, { '=', First, Second } ]),
    Res = prolog_matching:var_match(First, Second, Context ),
    ?DEV_DEBUG("~p aim = ~p",[{?MODULE,?LINE}, Res ]),
    Res

    
;
aim({ '\\=', First, Second }, Context, _Index, _TreeEts  )->
    ?DEBUG("~p aim = ~p",[{?MODULE,?LINE}, { '=', First, Second } ]),
    case prolog_matching:var_match(First, Second, Context ) of
	{false, _ } -> {true, Context};
	_->{false,Context}
    end
    
;
aim({ '==', First, Second }, Context, _Index, _TreeEts  )->
   {Res, _ } = prolog_matching:var_match(First, Second, Context ),
   {Res, Context}
    
;
aim({ '>=', First, Second }, Context, _Index, _TreeEts  )->
    
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { One >= Two, Context },
    Res
    
   
    
;
aim({'=<', First, Second }, Context, _Index, _TreeEts  )->
	    
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { One =< Two, Context }, Res
;
aim({ '>', First, Second }, Context, _Index, _TreeEts  )->
    
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    
    Res = { One > Two, Context }, Res
    
   
    
;
aim({'<', First, Second }, Context, _Index, _TreeEts  )->
	    
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { One < Two, Context },
    Res
;
aim( {'=\\=', First, Second }, Context, _Index, _TreeEts )->
    One = arithmetic_process(First, Context),
    Two = arithmetic_process(Second, Context),
    Res = { not_compare(One ,Two), Context },
    Res
   
;
%%%user defined aims
aim(Body = {';', _, _ }, Context, Index, TreeEts) ->	

      case ets:lookup(TreeEts, Index ) of
	  [ {_Key,{ ?EMPTY, rule } } ] -> 
		 Pid = spawn(?MODULE, not_conv3, [{true}, Body, Context,erlang:self(), Index, TreeEts] ), 
% 		 
% 		      spawn_link(prolog, conv3, [true, Body,  Context,
% 							erlang:self(), Index, TreeEts]),
                ?WAIT("~p regis  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Body, Index} ]),
                ets:insert(TreeEts, { Index, { Pid, rule} } ),
                Pid ! start,
	        receive 
		    {result, Result}->
                        ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Body, Index} ]),
		         Result;
		    Some ->
                          ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Body, Index} ]),
			   ?DEBUG("~p got empty ~p ~n",[{?MODULE,?LINE}, Some]),
			   exit(Pid, finish),
			   {?EMPTY, Context}
	        end;
	  [ {_Key,{ Pid, rule } } ] -> 
		    aim_rule(Pid ,next, Context);
	  [] ->
	      ?DEBUG("~p got empty ~p ~n",[{?MODULE,?LINE}, Index]),
	      {?EMPTY, Context}
      end
;
aim(Body = {',', _, _ }, Context, Index, TreeEts) ->	

%       ?DEBUG("~p etses there ~p~n",[ {?MODULE, ?LINE}, {Body,
% 							 ets:tab2list(TreeEts),
% 							 Index} ]),
      case ets:lookup(TreeEts, Index ) of
	  [ {_Key,{ ?EMPTY, rule } } ] -> 
		 Pid = spawn(?MODULE, not_conv3,[{true}, Body, Context, erlang:self(), Index, TreeEts]), 
                ?WAIT("~p regis  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Body, Index} ]),
                ets:insert(TreeEts, { Index, { Pid, rule} } ),
                Pid ! start,
	        receive 
		    {result, Result}->
                        ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Body, Index} ]),
		         Result;
		    Some ->
                          ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Body, Index} ]),
			   ?DEBUG("~p got empty ~p ~n",[{?MODULE,?LINE}, Some]),
			   exit(Pid, finish),
			   {?EMPTY, Context}
	        end;
	  [ {_Key,{ Pid, rule } } ] -> 
		    aim_rule(Pid ,next, Context);
	  [] ->
	      ?DEBUG("~p got empty ~p ~n",[{?MODULE,?LINE}, Index]),
	      {?EMPTY, Context}
      end
;
aim(ProtoType, Context, Index, TreeEts )->
    
%       ?DEBUG("~p etses there ~p~n",[ {?MODULE,?LINE}, {ProtoType,
% 							 ets:tab2list(TreeEts),
% 							 Index} ]),
      case ets:lookup(TreeEts, Index ) of
	  [ {_, { _Pid, fact} }]->  %%change it
	  
		    aim_fact(ProtoType, Context, Index, TreeEts );
	 
	  [ {_Key,{ ?EMPTY, rule } } ] -> 
	      
		 new_rule_process( ProtoType, Context, Index, TreeEts, erlang:self() ),
		%%TODO change such blocks of codes
		
                ?WAIT("~p regis  wait aim rule    ~p ~n",[{?MODULE,?LINE},{ProtoType, Index} ]),

	        receive 
		    {result, Result}->
                        ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{ProtoType, Index} ]),
% 			 stat() 
		         Result;
		    Some ->
                          ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{ProtoType, Index} ]),
			   ?DEBUG("~p got empty ~p ~n",[{?MODULE,?LINE}, Some]),
			   delete_about_me(Index, TreeEts ),
			   {?EMPTY, Context}
	        end;
	  [ {_Key,{ Pid, rule } } ] -> 
		    aim_rule(Pid ,next, Context);
	  [] ->
	      ?DEBUG("~p got empty ~p ~n",[{?MODULE,?LINE}, Index]),
	      {?EMPTY, Context}
      end
.
%% function for work with temp aims
%% TODO rewrite it for use ets - table instead of processes
aim_rule(Pid,  Atom, Context )->
    ?DEBUG("~p call to aim  ~p ~n",[{?MODULE,?LINE},{Pid} ]),
    Pid ! Atom,
    ?WAIT("~p regis  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),

    receive 
	 {result, Result } ->
	      ?DEBUG("~p  reducer result ~p  ~n",[{?MODULE,?LINE}, Result ]),
	      ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),

	      Result;
	  Some ->
 	      ?WAIT("~p GOT  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),
	      ?DEBUG("~p exit aim ~p~n",[{?MODULE,?LINE}, Some]),
	      {?EMPTY, Context}	 
    end     
.

aim_fact(ProtoType, Context, Index, TreeEts )->
 
  { Time, Res } = timer:tc( fact_hbase, get_facts, [ Index, TreeEts ] ),
  
  ?TC("get facts for ~p took ~p ~n",[ ProtoType, Time ] ), 
  
  ?DEBUG("~p got from process fact hbase ~p ",[{?MODULE, ?LINE}, { Index, Res }]),
  
  case  Res of
      []->
	       new_rule_process( ProtoType, Context, Index, TreeEts, erlang:self() ),
       	       ?WAIT("~p regis  wait aim rule    ~p ~n",[{?MODULE,?LINE},{Index, ProtoType} ]),
	       receive 
		    {result, Result}->
			  ?WAIT("~p got  wait aim rule    ~p ~n",[{?MODULE,?LINE}, {Index, ProtoType} ]),
		         Result;
		    Some ->
			  ?WAIT("~p got  wait aim rule    ~p ~n",[{?MODULE,?LINE}, {Index, ProtoType} ]),
			  ?DEBUG("~p exit aim ~p~n",[{?MODULE,?LINE},Some]),
			  {?EMPTY, Context}
	       end;
      [Record]-> 
	      { Record, Context }
      
  end
.

%%working with linked facts this is the first step for implementation expert dynamic  system
worker_linked_rules(Body, Prefix)->
      TreeEts = ets:new(some_name ,[set,public]),
      ets:insert(TreeEts, {?PREFIX, Prefix }),
      FactName = erlang:element(1,Body ),
      ProtoType = tuple_to_list( common:my_delete_element(1, Body) ),
      ets:insert(TreeEts, {?DEBUG_STATUS, false}), %%turn off debugging
      List = ets:lookup( common:get_logical_name(Prefix,?META_LINKS) , FactName),
      foldl4(FactName, List, TreeEts, ProtoType),
      ets:delete(TreeEts)

.


%%DEPRECEATED use API instead
worker_linked_rules_conv( Body)->
      %process_flag(trap_exit, true),
      FactName = erlang:element(1,Body ),
      converter_monitor:regis( erlang:self(), { assert,  Body }  ),
      ProtoType = tuple_to_list( common:my_delete_element(1, Body) ),
      TreeEts = ets:new(some_t1aaable, [public, set] ),    
      ets:insert(TreeEts, {?DEBUG_STATUS, false}), %%turn off debugging
      ?COUNT("~p start adding",[FactName]),
      repeat(3, fact_hbase, add_new_fact, [Body, last]),
%       fact_hbase:add_new_fact( Body, last),     
      List = ets:lookup(?META_LINKS, FactName),
      Res = foldl4(FactName, List, TreeEts, ProtoType),
      ets:delete(TreeEts),
      exit(finish) %%for deleting all process 
	
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


foldl4(FactName, [], _TreeEts, _ProtoType)->
    ?COUNT("~p finish adding",[FactName]),
    true
;
foldl4(FactName, List, TreeEts, ProtoType)->
      foldl4(FactName, List, [] ,true, TreeEts, ProtoType)
.

foldl4(FactName, [ ], _Prev ,true, _TreeEts, ProtoType )->
  ?WAIT("~p finish all linking rules ~p ~p ~n",[{?MODULE,?LINE}, ProtoType, {FactName, true}]),
  ?COUNT("~p finish adding all",[FactName]),
  true
;
foldl4(FactName, [ Head  | Tail], _Prev ,true, TreeEts, ProtoType )->

    {_, _NameFact, Rule   } = Head,
    ?LOG("~p try do rule ~p for ~p ~n",[{?MODULE,?LINE}, Rule, FactName]),
    RuleCall = list_to_tuple([Rule| ProtoType]),
    ?WAIT("~p start next rule ~p ~p ~n",[{?MODULE,?LINE}, Rule, FactName]),
    
    Res = repeat(3, ?MODULE, repeated_linked_rules, [FactName, RuleCall, TreeEts ]),
    ?COUNT("~p finish rule ~p result ~p ~n",[FactName, Rule, Res]),
    
    ?WAIT("~p finish rule ~p ~p ~n",[{?MODULE,?LINE}, Rule, {FactName,Res}]),
    foldl4(FactName, Tail, Head, true, TreeEts, ProtoType)
.


repeated_linked_rules(FactName, RuleCall, TreeEts)->
    Pid = new_rule_process(RuleCall, dict:new(), erlang:now(), TreeEts, self() ),
    Res = linked_loop( FactName, RuleCall ),
    unlink(Pid),
    exit(Pid, finish),  
    Res

.


link_fact(Body, TreeEts)->
  link_fact(Body, TreeEts, ?SIMPLE_HBASE_ASSERT)
.

link_fact( Body, TreeEts, 1 )->
    [{_, RealPrefix}] = ets:lookup(TreeEts, ?PREFIX),
    worker_linked_rules( Body, RealPrefix  )

;
link_fact(_,_,_)->
  true.


linked_loop(FactName, RuleCall)->
      ?WAIT("~p regis  linked rule    ~p ~n",[{?MODULE,?LINE}, {FactName, RuleCall} ]),
      
      Result1=
      receive
	    {result, Result } -> 
		?LOG("~p got result ~p from aim ~p for ~p ~n",
		      [{?MODULE,?LINE}, Result, RuleCall, FactName] ),
	       	true						
		;
		      
	    finish ->
		  ?LOG("~p nothing for  ~p  from ~p ~n",[{?MODULE,?LINE}, FactName, RuleCall]),
		  true
		  ;
	    {'EXIT',Pd,finish} ->
		    ?LOG("~p normal for  ~p  from ~p ~n",[{?MODULE,?LINE}, FactName, RuleCall]),
		    false
		  ;
	    Some ->
		?LOG("~p this ~p  for  ~p  from ~p ~n",[{?MODULE,?LINE}, Some,FactName, RuleCall]),
		false
	  after ?FATAL_WAIT_TIME ->
     		    ?WAIT("~p got  timeout aim loop     ~p ~n",[{?MODULE,?LINE}, {FactName, RuleCall} ]),
		    false
		
      %%bad dessign TODO replace it processing mistake
%       after ?DEFAULT_TIMEOUT->
% 	    ?LOG("~p wait to long for process rule ~p for new fact ~p~n",[{?MODULE,?LINE},RuleCall, FactName ] )

      end,
      ?WAIT("~p GOT  linked rule  ~p ~n",[{?MODULE, ?LINE}, {FactName, RuleCall, Result1} ]), 
      Result1

.

%TODO rewrite to use ets tables instead of processes
%%start the  process for work with temp aim for work with rule
new_rule_process(ProtoType, Context, Index, TreeEts, ParentPid )-> 
     SearchBounded  = bound_aim(ProtoType, Context), %%HACK can be replaced
     {TempSearchBound  ,_} =  prolog_shell:make_temp_aim(SearchBounded),
     ?DEBUG("~p rule process has started ~p ~n",[{?MODULE,?LINE}, TempSearchBound ]),
    Pid = spawn_link(?MODULE, start_rule, [ TempSearchBound, dict:new(), Index, TreeEts, ParentPid ] ),
    ets:insert(TreeEts, { Index, { Pid, rule} } ),
    Pid ! start,
    Pid
.



start_rule(ProtoType, Context, Index, TreeEts, ParentPid)->

	   ?DEBUG("~p start  ~p ~n",[{?MODULE, ?LINE}, {ProtoType, Index}]),
	   Name = element(1,ProtoType),%%from the syntax tree get name of fact
	   Search =  common:my_delete_element(1, ProtoType),%%get prototype
	   
	   converter_monitor:stat(search_rule,  common:get_logical_name(TreeEts, Name) , Search, true ),
	   RulesTable = common:get_logical_name(TreeEts, ?RULES), 
	   RuleList = ets:lookup(RulesTable, Name),
	    %%get all prototypes
	   ?DEBUG("~p got rules by ~p~n",[{?MODULE,?LINE}, {Name, RuleList, Search} ]), %%find matching rules
	   %TODO do not try find all matched prototypes
	   MatchedRules  = lists:foldr(fun(E, Hash)->  
				      Pat = erlang:element(2, E  ) ,
				      ?DEBUG("~p pattern compare ~p ~n",[{?MODULE,?LINE}, {Pat,Search}]),		      
				      case prolog_matching:match( tuple_to_list(Pat), tuple_to_list(Search),[]) of
					  []-> Hash;
					  [ Var ]-> 
						?DEBUG("~p find matched ~p ~n",[{?MODULE,?LINE}, {Var, Pat}]),
						LocalContext = dict:new(),
						NewLocalContext = fill_context( Var,
										 Pat,
										 LocalContext),
						?DEBUG("~p fill context ~p ~n",[{?MODULE,?LINE},
										 dict:to_list(NewLocalContext)]),
 
						[{ Var, erlang:element(3, E), NewLocalContext } | Hash]
				      end
			    end, [], RuleList ),   
	    ?DEBUG("~p aim process rules by ~p~n",[{?MODULE,?LINE}, {Name, Search, MatchedRules} ]),
	    NewIndex = erlang:now(),
            ?WAIT("~p regis   rule    ~p ~n",[{?MODULE,?LINE}, {Index, ProtoType} ]),
	    %start the body of the rule
	    %TODO to ets
	    ChildPid = receive 
			  start ->
			      ?WAIT("~p GOT   rule    ~p ~n",[{?MODULE,?LINE}, {Index, ProtoType} ]),
			      spawn_link(?MODULE, start_matched_rules, [Name, MatchedRules,
									 erlang:self(), NewIndex, TreeEts] )
			end,
	    ChildPid ! start,
	    get_loop(Name, ChildPid, ParentPid, Context, TreeEts, Index, ProtoType)
   %%match return bounded prototype
.
%%get the result
get_loop(Name, ChildPid, ParentPid, Context, TreeEts, Index, MatchedRules)->
    ?WAIT("~p regis  rule result    ~p ~n",[{?MODULE,?LINE}, {Index, ChildPid, MatchedRules} ]),
    receive 
	next -> 
		ChildPid ! next, 
	       ?WAIT("~p got  rule result    ~p ~n",[{?MODULE,?LINE}, {Index, ChildPid} ]),
		get_loop(Name, ChildPid, ParentPid, Context, TreeEts, Index, MatchedRules);
	{result, Result = {?EMPTY,_} }->
		?DEBUG("~p got result from aim ~p~n",[{?MODULE,?LINE}, Result] ),
		ParentPid !  {result, Result },
		converter_monitor:stat(rule,  Name , Result, true ),
		delete_about_me(Index,TreeEts),
		exit(normal);		  
	{result, Result } -> 
		?DEBUG("~p got result from aim ~p~n",[{?MODULE,?LINE}, Result] ),
		converter_monitor:stat(rule,  Name , Result, true ),
		ParentPid !  {result, Result },
               ?WAIT("~p GOt  rule result    ~p ~n",[{?MODULE,?LINE}, {Index, ChildPid} ]),
		get_loop(Name, ChildPid, ParentPid, Context, TreeEts, Index, MatchedRules);
	'exit' ->
		  ?DEBUG("~p exit signal from ~p~n",[{?MODULE,?LINE},ParentPid]),
% 		  ets:delete(TreeEts, Index),
		  ?WAIT("~p GOt  rule result    ~p ~n",[{?MODULE,?LINE}, {Index, ChildPid} ]),
		  ParentPid ! finish,  
		  delete_about_me(Index,TreeEts),
		  exit(normal);		  
	'finish' ->
		  ?DEBUG("~p exit signal from ~p~n",[{?MODULE,?LINE},ParentPid]),
% 		  ets:delete(TreeEts, Index),
		  ?WAIT("~p GOt  rule result    ~p ~n",[{?MODULE,?LINE}, {Index, ChildPid} ]),
% 		  ChildPid ! finish,
		  exit(ChildPid, finish),
		  delete_about_me(Index,TreeEts),
		  exit(normal);		
	 Some ->
		  ?DEBUG("~p exit aim ~p~n",[{?MODULE,?LINE},Some]),
	         ?WAIT("~p GOT  rule result    ~p ~n",[{?MODULE,?LINE}, {Index, ChildPid} ]),
	          delete_about_me(Index, TreeEts),
	          exit(ChildPid, finish),
	          converter_monitor:stat(rule,  Name , Some, false ),
		  ParentPid ! finish
	after ?FATAL_WAIT_TIME ->
     		    ?WAIT("~p got  timeout aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, ChildPid} ]),
     		    delete_about_me(Index, TreeEts),
     		    converter_monitor:stat(rule,  Name , timeout, false ),
     		    exit(ChildPid, finish),
		    true
		
    end
.
delete_about_me(Index,TreeEts)->
    ets:delete(TreeEts, Index).
    
%%function process the last aim in the tree

aim_loop([{false, _ }], _, _, _, _, _, Context, _MainProtoType)->
      {false, Context }
;
aim_loop({false, _ }, _, _, _, _, _, Context, _MainProtoType)->
      {false, Context }
;
aim_loop([{?EMPTY, _Some }], _, _, _, _, _, Context,_MainProtoType)->
	{?EMPTY, Context}
;
aim_loop({?EMPTY, _Some }, _, _, _, _, _, Context,_MainProtoType)->
	{?EMPTY, Context}
;
aim_loop({Result, _SomeContext}, one, ParentPid,  Index, _TreeEts, true, LocalContext, MainProtoType)->
	?DEBUG("~p true process result on one leap ~p ~n",[{?MODULE,?LINE}, {Result,  MainProtoType} ]),
	BoundedSearch = bound_vars(MainProtoType, LocalContext ),
      	ParentPid ! {result, {BoundedSearch, LocalContext} }, %%first time
       ?WAIT("~p regis  aim loop     ~p ~n",[{?MODULE,?LINE}, {ParentPid, MainProtoType} ]),

	receive 
	     finish ->	  
		    ?WAIT("~p got finish aim loop     ~p ~n",[{?MODULE,?LINE}, {ParentPid, MainProtoType} ])
		    ,
		     exit(finish); %%send child finish signal	     
	     next -> 
	           ?WAIT("~p got next  aim loop     ~p ~n",[{?MODULE,?LINE}, {ParentPid, MainProtoType} ]),
		  
		    {?EMPTY, LocalContext};
	     Some ->
		?WAIT("~p got  unexpected aim loop     ~p ~n",[{?MODULE,?LINE}, {Some, MainProtoType} ]),
		?LOG("~p unexpected error ~p ~n",[{?MODULE,?LINE},Some ]),
% 		 finish
		  exit( finish)
	      after ?FATAL_WAIT_TIME ->
		    ?WAIT("~p got  timeout aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		    ParentPid ! finish
	end
;
aim_loop({Result, NewLocalContext}, one, ParentPid,  Index, _TreeEts, Body, LocalContext, MainProtoType)->
		  
	BoundedSearch = bound_vars(MainProtoType, NewLocalContext ),
	?DEBUG("~p process result on one leap ~p ~n",[{?MODULE,?LINE}, {Result, Body, MainProtoType, BoundedSearch,
									  dict:to_list(NewLocalContext)
									  } ]),
      	ParentPid ! {result, {BoundedSearch, NewLocalContext} }, %%first time
 	?WAIT("~p regis  aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),

	receive 
	     finish ->	  
		?WAIT("~p GOT finish aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ])
		,
% 		finish; %%send child finish signal
		exit( finish);
	     next -> 
	     	    ?WAIT("~p GOT next aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		    {?EMPTY, LocalContext};
	     Some ->
     		?WAIT("~p GOT unexpected aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, Some} ]),
		?LOG("~p unexpected error ~p ~n",[{?MODULE,?LINE},Some ]),
		 exit(finish)
% 		 finish
	      after ?FATAL_WAIT_TIME ->
     		    ?WAIT("~p got  timeout aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		    ParentPid ! finish,
		    finish
	end
;
aim_loop({true, NewLocalContext}, Child4Stick, ParentPid,  Index, _TreeEts, Body, LocalContext, MainProtoType)->
	?DEBUG("~p process result on one leap ~p ~n",[{?MODULE,?LINE}, {true, Body, MainProtoType} ]),
			  
	BoundedSearch = bound_vars(MainProtoType, NewLocalContext ),
      	ParentPid ! {result, {BoundedSearch, NewLocalContext} }, %%first time
 	?WAIT("~p regis  aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),

	receive 
	     finish ->	  
		?WAIT("~p GOT finish aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ])
		,
		exit(finish); %%send child finish signal
	     next -> 
	     	    ?WAIT("~p GOT next aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		    {?EMPTY, LocalContext};
	     Some ->
     		?WAIT("~p GOT unexpected aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, Some} ]),
		?LOG("~p unexpected error ~p ~n",[{?MODULE,?LINE},Some ]),
		 exit(finish)
	      after ?FATAL_WAIT_TIME ->
     		    ?WAIT("~p got  timeout aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		    ParentPid ! finish,
		    exit(finish)
	end
;

%%%TODO may be change it to record ??!!!
aim_loop({Result, _SomeContext}, Child4Stick, ParentPid,  
	Index, TreeEts, Body, LocalContext, MainProtoType)->
	?DEBUG("~p process result on one leap ~p ~n",[{?MODULE,?LINE}, {Result, Body} ]),

        {true, NewLocalContext} = hack_match_results_aim(Body, Result, LocalContext),
	BoundedSearch = bound_vars(MainProtoType, NewLocalContext ),
        ?DEBUG("~p result on one leap ~p ~n",[{?MODULE,?LINE}, {MainProtoType, BoundedSearch,
								  dict:to_list(NewLocalContext)  } ]),
  	?WAIT("~p regis  aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
      	ParentPid ! {result, {BoundedSearch, NewLocalContext} }, %%first time 
	receive 
	     finish ->	 
	      	?WAIT("~p GOT finish  aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
% 		finish(TreeEts, Index),    
	      	exit(finish); %%send child finish signal
	     next -> 
		  ?WAIT("~p GOT next aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		  ?TRACE(Index, TreeEts, Body, LocalContext),
		  {Time , Res} =   timer:tc(?MODULE, 'aim', [ Body, LocalContext,  Index, TreeEts ] ),
  		  ?TRACE2(Index, TreeEts, Res, Body),
		  ?TC("~p aim process in  ~p ~n",[{?MODULE,?LINE}, {Res ,Time, Body} ]),
		  
		  aim_loop(Res, Child4Stick, ParentPid,  Index, TreeEts, Body,  LocalContext,MainProtoType)
		  
	after ?FATAL_WAIT_TIME->
	      ?WAIT("~p got  timeout aim loop     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
	      
	      ParentPid ! finish,
	      exit(finish)
	end
.
start_matched_rules(RullName, MatchedRules, ParentPid, NewIndex, TreeEts )->
	?WAIT("~p regis  aim matched rules     ~p ~n",[{?MODULE,?LINE}, {NewIndex, MatchedRules} ]),
	receive
	    start->
		  ?WAIT("~p GOT  aim matched rules     ~p ~n",[{?MODULE,?LINE}, {NewIndex, MatchedRules} ]),
		  ?DEV_DEBUG("~p begin process rules~n",[{?MODULE,?LINE}]),
		  lists:foreach( fun( {Elem, Body, CurLocalContext} )-> 
					  ?DEBUG("~p make conv  ~p~n",[{?MODULE,?LINE}, 
									{ Body, Elem,
									dict:to_list(CurLocalContext) } ]),
					  converter_monitor:stat(call_rule,  RullName, Elem, true ),
					  
					  conv3( Elem,
						  Body, CurLocalContext,
						  ParentPid, NewIndex, TreeEts )
		  end, MatchedRules)
	end,
	?DEBUG("~p finish process rules~n",[{?MODULE,?LINE}]),
	delete_about_me(NewIndex, TreeEts),
	ParentPid ! 'exit'
.	
	      	

finish(Tree, Index)->
    ?DEBUG("~p try finish process ~p",[{?MODULE,?LINE}, {Index}]),
    case ets:lookup(Tree,Index) of
	[{_, {Pid, _} }]->  Pid ! finish, delete_about_me(Index, Tree);
	[] ->
	    true
    end
    
  

.


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



conv3(MainProtoType, { ',', '!', Body }, Context, ParentPid, Index, TreeEts )->
	%start process of fact and rule calculation
	?DEBUG("~p  process ! ~p ~n",[{?MODULE,?LINE}, { Body, dict:to_list(Context) } ]),
	%%%form params for 
	NewIndex  = erlang:now(),
	
	?TRACE(Index, TreeEts, '!', Context),
	case conv3(MainProtoType, Body,Context, ParentPid, NewIndex, TreeEts)	of
	    _AnyThing ->
		ParentPid ! {result,{ ?EMPTY, Context}},
		finish
	end
;
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
%			       }
%			      }  
%			    }
%		}

%%working with our tree
conv3(MainProtoType, { ';', Rule, Body }, Context, ParentPid, Index, TreeEts )->
	?DEBUG("~p  process of ;  ~p ~n",[{?MODULE,?LINE}, {Rule, Body, dict:to_list(Context) } ]),
	%%%form params for 
	?TRACE(Index, TreeEts,';', Context),
	case conv3(MainProtoType, Rule, Context, ParentPid, erlang:now(), TreeEts  ) of 
	    {?EMPTY, _Context1 } -> conv3(MainProtoType, Body, Context, ParentPid, erlang:now(), TreeEts);
	    {false,  _Context1 }->  conv3(MainProtoType, Body, Context, ParentPid, erlang:now(), TreeEts);
	    Res -> 
		  ?LOG("~p unexpected return ~p~n",[{?MODULE,?LINE}, Res ]),
		  {false, Context}
	end
;
conv3(MainProtoType, { ',', {'not', Rule }, Body }, Context, ParentPid, Index, TreeEts ) when is_atom(Rule)->
      conv3(MainProtoType, { ',', {'not', {Rule, true} }, Body }, Context, ParentPid, Index, TreeEts )
;
conv3(MainProtoType, { ',', {'not', Rule }, Body }, Context, ParentPid, Index, TreeEts )->
	%start process of fact and rule calculation
	?DEBUG("~p  process  ~p ~n",[{?MODULE,?LINE}, {Rule, Body, dict:to_list(Context) } ]),
	
	?TRACE(Index, TreeEts, {'not', Rule }, Context ),
      
        NewIndex  = erlang:now(),
 	Params = [ {true, Context}, Rule, Body , Context, ParentPid, NewIndex, TreeEts, MainProtoType ],
	Pid = spawn(?MODULE, not_conv3, [ {'_'}, Rule, Context, erlang:self(), Index, TreeEts] ),
	ets:insert(TreeEts, { Index, { Pid, rule} } ),
	Pid ! start,

	receive
	    finish ->
		    ?LOG("~p process finish  ~n",[{?MODULE,?LINE}]), 
		    ?TRACE2(Index, TreeEts, true, {'not', Rule }),
		    erlang:apply(?MODULE, foldl3, Params);
	    {'EXIT', Pid, Reason}->
		   ?LOG("~p process finish by ~p~n",[{?MODULE,?LINE}, Reason ]), 
		    ?TRACE2(Index, TreeEts, true, {'not', Rule }),
		    erlang:apply(?MODULE, foldl3, Params);
	    Some -> 
		  ?LOG("~p return result ~p~n",[{?MODULE,?LINE}, Some ]),
		  exit(Pid, finish),
		  delete_about_me(Index, TreeEts ),
		  {false, Context}
	end
;
conv3(MainProtoType, { ',', Rule, Body }, Context, ParentPid, Index, TreeEts ) when is_atom(Rule)->
      conv3(MainProtoType,  { ',', { Rule, true }, Body }, Context, ParentPid, Index, TreeEts)    
;
conv3(MainProtoType, { ',', Rule, Body }, Context, ParentPid, Index, TreeEts )->
	%start process of fact and rule calculation
	?DEBUG("~p  process  ~p ~n",[{?MODULE,?LINE}, {Rule, Body, dict:to_list(Context) } ]),
	{Time1, Child4Stick} = timer:tc(?MODULE, start_fact_listener_process,
					[ Rule,Context ,Index, TreeEts ] ),
	?DEBUG("~p start_fact_listener_process   ~p ~n",[{?MODULE,?LINE}, {  Child4Stick } ]),
				
	{Time , Res} =   timer:tc(?MODULE, 'aim' ,[ Rule, Context,  Index, TreeEts] ),
        ?TRACE2(Index, TreeEts, Res, Rule),
	?TC("~p conv3 process in  ~p ~n",[{?MODULE,?LINE}, { Time, Time1 } ]),
	%%%form params for 
	NewIndex  = erlang:now(),
	Params = [ Res, Rule, Body , Context, ParentPid, NewIndex, TreeEts, MainProtoType ],
	partion_results_foldl(Res, Params, Index, TreeEts, Child4Stick, Rule, Context )	
;
conv3(MainProtoType, '!', Context, ParentPid, Index, TreeEts )-> %%last rule in syntax tree 
	%%Search bounded need for hbase
	?DEBUG("~p  process  ! in the ending of rule ~p ~n",[{?MODULE,?LINE}, { dict:to_list(Context) } ]),
	?DEBUG("~p process result on one leap ~p ~n",[{?MODULE,?LINE}, { MainProtoType } ]),
	?TRACE(Index, TreeEts,  '!', Context ),
	BoundedSearch = bound_vars(MainProtoType, Context ),

	ParentPid ! {result, {BoundedSearch, Context} }, %%first time
	?WAIT("~p regis  ! conv     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),

	receive 
	     finish ->	  
		  ?WAIT("~p GOT  ! conv finish     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		   finish; %%send child finish signal
	     next -> 
     		  ?WAIT("~p GOT  next ! conv     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		   ParentPid ! {result, {?EMPTY, Context} },
		   finish;
	     Some ->
    		  ?WAIT("~p GOT  ! conv unexpected     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType,Some} ]),

		  ?LOG("~p unexpected error ~p ~n",[{?MODULE,?LINE},Some ]),
		   finish
	      after ?FATAL_WAIT_TIME ->
		    ?WAIT("~p GOT  ! conv timeout     ~p ~n",[{?MODULE,?LINE}, {Index, MainProtoType} ]),
		    ParentPid ! finish
	end
;

conv3(MainProtoType, { 'not', Body }, Context, ParentPid, Index, TreeEts) when is_atom(Body)->
      conv3(MainProtoType, { 'not', {Body,true} }, Context, ParentPid, Index, TreeEts)    
;
conv3(MainProtoType, { 'not', Body }, Context, ParentPid, Index, TreeEts )->
	%start process of fact and rule calculation
	?DEBUG("~p  process  ~p ~n",[{?MODULE,?LINE}, { Body, dict:to_list(Context) } ]),	
	?TRACE(Index, TreeEts,  { 'not', Body }, Context ),
	
	Pid = spawn(?MODULE, not_conv3, [ {'_'}, Body, Context, erlang:self(), Index, TreeEts] ),
	ets:insert(TreeEts, { Index, { Pid, rule} } ),
	Pid ! start,
	CurrentRes =
	receive
	    finish ->
		    true;
	    {'EXIT', Pid, Reason}->
		   ?LOG("~p process not finish  ~p~n",[{?MODULE,?LINE}, Reason ]), 
		   true;
	    Some -> 
		   ?LOG("~p return for not result ~p~n",[{?MODULE,?LINE}, Some ]),
		   exit(Pid, finish),
		   delete_about_me(Index, TreeEts ),
		   false %%in this architicture almost impossible
	
        end,
        ?TRACE2(Index, TreeEts, CurrentRes, {'not', Body }),
        aim_loop({CurrentRes, Context}, one, ParentPid,   Index, TreeEts, Body, Context, MainProtoType)
;

conv3(MainProtoType, Body, Context, ParentPid, Index, TreeEts) when is_atom(Body)->
      conv3(MainProtoType, {Body,true}, Context, ParentPid, Index, TreeEts)    
;
conv3(MainProtoType, Body, Context, ParentPid, Index, TreeEts )-> %%last rule in syntax tree 
	%%Search bounded need for hbase
	?DEBUG("~p  process  ~p ~n",[{?MODULE,?LINE}, { Body, dict:to_list(Context) } ]),
	{Time1, Child4Stick} = timer:tc(?MODULE, start_fact_listener_process,[Body, Context, Index, TreeEts]),
	{Time , Res} =   timer:tc(?MODULE, 'aim', [ Body, Context,  Index, TreeEts ]),
	
	?TRACE2(Index, TreeEts, Res, Body),
	?TC("~p conv3 process in  ~p ~n",[{?MODULE,?LINE}, {Res, Time, Time1 } ]),
	aim_loop(Res, Child4Stick, ParentPid,   Index, TreeEts, Body, Context, MainProtoType)
.


partion_results_foldl({?EMPTY,_}, _Params, _Index, _TreeEts, 
	      _Child4Stick, Rule, Context  )->
   ?DEV_DEBUG("~p false of rule ~p",[{?MODULE,?LINE}, Rule]),
  {?EMPTY, Context}
;
partion_results_foldl({false,_}, _Params, _Index, _TreeEts, 
	    _Child4Stick, Rule, Context  )->
  ?DEV_DEBUG("~p false of rule ~p",[{?MODULE,?LINE}, Rule]),
  {false, Context}
;
%%system function are not contain dual 
partion_results_foldl(_Res, Params, _Index, _TreeEts, one, _Rule, _Context )->
    
      erlang:apply( ?MODULE, foldl3, Params )
      
;
partion_results_foldl(_Res, Params, Index, TreeEts, Child4Stick, Rule, Context )->

      case erlang:apply( ?MODULE, foldl3, Params )  of 
	    CurrentRes = {?EMPTY, _SomeContext} -> 	    
		      ?TRACE(Index , TreeEts,  Rule, Context),
		      {Time , NewRes} =   timer:tc(?MODULE, 'aim', [ Rule, Context , Index, TreeEts ] ),
		      ?TRACE2(Index, TreeEts, NewRes, Rule),
		      ?TC("~p conv3 process in  ~p ~n",[{?MODULE,?LINE}, {  Rule, Time, NewRes  } ]),
		      [_OldRes | OtherParams] = Params,
		      partion_results_foldl(NewRes, [NewRes| OtherParams], Index, TreeEts, Child4Stick, Rule, Context )
		     ;
	    {false, _}->
      
    		      ?TRACE(Index , TreeEts,  Rule, Context),
		      {Time , NewRes} =   timer:tc(?MODULE, 'aim', [ Rule, Context , Index, TreeEts ] ),
		      ?TRACE2(Index, TreeEts, NewRes, Rule),
		      ?TC("~p conv3 process in  ~p ~n",[{?MODULE,?LINE}, {  Rule, Time, NewRes  } ]),

		       [_OldRes | OtherParams] = Params,
		       partion_results_foldl(NewRes, [NewRes| OtherParams], Index, TreeEts, Child4Stick, Rule, Context )
		    ;   
	    finish ->
		  finish(TreeEts, Index),
		  finish;
	    Result ->
	    
		    Result %%this will never happend i gause
      end
.
%%HACK
not_conv3( MainProtoType, Rule, Context, ParentPid, Index, TreeEts  )->
    receive 
	start->
	    conv3(MainProtoType, Rule, Context, ParentPid, Index, TreeEts)
    end,
    delete_about_me(Index, TreeEts ),
    ParentPid ! finish
.



foldl3( finish, _PrevSearch,
	_Body, LocalContext, _ParentPid, _Index, _TreeEts,  _MainProtoType)->
	finish
;

foldl3([{?EMPTY, _SomeContext}],_,_, LocalContext,_ParentPid,
			    _Index, _TreeEts, _MainProtoType)->
      {?EMPTY, LocalContext};
      
foldl3( [{false, _SomeContext}], _PrevSearch,
	_Body, LocalContext, _ParentPid, _Index, _TreeEts,  _MainProtoType)->
	{false, LocalContext}
;

foldl3({?EMPTY, _SomeContext},_,_, LocalContext,_ParentPid,_Index, _TreeEts, _MainProtoType)->
      {?EMPTY, LocalContext};
      
foldl3( {false, _SomeContext}, _PrevSearch,
	_Body, LocalContext, _ParentPid, 
	_Index, _TreeEts,  _MainProtoType)->
	{false, LocalContext}
;
 %%true return all operators such as + / is of = and == assert
foldl3( { {true}, NewContext} , _PrevSearch,
        Body, _LocalContext, ParentPid, Index, TreeEts,  MainProtoType)->
       
        conv3(MainProtoType, Body, NewContext, ParentPid, Index, TreeEts)
 
;
foldl3( {true, NewContext} , _PrevSearch,
       Body, _LocalContext, ParentPid, Index, TreeEts,  MainProtoType)->
      
       conv3(MainProtoType, Body, NewContext, ParentPid, Index, TreeEts)

;
foldl3( { SearchHead, _Context } , PrevSearch, Body, LocalContext, ParentPid, Index, TreeEts,  MainProtoType)->
      ?DEBUG("~p prepare to the next aim  ~p ~n",[{?MODULE,?LINE}, 
		      {SearchHead,PrevSearch,
		      dict:to_list(LocalContext)} ]),
      [_SearchList|Params] = tuple_to_list(PrevSearch),
	
      NewLocalContext = fill_context(  SearchHead,
				        Params,
				        LocalContext),
      ?DEBUG("~p prepare to the next aim  ~p ~n",[{?MODULE,?LINE},
	      {SearchHead, PrevSearch, dict:to_list(NewLocalContext)} ]),
      conv3(MainProtoType, Body, NewLocalContext, ParentPid, Index, TreeEts)
.









%%ADD rules work

% 
% {',',
%       {fact1,{'X'} }, 
%       
%       {',',{assert,{':-',{fact3,{'X1'}},{',',{fact3,{'X1'}},{fact,{'X5'}}}}},{fact1,{'X1'}}}
%       
%       }

%%TODO optimize this
%%%atom 'one' tell us about none duality of the aim
start_fact_listener_process(  {'read', _X}, Context,Index,  TreeEts)->           
      ?TRACE(Index, TreeEts, 'read', Context),
      one
;
start_fact_listener_process(  {'get_char', _X}, Context,Index,  TreeEts)->           
      ?TRACE(Index, TreeEts, 'get_char', Context),
      one
;


start_fact_listener_process({'fact_statistic',true}, Context,Index,  TreeEts)->           
      ?TRACE(Index , TreeEts,  'fact_statistic', Context),
      one

;
start_fact_listener_process({'system_stat',true}, Context,Index,  TreeEts)->           
      ?TRACE(Index , TreeEts,  'system_stat', Context),
      one

;
start_fact_listener_process({'nl',true}, Context,Index,  TreeEts)->           
      ?TRACE(Index , TreeEts,  'nl', Context),
      one

;
start_fact_listener_process(Body = {'writeln',_}, Context,Index,  TreeEts)->           
      ?TRACE(Index , TreeEts,  Body, Context),
      one

;
start_fact_listener_process(Body = {'create_namespace', _Name}, _Context, _Index,  _TreeEts)->           
      one
;
start_fact_listener_process(Body = {'use_namespace', _Name}, _Context, _Index,  _TreeEts)->           
      one
;
start_fact_listener_process(Body = {'write',_}, Context,Index,  TreeEts)->           
      ?TRACE(Index , TreeEts,  Body, Context),
      one

;
start_fact_listener_process(Body = {'functor',_,_,_}, Context,Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
      one

;
start_fact_listener_process(Body = {'arg',_,_,_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process( Body = {'atom',_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'var',_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'nonvar',_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'integer',_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'atomic',_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'float',_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body ={'list',_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body ={'date',_In,_Type,_Accum}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'op',_,_,_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'assert',_,_,_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'assertz',_,_,_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'asserta',_,_,_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
      one

;
start_fact_listener_process(Body = {'retract',Var }, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
      BodyBounded = prolog_matching:bound_body(Var ,Context),
      Pid =  spawn_link(?MODULE, start_retract_process, [BodyBounded, Context, Index, TreeEts, erlang:self()] ),
      ets:insert(TreeEts, { Index, { Pid, rule } } ),
      Pid

;
start_fact_listener_process(Body = {'meta',_,_,_}, Context, Index,  TreeEts)->  %%links 
        ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'retract',_,_,_}, Context, Index,  TreeEts)->  %%links 
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'retract',_,_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'assert',_,_}, Context,Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'assertz',_,_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'asserta',_,_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'assert',_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'assertz',_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'asserta',_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;

start_fact_listener_process(Body = {'>',_,_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'<',_,_},  Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'=<',_,_},  Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
      one

;
start_fact_listener_process(Body = {'>=',_,_}, Context, Index,  TreeEts)->      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'=:=',_,_}, Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'=\\=',_,_},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'\\=',_,_},  Context, Index,  TreeEts)->      
       
       ?TRACE(Index , TreeEts,  Body, Context),
 	one
;
start_fact_listener_process(Body = {'==',_,_},  Context, Index,  TreeEts)->      
       
       ?TRACE(Index , TreeEts,  Body, Context),
 	one
;
start_fact_listener_process(Body = {'=',_,_},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
      one

;
start_fact_listener_process(Body = {'is',_,_},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'-',_,_},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
 
	one
;
start_fact_listener_process(Body = {'/',_,_},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process(Body = {'*',_,_},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;

start_fact_listener_process(Body = {'+',_,_},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  Body, Context),
	one

;
start_fact_listener_process( {true, true},  Context, Index,  TreeEts)->
      
      ?TRACE(Index , TreeEts,  true, Context),
	one

;
start_fact_listener_process( {false,false},  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  false, Context),
	one

;
start_fact_listener_process(true,  Context, Index,  TreeEts)->
      
      ?TRACE(Index , TreeEts,  true, Context),
	one

;
start_fact_listener_process(false,  Context, Index,  TreeEts)->      
      
      ?TRACE(Index , TreeEts,  false, Context),
	one

;
%sub body
start_fact_listener_process(Body = {';',_,_},  Context, Index,  TreeEts)->      
      fill_rule(TreeEts, Index),
      ?TRACE(Index , TreeEts,  Body, Context),
      ok

;
start_fact_listener_process(Body = {',',_,_},  Context, Index,  TreeEts)->      
      fill_rule(TreeEts, Index),
      ?TRACE(Index , TreeEts,  Body, Context),
      ok
;
start_fact_listener_process(Body, Context, Index, TreeEts)->      

      
      TempSearchBound = bound_aim(Body, Context),  
       ?DEBUG("~p fact process  ~p ~n",[{?MODULE,?LINE}, {Body, TempSearchBound,dict:to_list(Context)} ]),

      {SearchBounded, _} = prolog_shell:make_temp_aim(TempSearchBound),
      ?TRACE(Index , TreeEts,  SearchBounded, Context),
      Pid = fact_hbase:start_fact_process( SearchBounded, Index, TreeEts, erlang:self() ),      
      ?DEBUG("~p fact process has started ~p ~n",[{?MODULE,?LINE}, SearchBounded ]),

      ets:insert(TreeEts, { Index, { Pid, fact} } ),
      Pid ! start,
      ?WAIT("~p regis  fact listener   ~p ~n",[{?MODULE,?LINE}, {SearchBounded, Index} ]),

      ActualPid =
      receive  %sync processes
	    ok ->
	        ?WAIT("~p GOT  fact listener   ~p ~n",[{?MODULE,?LINE}, {SearchBounded, Index} ]),
		ok;
	    finish-> %%fact_hbase delete already info from the TreeEts.
		?DEBUG("~p exit and search rules  ~n",[{?MODULE,?LINE}]),
   	        ?WAIT("~p got  rule listener   ~p ~n",[{?MODULE,?LINE}, {SearchBounded, Index} ]),
		fill_rule(TreeEts, Index),
		ok;
	    Some ->
               ?WAIT("~p  got unexpected  fact listener   ~p ~n",[{?MODULE,?LINE}, {SearchBounded, Index,Some} ]),
		?LOG("~p unexpected error ~p try rule  ~n",[{?MODULE,?LINE},Some ]),
		fill_rule(TreeEts, Index),
	        ok
      end,
      ActualPid
.


fill_rule(TreeEts, Index)->
      ets:insert(TreeEts, { Index, { ?EMPTY, rule} } )
.
 

 
 
 
 
%%% make temp aims

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
	{   lists:reverse(  Res  )++ [NewHead | NewTail ], NewContext2  } %%%%BLYAAAA	
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
    { list_to_atom( "_" ++ atom_to_list(Var)++ "__"++integer_to_list(Mic) ) }
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
    NewVar1+NewVar2;
a_result('mod', NewVar1, NewVar2)->
    NewVar1 rem NewVar2;
a_result('-', NewVar1, NewVar2)->
     NewVar1-NewVar2;
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
  case ets:lookup(Table, Op) of
      [ {Op, Status1, Status2 } ] -> 
	  ?DEBUG("~p find custom operator ~p ",[{?MODULE,?LINE}, {Op, Status1, Status2 } ]),
	 {yes,  Status1, Status2  };
      [ {Op, Status1, Status2, Status3 } ] -> 
	  ?DEBUG("~p find custom operator ~p ",[{?MODULE,?LINE}, {Op, Status1, Status2, Status3 } ]),
	  {yes,  Status1, Status2, Status3  };
      _ -> no;
      []-> no
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
%TODO avoid this by saving facts and rules in one tabl

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
