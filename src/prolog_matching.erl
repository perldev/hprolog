-module(prolog_matching).
-include("prolog.hrl").
-compile(export_all).

% {'+',{'+',{'R1'},{'R'}},{'R4'}}




bound_body(Var = { _ }, Context)->%just variable
         ?DEV_DEBUG_MATCH("~p try find there ~p~n",[{?MODULE,?LINE},{ Var ,dict:to_list(Context)} ]),
	 case  find_var(Context, Var ) of
	       nothing-> Var;
	       Body-> bound_body(Body, Context)
	 end 
;

bound_body({Operator, Var1, Var2}, Context)->
	
         BoundVar2 = bound_body(Var2, Context),
         BoundVar1 = bound_body(Var1, Context),
         ?DEV_DEBUG_MATCH("~p match find there ~p~n",[{?MODULE,?LINE},{ {Operator, Var1, Var2},{Operator, BoundVar1, BoundVar2}, dict:to_list(Context)} ]),
         {Operator, BoundVar1, BoundVar2}   
;
bound_body(Var, Context) when is_tuple(Var)->
	 
	 BoundVar = 
	      lists:map(fun(E)->
			     bound_body(E, Context )
			 end , tuple_to_list(Var) ),
	 list_to_tuple(BoundVar)
;
bound_body(Var, Context) when is_list(Var)->  
     bound_body_list(Var, Context) 
    
;
bound_body(Var, _Context)->
	 Var
.


bound_body_list([], In, _C)->
    lists:reverse(In);

bound_body_list([Head|Tail], In, Context)->
    New = bound_body(Head,Context),
    bound_body_list(Tail,[New|In],Context);
    
bound_body_list(UnBoundTail = {_}, In, Context)->
    New = bound_body(UnBoundTail, Context),
    add_tail_var(In, New)
.
    
    
bound_body_list(List, Context)->
    bound_body_list(List, [], Context).

    
    



    
%%HACK for negative numbers
var_match(Var1, {'-',Some} , Context ) when is_number(Var1)->	
    case Var1 <0  of
	true-> var_match({'-', -1* Var1},{'-',Some}, Context);
	false ->{false, Context}
    end
;
var_match({'-',Some}, Var2, Context ) when  is_number(Var2)->	
    case Var2 <0  of
	true-> var_match({'-',Some}, {'-', -1*Var2 }, Context);
	false ->{false, Context}
    end
;
var_match(_Var1, _Var1 , Context )->	
    {true, Context}
;
var_match({Operator, Var1, Var2  }, {Operator, Var3, Var4  } , Context )->	
      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
						     {{Operator, Var1, Var2  }, {Operator, Var3, Var4 },
						       dict:to_list(Context)} ]),

     case   var_match(Var1, Var3 , Context) of
	  {false, _}->{false, Context};
	  {true, NewContext}->
			  case   var_match(Var2, Var4 , NewContext) of
			      {false, _}->{false, Context};
			      Res ->
				    Res
			  end
    end  
;
var_match({_Operator1, _Var1, _Var2  }, {_Operator, _Var3, _Var4  } , Context )->	
      ?DEV_DEBUG_MATCH("~p fail there ~n",[{?MODULE,?LINE}]),
      {false, Context}
;
var_match( {'_'}, {'_'} , Context )->	
  {true,Context}
;
var_match( {'_'}, _Var2  , Context )->	
  {true,Context}
;
var_match(_Var1, {'_'}, Context )->	
  {true,Context}
;
var_match(Var1 = {_}, Var2={_} , Context )->	
      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
						     {Var1, Var2, dict:to_list(Context)} ]),
						     				     
     BoundVar1 = bound_body(Var1, Context),
     BoundVar2_1  =  bound_body(Var2, Context),
     case BoundVar1 of
	  Var1 -> {true, store_var( { Var1, BoundVar2_1 }, Context) }; 
	  _	-> 
		    var_match(BoundVar1, BoundVar2_1, Context )
      end
;
var_match(Var1 = {_}, Var2 , Context )->	
      BoundVar1 = bound_body(Var1, Context),

      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
						     {{Var1,BoundVar1}, Var2, dict:to_list(Context)} ]),
    					     
      case BoundVar1 of
	  Var1 -> {true, store_var( { Var1, Var2 }, Context) }; 
	  _	-> 
		  case
		  var_match(BoundVar1, Var2, Context) 
		  of
 		      {true, NewContext} -> 
			      NewVar =  bound_body(Var1, NewContext),
			          ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
					     NewVar]),
			      {true, store_var( { Var1, NewVar }, NewContext) };
 		      Got -> 
			  ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE}, Got]),
			  {false, Context}
 		  end
			  
      end
;
var_match(Var1, Var2={_} , Context )->	
      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
						     {Var1, Var2, dict:to_list(Context)} ]),
     BoundVar2 = bound_body( Var2, Context),
     case BoundVar2 of
	  Var2 -> {true, store_var( { Var2, Var1 }, Context) }; 
	  _	-> 
	       case
		   var_match(Var1, BoundVar2, Context ) 
		  of
 		      {true, NewContext} -> 
			?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},bound_body(Var2, NewContext)]),

			  {true, store_var( { Var2, bound_body(Var2, NewContext) }, NewContext) };
 		      Got ->
 		      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE}, Got]),
			{false, Context}
 		  end
 		  
		   
      end
;


var_match(Var1, Body =  { _Operator, _Var3, _Var4  } , Context )->	
      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
						     {Var1, Body, dict:to_list(Context)} ]),
     	BoundBody = bound_body(Body, Context),
	case is_var(Var1) of
		true ->
		      case find_var(Context, Var1) of
			    nothing-> {true, store_var( { Var1, BoundBody }, Context) };
			    Var -> var_match(Var, BoundBody, Context)
		      end;
		_ -> 
		      ?DEV_DEBUG_MATCH("~p ~p ~n",[{?MODULE,?LINE},
						     {Var1, Body, dict:to_list(Context)} ]),
		      {false, Context}
	end 
;
var_match(Body = {_Operator, _Var1, _Var2  }, Var3  , Context )->	
      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
						     {Var3, Body, dict:to_list(Context)} ]),
	BoundBody = bound_body(Body, Context),
  	case is_var(Var3) of
		
		true ->
		      case find_var(Context, Var3) of
			    nothing -> {true, store_var( { Var3, BoundBody }, Context) };
			    Var -> var_match(Var, BoundBody, Context)
		      end;
		_ ->
		    ?DEV_DEBUG_MATCH("~p ~p ~n",[{?MODULE,?LINE},
						     {Var3, Body, dict:to_list(Context)} ]), 
		    {false, Context}
	end 
;


%%there is no variables on this step!!!
%%%but can be lists :(

var_match(Var1, Var2 , Context )->	
      ?DEV_DEBUG_MATCH("~p making matching there ~p ~n",[{?MODULE,?LINE},
						     {Var1, Var2, dict:to_list(Context)} ]),
	case {is_tuple(Var1), is_tuple(Var2)} of
	     {true,true}->
		       VarList1 =  tuple_to_list(Var1),
		       VarList2 = tuple_to_list(Var2),
			prolog_matching:match_common(VarList1, VarList2, Context);  
             _ ->
		  ?DEV_DEBUG_MATCH("~p list there ~n",[{?MODULE,?LINE} ]),
		  case  {is_list(Var1), is_list(Var2) } of
		        {true, true} ->
			      var_match_list( Var1, Var2, Context );
			  _ ->
		           ?DEV_DEBUG_MATCH("~p bad match there ~n",[{?MODULE,?LINE} ]),
			  {false, Context}
		  end
			  

	end
.

var_match_list([], [], Context)->
    {true, Context}

;
var_match_list(X, [], Context) when is_list(X)->
    {false, Context}

;
var_match_list([], X, Context) when is_list(X)->
    {false, Context}

;
var_match_list([Head|List1], [Head|List2], Context)->
    var_match_list(List1,List2,Context)

;

var_match_list([Head|List1], [Head1|List2], Context)->
    case var_match(Head, Head1,Context) of 
	{true,NewContext}->
	     var_match_list(List1,List2,NewContext);
	_ -> {false,Context}
    end
;
var_match_list(Var, List, Context)->
    case var_match(Var, List,Context) of 
	{true,NewContext}->
	    {true, NewContext};
	_ -> {false,Context}
    end
.


match_common([], [], Context)->
	{true, Context}
;
match_common([], List, Context)->
	{false, Context}
;	
match_common(List, [], Context)->
	{false, Context}
;
match_common([Head|VarList1], [Head  |VarList2], Context)->
	match_common(VarList1, VarList2, Context)
;
match_common([Head1| VarList1], [Head2 | VarList2], Context)->
	case var_match(Head1, Head2, Context ) of
	      {true, NewContext}-> match_common(VarList1, VarList2, NewContext);
	      _ -> 
		    ?DEV_DEBUG_MATCH("~p fail common match ~p ",[{?MODULE,?LINE},{Head1, Head2,dict:to_list(Context)}]),
		    {false, Context} %%TODO may be troubles
	end
.




is_var( { Atom } ) when is_atom(Atom)->
    true;    
is_var(X)->
    X.

fill_list(Context, Elem)->
  fill_list(Context, Elem, [])
.

add_tail_var([Head | Tail ], VarName)-> %%BAD HACK
    lists:reverse(Tail) ++ [Head |VarName] 
.


%%matching vars  on step of bounding vars
match_vars(Pretend, Search, Res, Dict )->
    match_vars(Pretend, Search, Res, Pretend, Dict  )
.

match_vars([], [], Res, Pretend, _Dict )->
    [list_to_tuple(Pretend)| Res]
;
match_vars([Head| List], [Search| Tail], Res, Pretend, Dict )->
      case prolog_matching:is_var(Search) of
	    true -> 
		    case find_var( Dict, Search ) of %%val in search wasn't bounded
			  nothing -> NewDict = store_var({Search, Head}, Dict), 
				     match_vars(List, Tail, Res, Pretend, NewDict  );
			  Head ->    match_vars(List, Tail, Res, Pretend, Dict  );
			  Not -> 
			      ?DEV_DEBUG_MATCH("~p match struct ~p~n",[{?MODULE,?LINE}, {Head, Search, Not} ]),
			      Res
			  
		    end;
	    Head -> 
		    match_vars( List, Tail, Res, Pretend, Dict  );
	    Not -> 
		    ?DEV_DEBUG_MATCH( "~p match struct ~p~n",[ {?MODULE,?LINE}, {Head, Search, Not} ] ),
		    case var_match(Search,Head, Dict  ) of
			  {true, NewDict } -> match_vars( List, Tail, Res, Pretend, NewDict  );
			  _ -> Res
		    end
      end		
.


fill_list(_Context, [ ], Res)->
	lists:reverse(Res)
;
fill_list(Context, [Head| Tail], Res)->
       case find_var(Context, Head ) of 
	    nothing -> fill_list(Context, Tail, [ Head| Res]);
	    X -> fill_list(Context, Tail, [ X| Res ])
	end;
	
	
fill_list(Context, Head , Res) ->
	case find_var(Context, Head ) of 
	    nothing -> add_tail_var(Res, Head);
	    X -> fill_list(Context, [], [X| Res ])
 	end
.
	  


find_var_no(Dict, Key)->
  case dict:find(Key, Dict) of
      error ->  Key;
      { ok, Value } ->  Value
  end.  

find_var(Dict, Key)->
  case dict:find(Key, Dict) of
       error ->  nothing;
      { ok, Key} ->  nothing;
      { ok, Value } ->  Value
  end.

store_var({ Name, Value },   In)->
    dict:store(Name, Value, In  )

.


%%matching functions
%%%work only with one PATTERN,means it work with (X+Y,6,X) and pattern (X,Y,Z)
%% make normal match coloborate

match(Whole, Search, Res)->
    Var = dict:new(),
    match(true, Whole, Search, Var, Res, [], Search ).

match(false, _Head, _Search, _Dict, Res, _ , _WholeSearch)->
    Res;
    
match(_, [],[], Dict, Res, RecRes, WholeSearch)->
    %%after cycle fill all varibles if it exists
    ?DEV_DEBUG_MATCH("~p final ~p~n",[{?MODULE,?LINE}, {  Res, RecRes,dict:to_list(Dict) } ]),
    Value = lists:map(fun(Elem)-> %%TODO I DONT LIKE THIS  
			    bound_body(Elem, Dict)
		      end, lists:reverse(RecRes) ),
    ?DEV_DEBUG_MATCH("~p form result matching  ~p~n",[{?MODULE,?LINE}, {Res, Value, WholeSearch,dict:to_list(Dict)} ]),
    match_vars(Value, WholeSearch, Res, Dict) %%match in var context
;
match(_, [], _SearchVal, _Var, Res, _RecRes,_WholeSearch)->%%case search and patterns not equal - we don't fail
  Res
;
match(_, _Head, [], _Var, Res, _RecRes,_WholeSearch)->%%case search and patterns not equal - we don't fail
  Res
;
match(_, [Pattern|Tail],[SearchVal|Search], Context, Res, RecRes, WholeSearch)->

    ?DEV_DEBUG_MATCH("~p process matching  ~p ",[{?MODULE,?LINE},
		  { Pattern, SearchVal, Res, dict:to_list(Context), RecRes } ]),
    
    {Prev, NewContext } =  case is_list(Pattern) of
			    true ->  
				      ?DEV_DEBUG_MATCH("~p proc as list ~n",[{?MODULE,?LINE} ]),
				      match_process_list(true, Pattern, SearchVal, Context, SearchVal );
			    false -> 
				      ?DEV_DEBUG_MATCH("~p proc as var ~n",[{?MODULE,?LINE} ]),
				      process_var(Pattern, SearchVal, Context  )
			end,		    
    ?DEV_DEBUG_MATCH("~p got var ~p~n",[{?MODULE,?LINE}, {Prev, dict:to_list(NewContext) } ]),
    match(Prev, Tail, Search, NewContext, Res, [ Prev| RecRes], WholeSearch  ).

%%%this procedures work only with on item of pattern
process_var(Head, {Var}, Dict ) when is_atom(Var)->
 	?DEV_DEBUG_MATCH("~p  var ~p~n",[{?MODULE,?LINE}, {  Head, {Var}  } ]),
 	{Head, Dict};
process_var(Head, InVar, Context)->
 	?DEV_DEBUG_MATCH("~p  struct ~p~n",[{?MODULE,?LINE}, {  Head, InVar } ]),
	
       case var_match(Head, InVar, Context) of
 	  {true, NewContext}-> {Head, NewContext };
 	  _ ->{false, Context}
       end
     
.


%%in case wheather pattern is list
match_process_list(false, _, _ , Dict, _MatchedStruct)->
      ?DEV_DEBUG_MATCH("~p triggered ~n",[{?MODULE,?LINE}]),
      {false, Dict}
;
match_process_list(true, Pattern, Pattern, Dict, _MatchedStruct )-> %%for constants
      ?DEV_DEBUG_MATCH("~p triggered contstants ~p ~n",[{?MODULE,?LINE},Pattern]),
      {Pattern, Dict}
;
match_process_list(_, [] ,Key = {Val}, Dict, MatchedStruct) when is_atom(Val) -> %%if it is empty list but Search isn't it
  ?DEV_DEBUG_MATCH("~p triggered var pattern ~p ~n",[{?MODULE,?LINE}, MatchedStruct]),
  {MatchedStruct, store_var( { Key, [] }, Dict ) };  
match_process_list(_, Key = {Val}, [], Dict, MatchedStruct) when is_atom(Val) -> %%if it is empty list but Search isn't it
  ?DEV_DEBUG_MATCH("~p triggered var pattern ~p ~n",[{?MODULE,?LINE}, MatchedStruct]),
  {MatchedStruct, store_var( { Key, [] }, Dict ) };
  
match_process_list(_, [], [], Dict, MatchedStruct)-> %%if it is empty list but Search isn't it
  ?DEV_DEBUG_MATCH("~p triggered hidden pattern ~p ~n",[{?MODULE,?LINE}, MatchedStruct]),
  {MatchedStruct ,Dict};
  
match_process_list(_, [], _Search, Dict, MatchedStruct)-> %%if it is empty list but Search isn't it
  ?DEV_DEBUG_MATCH("~p triggered empty pattern ~p ~n",[{?MODULE,?LINE}, MatchedStruct]),
  {false ,Dict};
  
%%if search is not empty 
match_process_list(_,  Val, [], Dict, _MatchedStruct)->%%%it assumes that there is something in Search 
  ?DEV_DEBUG_MATCH("~p triggered unexpected pattern ~p ~n",[{?MODULE,?LINE}, Val]),
  {false ,Dict};

  
%%common process elements
match_process_list(_, [Pattern| TailPattern], [SearchVal| Tail ], Dict, MatchedStruct  )->
	  ?DEV_DEBUG_MATCH("~p main function  of list match  ~p ~n",[{?MODULE,?LINE}, {Pattern, SearchVal} ]),
	  
	  case is_var(Pattern) of
	       true ->
		         {Res, NewDict} = process_var(Pattern, SearchVal, Dict  ),
			 match_process_list(Res, TailPattern, Tail, NewDict, MatchedStruct ) ;
		SearchVal ->
			 match_process_list(SearchVal, TailPattern, Tail, Dict, MatchedStruct );
		_SomeThingElse ->
			{Res, NewDict} = process_var(Pattern, SearchVal, Dict  ),
			match_process_list(Res, TailPattern, Tail, NewDict, MatchedStruct ) 
% % 			 {false, Dict}
	  end
;
%%finish there
match_process_list(_, Pattern, Search, Dict, MatchedStruct  )->
	  ?DEV_DEBUG_MATCH("~p final function  of list match  ~p ~n",[{?MODULE,?LINE}, {Pattern, Search} ]),

      	  case is_var(Pattern) of
	       true ->
		         case process_var(Pattern, Search, Dict  ) of
			      {false, _ }-> {false,Dict};
			      {_, NewDict} -> {MatchedStruct, NewDict}
			 end;
		Search ->
			 {MatchedStruct, Dict};
		SomeThingElse ->

			 case   is_var(Search) of
 			      true -> process_var(Pattern, Search, Dict);
 			      _Pat  ->
% 			      
 				    ?DEV_DEBUG_MATCH("~p triggered unexpected else ~p ~n",[{?MODULE,?LINE}, SomeThingElse]),
 				    {false, Dict}
 			end
	  end
.


% function analog of lists:zip for n lists , return lists of lists

%%list is list of lists for zip
my_zip_n([List, Next ])->
   my_zip(List, Next)
;
my_zip_n([List, Next1, Next2 ])->
   Next = my_zip( List, Next1 ),
   my_zip_l( Next2, Next )
;
my_zip_n([ List ])->
    List
;
my_zip_n([ ])->
    []
;
my_zip_n([ Next1,Next2 | List ])->
    NewList = my_zip( Next1, Next2 ),
    my_zip_n( List, NewList )
.

my_zip_n([],In)->
    In;
my_zip_n([ Head |Tail ], In)->
    NewList = my_zip_l( Head, In ),
    my_zip_n( Tail, NewList)
.


my_zip([],[])->
    [];
my_zip([Head |List1], [Head1| List2])->
    my_zip(List1, List2, [ [ Head , Head1  ] ] ).

my_zip([], [], In)->
    In
;
my_zip(_, [], In)->
    In
;
my_zip([], _, In)->
    In
;
my_zip([ Head |List1], [Head1 |List2], In)->
    my_zip(List1, List2, [ [ Head , Head1  ] | In ] )
.


my_zip_l([],[])->
    [];
my_zip_l([Head |List1], [Head1| List2])->
    my_zip_l(List1, List2, [ [ Head | Head1  ] ] ).


my_zip_l([], [], In)->
    In
;
my_zip_l(_, [], In)->
    In
;
my_zip_l([], _, In)->
    In
;
my_zip_l([ Head |List1], [Head1 |List2], In)->
    my_zip_l(List1, List2, [ [ Head | Head1  ] | In ] )
.



