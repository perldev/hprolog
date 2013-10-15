-module(prolog_built_in).
-export([inner_defined_aim/6]).
-include("prolog.hrl").



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


%%math function 


inner_defined_aim(NextBody, PrevIndex ,Body = { pi,  _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B } = prolog_matching:bound_body( Body, Context),     
     R = math:pi(),
     prolog_matching:var_match(X1B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { sin, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:sin(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { cos, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:cos(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { tan, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:tan(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { asin, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:asin(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { acos, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:acos(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { atan, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:atan(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { atan2, _X1, _X2, _Res  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B, Res } = prolog_matching:bound_body( Body, Context),     
     R = math:atan(X1B, X2B),
     prolog_matching:var_match(Res, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { sinh, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:sinh(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { cosh, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:cosh(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { tanh, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:tanh(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { asinh, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:asinh(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { acosh, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:acosh(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { atanh, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:atanh(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { exp, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:exp(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { log, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:log(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { log10, _X1, _X2  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:log10(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { pow, _X1, _X2 , _Res  }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B, Res } = prolog_matching:bound_body( Body, Context),     
     R = math:pow(X1B, X2B),
     prolog_matching:var_match(Res, R, Context)
;
inner_defined_aim(NextBody, PrevIndex ,Body = { sqrt, _X1, _X2   }, Context, _Index, TreeEts  ) ->
     {_, X1B, X2B } = prolog_matching:bound_body( Body, Context),     
     R = math:sqrt(X1B),
     prolog_matching:var_match(X2B, R, Context)
;
inner_defined_aim(_NextBody, PrevIndex ,Body_ = { 'atom_length', Name, _Length  }, Context, _Index, _TreeEts  ) ->
 
        Body = prolog_matching:bound_body( Body_, Context),
        Atom = erlang:element(2,Body ),
        Count = erlang:element(3, Body ),
        case is_atom(Atom) of
            false ->
                throw({instantiation_error, {Atom, Name} });
            true ->
                 Length =  length(atom_to_list (Atom)  ),
                 case prolog_matching:is_var(Count) of
                        true ->
                             {true,   prolog_matching:store_var( {Count, Length } , Context ) };
                        _->
                            case Count of
                                Length-> { true, Context};
                                _->{false, Context}
                            end
                 
                 end
                
        end   
;     
%%%usual assert
inner_defined_aim(NextBody, PrevIndex ,{ 'assertz', Body = {':-', _ProtoType, _Body1 }  }, Context, Index, TreeEts  ) ->
  inner_defined_aim(NextBody, PrevIndex , { 'assert', Body   }, Context, Index, TreeEts   )
;
inner_defined_aim(_NextBody, PrevIndex ,{ 'assert', Body = {':-', _ProtoType, _Body1 }  }, Context, _Index, TreeEts ) ->
      dynamic_new_rule(Body, last, TreeEts),
      {true, Context}

;
inner_defined_aim(_NextBody, PrevIndex ,{ 'asserta', Body = {':-', _ProtoType, _Body1 }   }, Context, _Index, TreeEts  )->
     dynamic_new_rule(Body, first, TreeEts),
     {true,Context}
    
;


inner_defined_aim(NextBody, PrevIndex ,{ 'assertz', Body   }, Context, Index, TreeEts  ) when is_tuple(Body)->
      inner_defined_aim(NextBody, PrevIndex , { 'assert', Body   }, Context, Index, TreeEts   )
;
inner_defined_aim(_NextBody, PrevIndex ,{ 'assert', Body   }, Context, _Index, TreeEts  ) when is_tuple(Body)->     
      add_fact(Context, Body, last, TreeEts, ?SIMPLE_HBASE_ASSERT)     
;
inner_defined_aim(_NextBody, PrevIndex ,{ 'asserta', Body   }, Context, _Index, TreeEts  ) when is_tuple(Body)->
      add_fact(Context, Body, first, TreeEts, ?SIMPLE_HBASE_ASSERT)
;
inner_defined_aim(_NextBody, PrevIndex, {'inner_retract___', Body }, Context, _index, TreeEts)->
%%only facts
%%TODO rules
     delete_fact(TreeEts, Body, Context,?SIMPLE_HBASE_ASSERT),
     %%it's alway true cause this system predicat  must be used before call(X)
     {true, Context}
; 
inner_defined_aim(_NextBody, PrevIndex, Body = { 'meta', _FactName, _ParamName, Val }, Context, _Index, TreeEts  )->
    NewBody = prolog_matching:bound_body(Body, Context),
    Res = fact_hbase:meta_info(NewBody,  TreeEts),     
    {MainRes ,NewContext} = prolog_matching:var_match(Res, Val, Context),
    {MainRes ,NewContext}
;




% (X,Y) it's almost like  = but in reversed order
inner_defined_aim(_NextBody, PrevIndex, Body_ = { 'copy_term', X, Y }, Context, _Index, _  )->
         prolog_matching:var_match( Y, X, Context)           
;
inner_defined_aim(NextBody, PrevIndex ,Body_ = { 'functor', _SomeTerm, _SomeNameI, _SomeCountI }, Context, _Index, _  )->
        Body = prolog_matching:bound_body( Body_, Context),
        Name = erlang:element(3,Body ),
        Count = erlang:element(4, Body ),
        Term  = erlang:element(2, Body),
        case catch   erlang:element(1, Term) of
            {'EXIT', Reason}->
                throw({type_exception, Reason});
            SomeName->
                SomeCount = erlang:size(Term) - 1,
                prolog_matching:var_match({'functor', Name, Count }, {'functor', SomeName, SomeCount } ,Context)
        end
;
inner_defined_aim(NextBody, PrevIndex, Body_  = {  'arg', _Count, _Body, _Value }, Context, _Index, _  )->
        Body = prolog_matching:bound_body( Body_, Context),
        Number = erlang:element(2, Body),
        Term = erlang:element(3, Body),
        Value = erlang:element(4, Body),
        case catch erlang:element(Number + 1, Term  )  of
               {'EXIT', Reason}->  %means var
                    throw({type_exception, Reason});                    
                Some ->
                      prolog_matching:var_match(Some, Value, Context)
        end
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
                    prolog:inner_change_namespace( fact_hbase:check_exist_table(TableName), Name, TreeEts );
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
inner_defined_aim(NextBody, PrevIndex ,Body = { 'length', _List, _Size   }, Context, _Index, TreeEts  )->
     {_, List, Size } = prolog_matching:bound_body( Body, Context),     
    case catch(length(List)) of
         {'EXIT', Reason} -> throw({unexpected_return_value, Reason });
         Result -> prolog_matching:var_match(Size, Result, Context)
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
              _ -> false 
        end,
      {Res, Context}      
;
inner_defined_aim(NextBody, PrevIndex ,{ 'number', X  }, Context, _Index, _  ) ->
        Body = prolog_matching:bound_body( X, Context),
        Res =
        case Body of
              Body when is_number(Body)->
                  true;
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
              _ -> false 
        end,
        {Res, Context}

    
;

inner_defined_aim(NextBody, PrevIndex ,{ 'compound', X  }, Context, _Index, _  ) ->
    Body = prolog_matching:bound_body( X, Context),
    { case Body  of
        Body_ when  is_number(Body_)->
            false;
        Body_ when  is_atom(Body_)->
            false;    
        {Body_} when is_atom(Body_)->
            fallse;
        _-> true
      end, Context}  

;
inner_defined_aim(NextBody, PrevIndex ,{ 'var', X  }, Context, _Index, _  ) ->
        Body = prolog_matching:bound_body( X, Context),
        Res =
        case Body of
          {Key} when is_atom(Key)->
               true;
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
        Res ->
              ?WRITELN(TreeEts, lists:flatten(io_lib:format("i can parse input ~p",[Res] ) ) ),
              {false, Context}
 
 end
;
inner_defined_aim(NextBody, PrevIndex ,Body = {read_str, X }, Context, _Index, TreeEts)->
    TempX = ?READ_STR(TreeEts), %%read prolog term
    case TempX of
      {'EXIT', Res} ->
            ?WRITELN(TreeEts, lists:flatten(io_lib:format("i can parse input ~p",[Res] ) ) ),
             {false, Context};
       _ ->
              Res = prolog_matching:var_match( TempX, X, Context ),
              Res
 
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


inner_defined_aim(_NextBody, _PrevIndex , Body = {'copy_namespace',  NameSpace2 },  Context, _Index, TreeEts)->
      [ { system_record, ?PREFIX, NameSpace1 } ] = ets:lookup(TreeEts, ?PREFIX),
      Res = prolog:memory2hbase( NameSpace1, NameSpace2),
      {Res, Context}      
;
inner_defined_aim(NextBody, PrevIndex , Body = {'date_diff', _First, _Second, _Type, _Acum },  Context, Index, TreeEts)->
      ?DEV_DEBUG("~p call date function with ~p",[{?MODULE,?LINE},Body]),
      BBody = prolog_matching:bound_body(Body, Context),
      {'date_diff', First, Second, Type, Acum } = BBody,
      case catch common:date_diff(First, Second, Type) of
        {'EXIT', Reason}->
            throw({exception_invalid_date, Reason});
         Res->
            prolog_matching:var_match(Body, {'date_diff', First, Second, Type, Res }, Context) 
      end
;
inner_defined_aim(NextBody, PrevIndex , Body = {'id', _In },  Context, Index, TreeEts)->
      ID = common:generate_id(),
      prolog_matching:var_match(Body, {'id', ID}, Context)       
      
;
inner_defined_aim(NextBody, PrevIndex , Body = {'localtime', _In },  Context, Index, TreeEts)->
      Date = common:get_date(),
      prolog_matching:var_match(Body, {'localtime', Date}, Context)       
;
inner_defined_aim(NextBody, PrevIndex , Body = {'date', _Ini, _Typei, _Accumi },  Context, Index, TreeEts)->
        

      {In, Type, Accum} = bound_vars:bound_vars( common:my_delete_element(1, Body) , Context ),
      ?DEV_DEBUG("~p call date function with ~p",[{?MODULE,?LINE},{In, Type, Accum}]),
      Res =
        case common:get_date(In,Type) of                
          false ->
              {false, Context};
          Var -> 
              prolog_matching:var_match(Body, 
                                    {'date',In, Type, Var }  , Context)
          
        end,
       Res
;
inner_defined_aim(NextBody, PrevIndex , {'is',  NewVarName = { _Accum }, Expr },
                  Context, _Index , _TreeEts )->
        CalcRes = common_process_expr(Expr, Context),
        ?DEBUG("~p calc expression ~p ~n",[{?MODULE,?LINE},{ NewVarName, Expr, CalcRes  } ]),
         prolog_matching:var_match(NewVarName, CalcRes, Context)

;

inner_defined_aim(NextBody, PrevIndex ,{ 'abolish', Body  }, Context, _Index, TreeEts  )->
        BBody = prolog_matching:bound_body(Body, Context),
        Res  = inner_abolish(BBody, TreeEts),
        {Res, Context}
;
% L =.. [ F | Args ],
inner_defined_aim(NextBody, PrevIndex ,{ '=..', First, Second }, Context, _Index, _TreeEts  )->

    BoundFirst = prolog_matching:bound_body(First, Context),
    BoundSecond = prolog_matching:bound_body(Second, Context),
    ?DEV_DEBUG("~p =.. ~p ~n   ",[{?MODULE,?LINE}, {BoundFirst, BoundSecond} ]),
    case prolog_matching:is_var(BoundFirst)  of
        true ->  
                ToTerm = list_to_tuple(BoundSecond),
                {true, prolog_matching:store_var( { BoundFirst, ToTerm }, Context ) };
        _ ->  
            case   prolog_matching:is_var(BoundSecond ) of
                    true->
                          ToTerm = tuple_to_list(BoundFirst),
                          {true, prolog_matching:store_var( { BoundSecond, ToTerm }, Context ) };
                    _->
                         ToTerm = tuple_to_list(BoundFirst),
                         prolog_matching:var_match(ToTerm, BoundSecond, Context)

              end        
    end
;
    
%     ToTerm = tuple_to_list(BoundFirst),
   
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
    Res;
inner_defined_aim(_NextBody, _PrevIndex , Body, _Context, _Index, TreeEts )->
    throw({'EXIT',non_exist, {Body,  TreeEts} } ).


  
%%%TODO test various variants wheather worker_linked_rules is failed or dynamic_new_rule/add_new_fact was failed
add_fact(Context, Body, last, TreeEts,  1)->
      BodyBounded = prolog:bound_aim(Body, Context),
      Name = erlang:element(1, Body),
      CheckResult = {is_deep_rule(Name, TreeEts), is_rule( BodyBounded )},
      ?DEV_DEBUG("~p add fact ~p", [{?MODULE,?LINE}, {CheckResult,BodyBounded}]),
      case CheckResult of 
            { true,_ } -> 
                    dynamic_new_rule(  {':-', Body, true }, last, TreeEts );
            { false, true } -> 
                    dynamic_new_rule(  {':-', Body, true }, last, TreeEts );       
            { false, false }->
%                   link_fact( BodyBounded, TreeEts ),
                    fact_hbase:add_new_fact( BodyBounded, last, TreeEts)
      end,
      ?DEBUG(" add ~p  yes ~n",[BodyBounded ]),
      
      Res = worker_linked_rules(BodyBounded, TreeEts),
      ?WAIT(" add ~p  result is ~p ~n",[Body, Res ]),
      {true,Context}
;
add_fact(Context, Body, first,TreeEts, 1)->
      BodyBounded = prolog:bound_aim(Body, Context),
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
      BodyBounded = prolog:bound_aim(Body, Context),
      dynamic_new_rule(  {':-', BodyBounded, true }, last, TreeEts ),
      {true, Context}
;
add_fact(Context, Body, first, TreeEts, 0)->
      BodyBounded = prolog:bound_aim(Body, Context),
      dynamic_new_rule(  {':-', BodyBounded, true }, first, TreeEts ),
      {true, Context}
. 
 

%TODO avoid this by saving facts and rules in one table
%% comments we are not able to avoid with using HBASE

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
        NameTable = common:get_logical_name(TreeEts, ?RULES),
        ?DEV_DEBUG("~p retracting fact   ~p from ~p",[{?MODULE,?LINE},  NewBody,{ NameTable, {Name, ProtoType, true } }]),
        %%TODO deleting matching rules !!! now we delete only facts
        ets:delete_object(NameTable, {Name, ProtoType, true } );
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
        
        
 %%abolish return always true       
-ifdef(USE_HBASE).
        
inner_abolish({'/', Name, Arity }, TreeEts) when is_atom(Name), is_integer(Arity) ->
       MetaTable = common:get_logical_name(TreeEts, ?META),
       RulesTable = common:get_logical_name(TreeEts, ?RULES), 
       case ets:lookup(MetaTable,Name ) of
                []-> 
                       case ets:lookup(RulesTable, Name) of
                            [] -> true;
                            RulesList  ->
                                    NewRulesList = lists:filter(
                                                    fun({_Name, ProtoType, _Body})->
                                                        Arity  /= tuple_size(ProtoType)                                             
                                                    end, RulesList   ),
                                    ets:delete(RulesTable, Name),
                                    ets:insert(RulesTable, NewRulesList),
                                    [ { system_record, ?PREFIX, NameSpace1 } ] = ets:lookup(TreeEts, ?PREFIX),
                                    prolog:memory2hbase( NameSpace1, NameSpace1),
                                    true
                       end;
                [ {Name, Arity, _HashFunction }  ] -> 
                        ets:delete(MetaTable, Name),
                        fact_hbase:delete_all_fact(Name, TreeEts );               
                 _-> true
                
             end
;
inner_abolish(Name, TreeEts) when is_atom(Name) ->
                RulesTable = common:get_logical_name(TreeEts, ?RULES), 
                case ets:lookup(RulesTable, Name) of
                            [] -> true;
                            RulesList  ->
                                    NewRulesList = lists:filter(
                                                    fun(RuleBody )->
                                                         2  /= tuple_size(RuleBody)
                                                    end, RulesList   ),
                                    ets:delete(RulesTable, Name),
                                    ets:insert(RulesTable, NewRulesList),
                                    [ { system_record, ?PREFIX, NameSpace1 } ] = ets:lookup(TreeEts, ?PREFIX),
                                    prolog:memory2hbase( NameSpace1, NameSpace1),
                                    true
               end

;
inner_abolish(_, _TreeEts) ->
    true
.

-else.

inner_abolish({'/', Name, Arity }, TreeEts) when is_atom(Name), is_integer(Arity) ->
        RulesTable = common:get_logical_name(TreeEts, ?RULES), 
        case ets:lookup(RulesTable, Name) of
                            [] -> true;
                            RulesList  ->
                                    NewRulesList = lists:filter(
                                                    fun({_Name, ProtoType, _Body})->
                                                        Arity  /= tuple_size(ProtoType)                                             
                                                    end, RulesList   ),
                                    ets:delete(RulesTable, Name),
                                    ets:insert(RulesTable, NewRulesList),
                                    true
        end
;
inner_abolish(Name, TreeEts) when is_atom(Name) ->
                RulesTable = common:get_logical_name(TreeEts, ?RULES), 
                case ets:lookup(RulesTable, Name) of
                            [] -> true;
                            RulesList  ->
                                    NewRulesList = lists:filter(
                                                    fun(RuleBody )->
                                                         2  /= tuple_size(RuleBody)
                                                    end, RulesList   ),
                                    ets:delete(RulesTable, Name),
                                    ets:insert(RulesTable, NewRulesList),
                                    true
                                 
               end

;
inner_abolish(_, _TreeEts) ->
    true
.



-endif.
      
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

 
%%%TODO think about it 
%% do we need automatic converting strings to number
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
    prolog:clean_tree(TreeEts),
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




 
 
    