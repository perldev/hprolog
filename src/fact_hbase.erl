-module(fact_hbase).
-compile(export_all).

-include("prolog.hrl").

% {
%   "type": "FilterList",
%   "op": "MUST_PASS_ALL",
%   "filters": [
%     {
%       "type": "RowFilter",
%       "op": "EQUAL",
%       "comparator": {
%         "type": "BinaryComparator",
%         "value": "dGVzdHJvdw\u003d\u003d"
%       }
%     },
%     {
%       "type": "ColumnRangeFilter",
%       "minColumn": "Zmx1ZmZ5",
%       "minColumnInclusive": true,
%       "maxColumn": "Zmx1ZmZ6",
%       "maxColumnInclusive": false
%     }
%   ]
% }
%     {
%         "latestVersion":true, "ifMissing":false, 
%         "qualifier":"aHJlZw==", "family":"Y29sdW1uX2ZhbWlseQ==", 
%         "op":"EQUAL", "type":"SingleColumnValueFilter", 
%         "comparator":{"value":"dmFs","type":"BinaryComparator"}
%     }

% 8> erlog_parse:term(Terms2).                                   
% {ok,{':-',{fact,{'X'}},{fact1,{'X'}}}}
% 9> ok,Terms2,L1} = erlog_scan:string("fact(X):- fact1(X),fact2(X1),fact(X1). ").     
% * 1: syntax error before: '}'
% 9> {ok,Terms3,L3} = erlog_scan:string("fact(X):- fact1(X),fact2(X1),fact(X1). ").
% 
% 10> erlog_parse:term(Terms3).
% {ok,{':-',{fact,{'X'}},
%           {',',{fact1,{'X'}},{',',{fact2,{'X1'}},{fact,{'X1'}}}}}}

create_new_namespace(Prefix)->
    case  check_exist_table(Prefix ++ ?META_FACTS) of
      false ->  create_new_meta_table(Prefix ++ ?META_FACTS),
		create_new_fact_table(Prefix ++ ?RULES_TABLE),
		true;
      _ ->
	  false
    end
.


load_rules2ets(Prefix)->
%       prolog:compile("pro.pl"),
      Scanner  = generate_scanner(1024,<<>>),
      Family = create_hbase_family_filter("description"),
      FamilyLinks = create_hbase_family_filter(?LINKS_FAMILY),
      CacheLinks = create_hbase_family_filter(?CACHE_FAMILY),
      ScannerCache  = generate_scanner(1024,  CacheLinks),
      
      ScannerMeta  = generate_scanner(3,  Family),
      
      ScannerAiMeta  = generate_scanner(1024,  FamilyLinks),
      
      ScannerUrlCacheMeta  = get_scanner(common:get_logical_name(Prefix, ?META_FACTS), ScannerCache),
      ScannerUrlAiMeta  = get_scanner(common:get_logical_name(Prefix, ?META_FACTS), ScannerAiMeta),
      ScannerUrlMeta  = get_scanner(common:get_logical_name(Prefix, ?META_FACTS), ScannerMeta),
      ScannerUrl  = get_scanner(common:get_logical_name(Prefix, ?RULES_TABLE), Scanner),
      ?DEBUG("~p get scanner url ~p ~n",[{?MODULE,?LINE}, ScannerUrl]),
      get_cache_meta_info(ScannerUrlCacheMeta, common:get_logical_name(Prefix, ?HBASE_INDEX) ), %%cach is inner struct 
      get_link_meta_info(ScannerUrlAiMeta, common:get_logical_name(Prefix, ?META_LINKS)  ),
      get_meta_facts(ScannerUrlMeta, common:get_logical_name(Prefix, ?META) ),
      get_and_load_rules(ScannerUrl, common:get_logical_name(Prefix,?RULES) )
.

get_cache_meta_info([], Table)->
    ?LOG("~p empty scanner~n",[{?MODULE,?LINE}])
;
get_cache_meta_info(Scanner, Table)->
	?DEBUG("~p send to ~p ~n",[ { ?MODULE, ?LINE }, {Scanner } ] ),
%  	 httpc:set_options( [ {verbose, debug} ]),
	Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
				    [{connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT }  ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_cache_link( Text1, Table ),
			    get_cache_meta_info(Scanner, Table);%% TODO another solution
			   
                    Res ->
			    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
			     unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            []         
	end.

% [{<<"Row">>,
%                                 [[{<<"key">>,<<"cGF5X29rcG9faXBfYw==">>},
%                                   {<<"Cell">>,
%                                    [[{<<"column">>,
%                                       <<"Y2FjaGU6cGF5X29rcG9faXBfY19p">>},
%                                      {<<"timestamp">>,1364556217799},
%                                      {<<"$">>,<<"MSwy">>}]]}],
%                                  [{<<"key">>,<<"cGF5X29rcG9fb2twbzJfYw==">>},
%                                   {<<"Cell">>,
%                                    [[{<<"column">>,
%                                       <<"Y2FjaGU6cGF5X29rcG9fb2twbzJfY19p">>},
%                                      {<<"timestamp">>,1364552735461},
%                                      {<<"$">>,<<"MSwy">>}]]}],
%                                  [{<<"key">>,<<"dXNlcl9hZ2VudF9j">>},
%                                   {<<"Cell">>,
%                                    [[{<<"column">>,
%                                       <<"Y2FjaGU6dXNlcl9hZ2VudF9jX2k=">>},
%                                      {<<"timestamp">>,1364556239381},
%                                      {<<"$">>,<<"MSwy">>}]]}],
%                                  [{<<"key">>,<<"dXNlcl9vc19j">>},
%                                   {<<"Cell">>,
%                                    [[{<<"column">>,
%                                       <<"Y2FjaGU6dXNlcl9vc19jX2k=">>},
%                                      {<<"timestamp">>,1364556199749},
%                                      {<<"$">>,<<"MSwy">>}]]}]]}]
	
process_cache_link([], Table)-> 
  []
;	
process_cache_link(Meta, Table)  when is_list(Meta)-> 
  process_cache_link( list_to_binary(Meta), Table )
;
process_cache_link(Meta, Table)->
       ?DEBUG("~p got meta ~p ~n ",[ {?MODULE,?LINE}, Meta ] ),

       Json = jsx:decode( Meta ),
       ?DEBUG("~p got  json is ~p ~n",[ {?MODULE,?LINE}, Json ] ),
        [ { _Row, Cache } ] = Json, 
	lists:foreach(fun(Elem)->
 		        ?DEBUG("~p read ~p ", [{?MODULE,?LINE}, Elem ] ), 
			[ {_KeyNameProps, RowName64 }, {_, RowCells }  ] = Elem,	  
			RowName = inner_to_atom( base64:decode(RowName64) ),
			lists:foreach( fun(E)-> 
					  CacheInfo  =  get_val(E, <<"column">>) ,
					  Col = inner_to_atom( cut_family( ?CACHE_FAMILY, CacheInfo )  ),
					  Val = binary_to_list( get_val(E, <<"$">> ) ) ,
					  ?DEBUG("~p insert cache info ~p ~n",[ {?MODULE,?LINE}, 
										   { RowName, Col, Val  } ] ),
					  ets:insert(Table, { RowName, Col, Val  })
					end, 
				      RowCells) 
		      end, Cache )


    
    

.



get_link_meta_info([], Table)->
    ?LOG("~p empty scanner~n",[{?MODULE,?LINE}])
;
get_link_meta_info(Scanner, Table)->
	 ?DEBUG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner } ] ),
%  	 httpc:set_options( [ {verbose, debug} ]),
	
	Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
				    [{connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT }  ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_meta_link( Text1, Table ),
			    get_link_meta_info(Scanner,  Table);%% TODO another solution
			   
                    Res ->
			    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
			     unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            []         
	end
.

% [{<<"Row">>,
%                       [[{<<"key">>,<<"ZmFjdA==">>},
%                         {<<"Cell">>,
%                          [[{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361293817204},
%                            {<<"$">>,
%                             <<"ZmFjdChYKSA6LSBmYWN0MyhYKSAsIGZhY3Q1KFgxKSAsIGZhY3Q2KFgxKS4=">>}]]}],
%                        [{<<"key">>,<<"ZmFjdDI=">>},
%                         {<<"Cell">>,
%                          [[{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361293309241},
%                            {<<"$">>,
%                             <<"ZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDIoWCkgLCBmYWN0MyhYMSkgLCBmYWN0KFgxKS4=">>}],
%                           [{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361293303540},
%                            {<<"$">>,
%                             <<"ZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDIoWCkgLCBmYWN0MyhYMSkgLCBmYWN0KFgxKS4=">>}],
%                           [{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361292801346},
%                            {<<"$">>,
%                             <<"ZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDIoWCkgLCBmYWN0MyhYMSkgLCBmYWN0KFgxKS4=">>}]]}]]}]

process_meta_link([], Table)-> 
    ?LOG("~p empty links ",[ {?MODULE,?LINE}] ),
    []
;
process_meta_link(Meta, Table)  when is_list(Meta)-> 
  process_meta_link( list_to_binary(Meta), Table )
;
process_meta_link(Meta, Table)->
       Json = jsx:decode( Meta ),
       ?DEBUG("~p got meta ~p ~n and json is ~p ~n",[ {?MODULE,?LINE}, Meta, Json ] ),
       [ { _Row, Links } ] = Json, 
       %TODO add weight sorting 
       lists:foreach(fun(Elem)->
 		        ?DEBUG("~p read ~p ", [{?MODULE,?LINE}, Elem ] ), 
			[ {_KeyNameProps, RowName64 }, {_, RowCells }  ] = Elem,	  
			RowName = inner_to_atom( base64:decode(RowName64) ),
			lists:foreach( fun(E)-> 
					  Cache  =  get_val(E, <<"column">>) ,
					  Col = inner_to_atom( cut_family( ?LINKS_FAMILY, Cache )  ),
					  Val = inner_to_atom( get_val(E, <<"$">>) ),
  					  ?DEBUG("~p insert link info ~p ~n",[ {?MODULE,?LINE}, {Col,Val} ] ), 
					  ets:insert(Table, { RowName, Col, Val  })
					end, RowCells) 
					
		      end, Links )
.

get_meta_facts([], Table)->
    ?LOG("~p empty scanner~n",[{?MODULE,?LINE}])
;
get_meta_facts(Scanner, Table)->
	 ?DEBUG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner } ] ),
%  	 httpc:set_options( [ {verbose, debug} ]),
	Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT } ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_meta( Text1, Table ),
			    get_meta_facts(Scanner, Table);%% TODO another solution
			   
                    Res ->
			    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
			     unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            []         
	end
.

get_and_load_rules([], Table )->
    ?LOG("~p empty scanner ~p ~n",[{?MODULE,?LINE},Table]), []
    
;
get_and_load_rules(Scanner, Table)->
	 ?DEBUG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner } ] ),
%  	 httpc:set_options( [ {verbose, debug} ]),
	Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT } ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_code( Text1, Table ),
			    get_and_load_rules(Scanner, Table);%% TODO another solution
			   
                    Res ->
			    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
			     unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            []         
	end
.

% [{<<"Row">>,
%                       [[{<<"key">>,<<"ZmFjdA==">>},
%                         {<<"Cell">>,
%                          [[{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361293817204},
%                            {<<"$">>,
%                             <<"ZmFjdChYKSA6LSBmYWN0MyhYKSAsIGZhY3Q1KFgxKSAsIGZhY3Q2KFgxKS4=">>}]]}],
%                        [{<<"key">>,<<"ZmFjdDI=">>},
%                         {<<"Cell">>,
%                          [[{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361293309241},
%                            {<<"$">>,
%                             <<"ZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDIoWCkgLCBmYWN0MyhYMSkgLCBmYWN0KFgxKS4=">>}],
%                           [{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361293303540},
%                            {<<"$">>,
%                             <<"ZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDIoWCkgLCBmYWN0MyhYMSkgLCBmYWN0KFgxKS4=">>}],
%                           [{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
%                            {<<"timestamp">>,1361292801346},
%                            {<<"$">>,
%                             <<"ZmFjdDIoWCkgOi0gZmFjdDMoWCkgLCBmYWN0NShYMSkgLCBmYWN0NihYMSkuZmFjdDIoWCkgOi0gZmFjdDIoWCkgLCBmYWN0MyhYMSkgLCBmYWN0KFgxKS4=">>}]]}]]}]


process_meta([], Table )->
     []
; 
process_meta(Meta, Table ) when is_list(Meta)->
      process_meta(  list_to_binary(Meta), Table )
;   
process_meta(Meta, Table )->
      Json = jsx:decode( Meta ),
      ?DEBUG("~p got meta ~p ~n",[ {?MODULE,?LINE}, Json ] ),
      [{_Row, Facts }] = Json,
      lists:foldl(fun process_hbase_meta_fact/2, Table, Facts )
.
process_hbase_meta_fact(Fact, Table)->
%       [{<<"key">>,<<"aXA=">>},
%                               {<<"Cell">>,
%                                [[{<<"column">>,<<"ZGVzY3JpcHRpb246Y291bnQ=">>},
%                                  {<<"timestamp">>,1362407186680},
%                                  {<<"$">>,<<"Mg==">>}],
%                                 [{<<"column">>,
%                                   <<"ZGVzY3JpcHRpb246aGFzaF9mdW5jdGlvbg==">>},
%                                  {<<"timestamp">>,1362407176514},
%                                  {<<"$">>,<<"bWQ1">>}]]}]
      [ { <<"key">>,Name64 }, {<<"Cell">>, Columns } ]= Fact,
      Name = list_to_atom( binary_to_list( base64:decode(Name64) ) ),
      Count = get_column(Columns, <<"description:count">>),
      Func = get_column(Columns,<<"description:hash_function">>),
      ?DEBUG("~p got meta info of fact ~p ~n",[{?MODULE,?LINE}, {Name, Count, Func } ]),
      ets:insert(Table, {Name, list_to_integer(Count), Func }),
      Table
.

get_column(Columns, Find)->
    
    lists:foldl(fun(List, I)->
		    { value, { _ ,Val64 } } = lists:keysearch( <<"column">>, 1, List  ),
		    Val = base64:decode(Val64),
		    case Val of
			Find -> 
			       { value, { _ ,Value64 } } = lists:keysearch( <<"$">>, 1, List  ),
			       binary_to_list( base64:decode( Value64 ) );
			_-> I
		    end
		end,"", Columns)
.


process_code([], _Table ) ->
      []
;    
process_code(Code, Table ) when is_list(Code)->
      process_code(  list_to_binary(Code), Table )
;    
process_code(  Code,  Table )->
    Json = jsx:decode( Code ),
    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Json ] ),
    [{_Row, Rules }] = Json,
    lists:foldl(fun process_hbase_rule/2,  Table ,Rules )
.
process_hbase_rule(Elem,  Table)->
      [{_Key, RuleName},{_Cell, Columns }] = Elem,
      ?DEBUG("~p find rule  ~p ~n",[ {?MODULE,?LINE}, Columns ] ),

      [ [ _Column, _TimeStamp, {<<"$">>, Code64 } ] ] = Columns,
      Code = base64:decode(Code64),
      ?DEBUG("~p find rule  ~p ~n",[ {?MODULE,?LINE}, Code ] ),
      CodeList = binary:split(Code, [<<".">>],[global]),
      lists:foldl(fun compile_patterns/2,  Table,  CodeList)
.
compile_patterns(<<>>,  Table)->
    Table

;
compile_patterns( OnePattern,  Table )->
      HackNormalPattern =  <<OnePattern/binary, " . ">>,
      ?DEBUG("~p begin process one pattern   ~p ~n",[ {?MODULE,?LINE}, HackNormalPattern ] ),
     {ok, Terms , L1} = erlog_scan:string( binary_to_list(HackNormalPattern) ),
     Res =  erlog_parse:term(Terms),
     fill_rule_tree(Res,  Table),
     Table
.
fill_rule_tree({  ok , Rule  = {':-',ProtoType, Body} },  Table )->
     Name  = element(1, ProtoType),
     Args =  common:my_delete_element(1, ProtoType),
     ?DEBUG("~p compile rule ~p~n",[{?MODULE,?LINE}, Rule  ]),
     ets:insert( Table,  { Name, Args, Body } )
;
fill_rule_tree( Rule, _Table )->
     ?WAIT("~p compile rule error ~p~n",[{?MODULE,?LINE}, Rule  ])

.



start_fact_process( Aim, Key, TreeEts, ParentPid)->
     spawn_link( ?MODULE, fact_start_link, [ Aim, Key, TreeEts, ParentPid] )
.
%%TODO add rest call to find all facts and rules 
%%% better solution for finding rules and facts
fact_start_link( Aim, Key, TreeEts, ParentPid ) when is_atom(Aim)->
    fact_start_link( {Aim, true}, Key, TreeEts, ParentPid )     
;
fact_start_link( Aim, Key, TreeEts, ParentPid )->
      ?DEBUG("~p check find in facts ~p ~n",[{?MODULE,?LINE},  Aim ]),
      Name = element(1,Aim),%%from the syntax tree get name of fact
%       Search =  common:my_delete_element(1, Aim),%%get prototype
      ?WAIT("~p regis wait in fact ~p ~n",[{?MODULE,?LINE},{Name, Aim, Key} ]),
      FactTable = common:get_logical_name( TreeEts, ?INNER),
      receive 
	  start ->
	      case ets:lookup(FactTable , Name ) of
		[]-> 
		    ?WAIT("~p got wait in fact ~p ~n",[{?MODULE,?LINE},{Aim, Key} ]),
		    ?DEBUG("~p wether it is a rule ~p ~n",[{?MODULE,?LINE}, { Name } ]),
		    case ets:lookup(FactTable, Name) of
			[]->
			      case check_exist_facts( Name, TreeEts ) of %%and try to find in hbase
				true-> 
				      fact_start_link_hbase(Aim, Key, TreeEts, ParentPid);
				false -> 
				      ?DEBUG("~p delete on start ~p ~n",[{?MODULE,?LINE},  Name  ]),
				      delete_about_me(Key, TreeEts),
				      ParentPid ! finish,
				      exit(normal)
			      end;
			R ->  
			      ?DEBUG("~p delete on start ~p ~n",[{?MODULE,?LINE},  Name  ]),
			      delete_about_me(Key, TreeEts),
			      ParentPid ! finish,
			      exit(normal)
		    end;
		_ ->
		    ?DEBUG("~p   start inner cache ~p ~n",[{?MODULE,?LINE},  Name ]),
		    fact_start_link_inner(Aim, Key, TreeEts, ParentPid)
	      end
    end
.



fact_start_link_inner( Aim, Key, TreeEts, ParentPid )->
      Name = element(1,Aim),%%from the syntax tree get name of fact
      Search =  common:my_delete_element(1, Aim),%%get prototype
      ?DEBUG("~p   begin work ~p ~n",[{?MODULE,?LINE},  Aim  ]),
      FactTable = common:get_logical_name( TreeEts, ?INNER),
      
      FactList = ets:lookup(FactTable, Name),
      
      SearchList = lists:map(fun(E)-> common:my_delete_element(1, E) end, FactList ),%% 
      
      PreRes = prolog:recr(SearchList, Search ),
      ?DEBUG("~p  started with ~p ~n",[{?MODULE,?LINE},  PreRes  ]),
      
      case PreRes of
	 [] -> 
	      ParentPid ! finish;
	 _ -> 
	      ParentPid ! ok
      end,
      process_loop(Key, PreRes, TreeEts)
.
      
fact_start_link_hbase( Aim, Key, TreeEts,  ParentPid )->
      Name = element(1,Aim), %%from the syntax tree get name of fact
      Search =  common:my_delete_element(1, Aim),%%get prototype
      ProtoType = tuple_to_list(Search ),
      CountParams = length(ProtoType),
      ?DEBUG("~p generate scanner ~p ~n",[{?MODULE,?LINE}, {Name,ProtoType } ]),
      process_flag(trap_exit, true),%%for deleting scanners
      
      case check_params_facts(Name, TreeEts) of
	  {CountParams, HashFunction} ->
		 case  check_index(ProtoType, Name, TreeEts) of
		      []->
			    HbaseTable =  common:get_logical_name(TreeEts, Name ),  
			    {Scanner, List } = start_recr( atom_to_list(HbaseTable), ProtoType  ),
			     case List of
				[] -> 
				      ParentPid ! finish; %%TODO may be die there
				_ -> 
				      ParentPid ! ok
			      end,
			      process_loop_hbase(Key, Scanner, List, ProtoType, TreeEts);
		       {Name,  PartKey }->
    			      ?DEBUG("~p find whole_key ~p ~n",[{?MODULE,?LINE}, { Name, PartKey } ]),
			       ParentPid ! ok,
			       NameTable =  common:get_logical_name(TreeEts, Name ),  
			       process_indexed_hbase(atom_to_list(NameTable), ProtoType, Key, [PartKey],  TreeEts);
		       {IndexTable , PartKey } ->
			      ?DEBUG("~p got index ~p ~n",[{?MODULE,?LINE}, {{IndexTable, Name} , PartKey } ]),
		              PreRes = get_indexed_records(PartKey, atom_to_list(IndexTable)),
		              case PreRes of
				[] -> 
				      ParentPid ! finish; %%TODO may be die there
				_ -> 
				      ParentPid ! ok
			      end,
			      process_indexed_hbase(atom_to_list(IndexTable), ProtoType, Key, PreRes,  TreeEts)
		end;
	  Res->
	      ?DEBUG("~p count  params not matched ~p ~n",[{?MODULE,?LINE}, {Res, CountParams } ]),
	      ParentPid ! finish	
      end
.


get_indexed_records(PartKey, IndexTable) when is_atom(IndexTable)->
      get_indexed_records( PartKey, atom_to_list(IndexTable) )
;
get_indexed_records(PartKey, IndexTable)->
      {Hbase_Res, Host } = get_rand_host(),
      case catch  httpc:request( get, { Hbase_Res++IndexTable++"/"++PartKey++"/"++?FAMILY,
					  [ {"Accept","application/json"}, {"Host", Host}] 
					},
					  [ {connect_timeout,?DEFAULT_TIMEOUT },
						      {timeout, ?DEFAULT_TIMEOUT } ],
					  [ {sync, true} ] ) of
			  { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
				  ?LOG("~p got indexed keys ~p ~n",[ {?MODULE,?LINE}, Text1 ] ),
				  Res = process_key_data(Text1),
				  Res;
			  Res ->
				  ?LOG("~p got unexpected ~p ~n",[ {?MODULE,?LINE}, Res ] ),
				  [] %%TODO add count of fail and fail exception
      end.

      
%   [{<<"Row">>,
%                                 [[{<<"key">>,
%                                    <<"MDAwMDAwNTQ1NDgxMmUzZjVlYmRlZjk2YzA1Y2RiZjY=">>},
%                                   {<<"Cell">>,
%                                    [[{<<"column">>,<<"cGFyYW1zOjE=">>},
%                                      {<<"timestamp">>,1364553734408},
%                                      {<<"$">>,
%                                       <<"ZTVjMDc5YTg0MmM5MTg5YzRiZjkyMGFiMzZiZTQ3ZDU=">>}]]} ]]  }] 

process_key_data([])->
    []
;
process_key_data(Text1) when is_list(Text1)->
    process_key_data( list_to_binary(Text1) )
;
process_key_data(Meta)->
       ?DEBUG("~p got indexed keys ~p ~n ",[ {?MODULE,?LINE}, Meta ] ),
       Json = jsx:decode( Meta ),
       ?DEBUG("~p got  json is ~p ~n",[ {?MODULE,?LINE}, Json ] ),
        [ { _Row, [ [ Key, { Cell, Vals }  ] ] } ] = Json, 
	lists:map(fun(Elem)->
 		        ?DEBUG("~p read ~p ", [{?MODULE,?LINE}, Elem ] ), 
			    Val = binary_to_list( get_val(Elem, <<"$">> ) ) ,
			    Val
		      end, Vals )
.





process_indexed_hbase(Table, ProtoType, Key, PreRes, TreeEts)->
    	?WAIT("~p regis  wait  hbase   indexed fact  ~p ~n",[{?MODULE,?LINE},{Key, PreRes} ]),
        receive 
	    {PidReciverResult ,get_pack_of_facts} ->
		  ?WAIT("~p GOT  wait in hbase indexed ~p ~n",[{?MODULE,?LINE},{Key, PreRes} ]),
		   ?DEBUG("~p got new request ~p ~n",[{?MODULE,?LINE}, {Key, PreRes} ]),
		   case catch lists:split(?GET_FACT_PACK, PreRes) of
			{'EXIT',R} ->
			    ?DEBUG("~p  indexed fact return  ~p  ~n",[{?MODULE,?LINE}, PreRes ]),
			    PidReciverResult ! [],
			    process_indexed_hbase(Table, ProtoType, Key, [], TreeEts);
   			{NewKey, NewPreRes} ->
			    ?DEBUG("~p indexed fact return  ~p  ~n",[{?MODULE,?LINE}, {NewKey, NewPreRes} ]),
			    Row =  hbase_get_key(ProtoType, Table, ?FAMILY, NewKey),
  			    ?DEBUG("~p got row  ~p  ~n",[{?MODULE,?LINE}, Row ]),
			    PidReciverResult ! Row,
			    process_indexed_hbase(Table, ProtoType, Key, NewPreRes, TreeEts)
		   end;
	      finish->
		  exit(normal);
	     {'EXIT', From, Reason} ->
		  ?WAIT("~p GOT  exit for hbase indexed ~p ~n",[{?MODULE,?LINE}, {From, Reason} ])
	end
.



hbase_get_key(Table,  Key)->
      { Hbase_Res, Host } = get_rand_host(),
       case catch  httpc:request( get, 
				    { Hbase_Res++Table++"/"++Key++"/",
				    [ {"Accept","application/json"}, {"Host", Host }]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT } ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->  
			    Result = process_key_data( Text1),
			    Result ;
			   
                    Res -> 
			    ?DEBUG("~p  got  through key   return ~p ~n  ~p  ~n",
				    [{?MODULE,?LINE},
				    { Table,  Key}, Res ]),
                            []         
	end
.


hbase_get_key(ProtoType, Table, Family, Key)->
      { Hbase_Res, Host } = get_rand_host(),
       case catch  httpc:request( get, 
				    { Hbase_Res++Table++"/"++Key++"/"++Family,
				    [ {"Accept","application/json"}, {"Host", Host }]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT } ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->  
			    Result = process_data( Text1, ProtoType),
			    Result ;
			   
                    Res -> 
			    ?DEBUG("~p  got  key   return ~p ~n  ~p  ~n",
				    [{?MODULE,?LINE},
				    {ProtoType, Table, Family, Key}, Res ]),
                            []         
	end
.


check_index(ProtoType, Name, TreeEts)->
	
	{_, FindKey, OutVals, WholeKeyReverse} = lists:foldl(fun(E, {Index,In, Vals, WholeKey})->
						case prolog_matching:is_var(E) of
						      true->
							  {Index+1,In, Vals,[ integer_to_list(Index) | WholeKey] };
						      Val ->
							  {Index+1, [ integer_to_list(Index) | In],
								[ Val | Vals ], [ integer_to_list(Index) | WholeKey] }
						end
					    end, {1, "",[],""} ,ProtoType),  
	BoundedKey = lists:reverse(OutVals),
	WholeKey = string:join( lists:reverse(WholeKeyReverse), ","),	  
	PossibleKey = string:join( lists:reverse(FindKey), ","),
	TableName = common:get_logical_name(TreeEts, ?HBASE_INDEX),
	Indexes = ets:lookup( TableName, Name),
        ?DEBUG("~p possible key ~p ~n",[{?MODULE,?LINE}, {Indexes, PossibleKey, WholeKey}]),
        
	case   PossibleKey of
	    WholeKey -> {Name, generate_key(BoundedKey) };
	    _ ->
		case ets:lookup(TableName, Name) of
		  [] -> [];
		  List ->
			    case lists:keysearch(PossibleKey, 3, List) of
				false -> [];
				{value, { Name, IndexTable,  PossibleKey  }  } ->
				    Key = generate_key( BoundedKey ),
				    ?DEBUG("~p find  ~p ~n",[{?MODULE,?LINE}, {IndexTable,Key }]),
				    {IndexTable , Key }
				  
			    end       
	      end
      end
.





process_loop_hbase(Key, Scanner, [], ProtoType, TreeEts)->
    receive 
	    {PidReciverResult ,get_pack_of_facts} ->
		   PidReciverResult ! [],
		   process_loop_hbase(Key, Scanner, [], ProtoType, TreeEts);
	    finish ->
		  exit(normal);
	    {'EXIT', From, Reason} ->
     	  	  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE},{Key, ProtoType} ]),
		  ?LOG("~p got exit signal  ~p ~n",[{?MODULE,?LINE}, {From, Reason} ])
    end
;

process_loop_hbase(Key, Scanner, Res, ProtoType, TreeEts)->
	  ?WAIT("~p regis  wait in fact hbase ~p ~n",[{?MODULE,?LINE},{Key, ProtoType} ]),

        receive 
	    {PidReciverResult ,get_pack_of_facts} ->
	  	  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE},{Key, ProtoType} ]),

		   ?DEBUG("~p got new request ~p ~n",[{?MODULE,?LINE}, {Key, Res} ]),
		    PidReciverResult ! Res,
		    NewList = get_data( Scanner, ProtoType),
		    process_loop_hbase(Key, Scanner, NewList, ProtoType, TreeEts);
	    finish ->
		  ?WAIT("~p GOT  finish in fact hbase ~p ~n",[{?MODULE,?LINE},{Key, ProtoType} ]),
		  delete_scanner(Scanner);    
	    {'EXIT', From, Reason} ->
     	  	  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE},{Key, ProtoType} ]),
		  ?LOG("~p got exit signal  ~p ~n",[{?MODULE,?LINE}, {From, Reason} ]),
		  delete_scanner(Scanner);
	    Some ->
    	  	  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE},{Key, ProtoType} ]),
		  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ]),
		  delete_scanner(Scanner)

	end
.



process_loop(Key, [], TreeEts)->
  delete_about_me(Key, TreeEts);
process_loop(Key, PreRes, TreeEts)->
    	 ?WAIT("~p regis  wait in fact inner ~p ~n",[{?MODULE,?LINE},{Key, PreRes} ]),

        receive 
	    finish -> 
		exit(normal);
	    {PidReciverResult ,get_pack_of_facts} ->
		  ?WAIT("~p GOT  wait in fact inner ~p ~n",[{?MODULE,?LINE},{Key, PreRes} ]),
		   ?DEBUG("~p got new request ~p ~n",[{?MODULE,?LINE}, {Key, PreRes} ]),
		   case catch lists:split(?GET_FACT_PACK, PreRes) of
			{'EXIT',R} ->
			    ?DEBUG("~p return  ~p  ~n",[{?MODULE,?LINE}, PreRes ]),
			    PidReciverResult ! PreRes,
			    process_loop(Key, [], TreeEts);
   			{Res ,NewPreRes} ->
			  ?DEBUG("~p return  ~p  ~n",[{?MODULE,?LINE}, {Res, NewPreRes} ]),
			   PidReciverResult ! Res,
			   process_loop(Key, NewPreRes, TreeEts)
		   end

	end
.

get_facts( Key, TreeEts )-> %%in this will not work method of cutting logic results
      case ets:lookup(TreeEts, Key) of
	   [{_, {Pid, fact}}]->  
		    call(Pid, get_pack_of_facts);
	   _ -> [ ] 
	   
      end
.

delete_about_me(Key, TreeEts)->
     ets:insert(TreeEts, {Key, { {}, rule} }) %%after process fact we will try find rules 

.

call(Pid,  Atom )->
    ?DEBUG("~p call to hbase reducer  ~n",[{?MODULE,?LINE} ]),
    Pid! { erlang:self(), Atom },
    ?WAIT("~p regis  wait in  inner fact call  ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),

    receive 
	 Result ->
	    ?WAIT("~p GOT  wait in  inner fact call  ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),
	    ?DEBUG("~p  reducer result ~p  ~n",[{?MODULE,?LINE}, Result ]),
	    Result
	after ?FATAL_WAIT_TIME ->
              ?WAIT("~p GOT  TIMEOUT in  inner fact call  ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),
	      ?DEBUG("~p  reducer didn't return result   ~n",[{?MODULE,?LINE}]),
	      %%TODO delete info from tree_processes ???
	      []
    end     
.

test()->
    
    fact_hbase:start_recr("pay",[<<"26002052615648">>,
				  <<"3285509459">>,
				  <<"14360570">>,
				  <<"29244827109111">>,
				  <<"305299">>,
				  <<"500.00">>,<<"UAH">>, {'DESC'},<<"Q">>,{'D'},<<"82522451">>]).
				  

start_recr(Facts, ProtoType)->
	InIndex = 1,
	{Filters, _I} = lists:foldl(fun(Pat,  {In,Index} )->
					  Elem = integer_to_list(Index),
					  case prolog_matching:is_var(Pat) of
					      true ->   {In, Index+1 };
					      _ -> Var = create_hbase_json_filter( {Pat, Elem} ),  
						    ?DEBUG(" ~p generate ~p~n",[{?MODULE,?LINE}, { Pat, Elem } ]),
						  {<< In/binary, ",", Var/binary>>, Index+1}
					  end
				      end, {<<>>,InIndex}, ProtoType ),
	  Size = length(ProtoType),
	  Main = generate_scanner( ?LIMIT*Size , Filters),
	  ?DEBUG("~p Generate scanner for hbase ~p ~n",[{?MODULE,?LINE},Main ]),
	  Scanner = get_scanner(Facts, Main),
	  List = get_data( Scanner, ProtoType),
	  {Scanner, List }
	  
.

generate_scanner(Limit, Some) when is_integer(Limit)->
      generate_scanner( list_to_binary( integer_to_list(Limit) ), Some)
;
generate_scanner(Limit, Some) when is_list(Limit)->
      generate_scanner( list_to_binary( Limit ), Some)
;
generate_scanner(Limit, <<>>)->
    <<"<Scanner batch=\"",Limit/binary, "\" />">>
;
generate_scanner(Limit, <<  Filter/binary >>)->
    <<"<Scanner batch=\"",Limit/binary, "\" ><filter>", Filter/binary ,"</filter></Scanner>" >>
;
generate_scanner(Limit, << ",", Filters/binary >>)->
    Filter = <<"{  \"type\":\"FilterList\",\"op\":\"MUST_PASS_ALL\",\"filters\":[",
		    Filters/binary, "] }" >>,
    <<"<Scanner batch=\"",Limit/binary, "\" ><filter>", Filter/binary ,"</filter></Scanner>" >>
.
%%TODO ADD SUPERVISOUR FOR all scanners

get_scanner(Facts, Scanner)->
	  
	{Hbase_Res, Host } = get_rand_host(),
	  

 	 ?LOG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner, 
				      Hbase_Res++Facts++"/scanner",
				      erlang:byte_size(Scanner) } ] ),

         case catch  httpc:request( post, { Hbase_Res++Facts++"/scanner",
				[ {"Content-Length", integer_to_list( erlang:byte_size(Scanner) )},
				  {"Content-Type","text/xml"},
				  {"Host", Host}
				],
                                  "application/x-www-form-urlencoded",
                                  Scanner }, [ {connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT } ],
                                 [ {sync, true}, {headers_as_is, true } ] ) of

% 		      {ok,{{"HTTP/1.1",201,"Created"},
%                         [{"location",
%                           "http://avias-db-2.ceb.loc:60050/pay/scanner/1361116056114593f85fb"},
%                          {"content-length","0"}],
%                         []}}
		      {ok,{ResHeader, Headers,[] } }  ->
                            ?LOG("~p got from hbase ~p ",[?LINE,Headers]),
                            case  lists:keysearch("location",1, Headers) of
				{ value, { _H ,Location } } -> store_host(Location, Host ), Location;
				false -> get_scanner(Facts, Scanner)
			    end;
                      Res ->
                            ?LOG("~p got from hbase ~p ",[?LINE,Res]),
                            get_scanner(Facts, Scanner)

      end
.

get_rand_host()->
      List = ets:tab2list(?SCANNERS_HOSTS_TABLE),
      [{Res, Count, Failed } |  NewList] = lists:sort(fun( {_ ,E1, _} , {_,E2,_} )-> E1<E2 end, List),
      ets:insert(?SCANNERS_HOSTS_TABLE, {Res, Count+1, Failed}),
      Res      
.

get_host(Scanner)->
  case  ets:lookup(?SCANNERS_HOSTS_LINK, Scanner ) of
        [{ _ ,Host}]  -> Host;
	 []->		
	      [ $h,$t,$t,$p,$:,$/,$/ | Tail ] = Scanner,
	      Stop = string:chr(Tail, $/),
	      string:sub_string(Tail, 1, Stop-1)	  
		      
  end
.
store_host(Scanner, Host)->
    ets:insert(?SCANNERS_HOSTS_LINK, {Scanner, Host} )
    
.
delete_host(Scanner)->
    ets:delete(?SCANNERS_HOSTS_LINK, Scanner )
.

get_data(undefined, _)->
    [];
get_data([], _)->
    [];
get_data(Scanner, ProtoType)->

% > GET /pay/scanner/13611163935993b6ca66d HTTP/1.1
% > User-Agent: curl/7.27.0
% > Host: avias-db-2.ceb.loc:60050
% > Accept: application/json
	 ?DEBUG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner } ] ),
%  	 httpc:set_options( [ {verbose, debug} ]),
	Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host }]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT } ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    
			    Result = process_data( Text1, ProtoType),
			    Result ;
			   
                    Res -> 
			    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),
			     unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            []         
	end
.


add_link(ASourceFact, AForeignFact, ARuleName, TreeEts )->	
	    SourceFact =  inner_to_list(ASourceFact),
	    ForeignFact =  inner_to_list(AForeignFact),
	    RuleName =  inner_to_list(ARuleName),
	    ?DEBUG("~p assert link ~p~n",[{ ?MODULE,?LINE },
					   { SourceFact,ForeignFact,RuleName } ]),
	    Key = SourceFact,
	    LTableName = common:get_logical_name(TreeEts, ?META_FACTS),
	    MakeCellSet = make_cell_normal(?LINKS_FAMILY, [ { ForeignFact,  RuleName  } ] ),
	    BKey= base64:encode(Key),
	    Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
	    {Hbase_Res, Host } = get_rand_host(),
	    
            case catch  httpc:request( post, { Hbase_Res++LTableName++"/"++Key++"/"++?LINKS_FAMILY,
 				[ {"Content-Length", integer_to_list( erlang:byte_size(Body) )},
				  {"Content-Type","text/xml"},
 				  {"Host", Host}
				],
                                   "application/x-www-form-urlencoded",Body },
                                   [ {connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}, {headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p ",[?LINE,Res]),
                            true

       end
  
  
.

add_new_fact(Body, Pos, TreeEts)->
    add_new_fact(Body, Pos, TreeEts, ?SIMPLE_HBASE_ASSERT)    
.

add_new_fact(Body, Pos, TreeEts,  1) when is_tuple(Body)->
    add_new_fact(tuple_to_list(Body), Pos, TreeEts, 1)
;
add_new_fact([ Name | ProtoType ] , Pos, TreeEts,  1)->
	  ?DEBUG("~p new fact there ~p ~n",[{?MODULE,?LINE}, [ Name | ProtoType ]  ]),
	  CountParams = length(ProtoType),
	  
	  RealName = common:get_logical_name(TreeEts, Name),
	  
	  converter_monitor:stat(try_add,  RealName , ProtoType, true ),
	  
	  case check_params_facts( Name, TreeEts ) of
	      {CountParams, _HashFunction } ->
		  Params = prototype_to_list(ProtoType),
		  index_work_add(Name, Params, TreeEts),%%TODO spawn this
		  Res = store_new_fact(RealName, Params ),
		  converter_monitor:stat(add,  RealName , ProtoType, Res ),
		  Res;
		  
		  
	      {CountParams1, _HashFunction } ->
		  ?DEBUG("~p params have not matched with exists~p",[{?MODULE,?LINE}, {CountParams1,CountParams } ]),
		  false
		  ;
	      false -> 
		  ?DEBUG("~p create new table ~p ~n",[{?MODULE,?LINE}, RealName ]),
		  create_new_fact_table(RealName),
		  ?DEBUG("~p store meta info ~p ~n",[{?MODULE,?LINE} ]),

		  ets:insert( common:get_logical_name(TreeEts, ?META), {Name, CountParams, "md5"} ),
		  store_meta_fact(Name,  common:get_logical_name(TreeEts, ?META_FACTS) , [
					  {"count" ,integer_to_list( CountParams) }, 
					  {"hash_function","md5" },
					   {"facts_count", "1" }
					  ]   ),%TODO NEW HASH function
		  Res = store_new_fact(RealName , prototype_to_list(ProtoType) ),
		  converter_monitor:stat(add, RealName, ProtoType, Res ),
		  Res
	  end
;
add_new_fact(Body, Pos,TreeEts,  Something) when is_list(Body)->
    add_new_fact(list_to_tuple(Body), Pos, TreeEts, Something)
;
add_new_fact(Body, last, TreeEts, _Something) ->
    Table = common:get_logical_name( TreeEts, ?INNER),
    ets:insert(Table, Body)
;
add_new_fact(Body, first, TreeEts,  _Something) ->
    Name = erlang:element(1, Body),
    Table = common:get_logical_name( TreeEts, ?INNER),
    case ets:lookup( Table , Name) of
	  []->
	      ets:insert(Table, Body);
	  List->
	      ets:delete(Table, Name),
	      ets:insert(Table, [Body| List ])
    end    
.


put_key_body(Name, Key, Body )->
	    LTableName = atom_to_list( Name ),
	    BKey= base64:encode(Key),
	    SendBody = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", Body/binary, "</Row></CellSet>" >>,    
	    ?DEBUG("~p new fact body with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Body ] ),
	    {Hbase_Res, Host } = get_rand_host(),
            case catch  httpc:request( post, { Hbase_Res++LTableName++"/"++Key++"/"++?FAMILY,
 				[ {"Content-Length", integer_to_list( erlang:byte_size(SendBody) )},
				  {"Content-Type","text/xml"},
 				  {"Host", Host}
				],
                                   "application/x-www-form-urlencoded",SendBody },
                                   [ {connect_timeout,?DEFAULT_TIMEOUT },{timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}, {headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p ~n",[?LINE,{Res,Name,Body }]),
                            false

	    end


.

hbase_add_index(IndexTable, Type, Params)->
      Keys =  string:tokens(Type, ","),
      KeyList = lists:map(fun(E)->
		      Index = list_to_integer(E),
		      lists:nth(Index, Params )
		 end,
		 Keys
		 ),
      Key = generate_key( KeyList ),
      MD5Key = generate_key( Params ),
      ?DEBUG("~p process delete index ~p",[{?MODULE,?LINE}, {Key,KeyList }  ]),
      MakeCellSet = make_cell_set( [ MD5Key ] ),
      converter_monitor:stat(try_add_index,  IndexTable , { Type,MD5Key, Key }, true ),
      Res = put_key_body(IndexTable, Key, MakeCellSet),
      converter_monitor:stat(add_index,  IndexTable , { Type,MD5Key, Key }, Res ),      
      Res
.

hbase_del_index(IndexTable, Type, Params)->
      Keys =  string:tokens(Type, ","),
      KeyList = lists:map(fun(E)->
		      Index = list_to_integer(E),
		      lists:nth(Index, Params )
		 end,
		 Keys
		 ),
      Key = generate_key( KeyList ),
      ?DEBUG("~p process delete index ~p",[{?MODULE,?LINE}, {Key,KeyList }  ]),
      converter_monitor:stat(try_del_index,  IndexTable , { Type, Key }, true ),
      Res = del_key(Key, IndexTable ),
      converter_monitor:stat(del_index,  IndexTable , { Type, Key }, Res ),
      Res
.

index_work_add(Name, Params, TreeEts)->
     Key = generate_key( Params ),
     Table = common:get_logical_name( TreeEts, ?HBASE_INDEX),
     case  ets:lookup( Table, Name) of
	   [] -> [];
	   List ->
		  lists:foreach(fun({_InKey, IndexTable, KeyName})->
					hbase_add_index(IndexTable, KeyName, Params)
				 end, List)
    end
.

index_work_del(Name,  Params, TreeEts) when is_tuple(Params)->
   index_work_del(Name,  tuple_to_list(Params), TreeEts) 
;
index_work_del(Name, Params, TreeEts)->
     Key = generate_key( Params ),
     RealName = common:get_logical_name(TreeEts, ?HBASE_INDEX),
     case  ets:lookup(RealName, Name) of
	   [] -> [];
	   List ->
		  lists:foreach(fun({_InKey, IndexTable, KeyName})->
				      hbase_del_index(IndexTable, KeyName, Params  )
		  end, List)   
     end


.	


check_params_facts(Name, TreeEts) when is_list(Name)->
      check_params_facts( list_to_atom(Name), TreeEts )


;
check_params_facts(Name, TreeEts) ->

     case ets:lookup( common:get_logical_name(TreeEts, ?META), Name  ) of
	  [ {Name, Count, HashFunction }  ] -> {Count, HashFunction};
	  []-> false
     end


.

check_exist_facts(Name, TreeEts) when is_list(Name)->
      check_exist_facts( list_to_atom(Name), TreeEts )


;
check_exist_facts(Name, TreeEts) ->
     case ets:lookup( common:get_logical_name( TreeEts, ?META), Name  ) of
	  [ Var ] -> true;
	  []-> false
     end
.



add_new_rule(Tree = { ':-' ,ProtoType, BodyRule}, Pos, TreeEts )->
  TableName = common:get_logical_name(TreeEts,?RULES_TABLE),
  add_new_rule(Tree, Pos, TableName, ?SIMPLE_HBASE_ASSERT )
.
add_new_rule(Tree = { ':-', ProtoType, BodyRule}, Pos, TableName, 1 )->
	?DEBUG("~p new rule to hbase  ~p ~n",[ {?MODULE,?LINE}, Tree ] ),
       [ Name | _ProtoType ] = tuple_to_list(ProtoType),
       PrologCode =  erlog_io:write1(Tree),
       V2 = list_to_binary( lists:flatten(PrologCode)++"." ),
      	?DEBUG("~p new rule table is  ~p ~n",[ {?MODULE,?LINE}, TableName ] ),

       case check_exist_rule( TableName ) of
	    false -> 
		store_new_rule(Name, V2, TableName  );
	     PrevCode ->
		case Pos of 
		    first ->
			  store_new_rule(Name, <<V2/binary,PrevCode/binary>>, TableName );
		    _-> store_new_rule(Name, <<PrevCode/binary, V2/binary >>, TableName )
		end
	end
;
add_new_rule(_Tree, _Pos, _TableName, _ )->
    true.


del_link_fact(Fact1, Fact2, TreeEts) when is_atom(Fact1) ->
	del_link_fact(atom_to_list(Fact1), Fact2, TreeEts)
;
del_link_fact(Fact1, Fact2, TreeEts) when is_atom(Fact2) ->
	del_link_fact(Fact1, atom_to_list(Fact2), TreeEts )
;

del_link_fact(Fact1, Fact2, TreeEts )->
      TableName = common:get_logical_name(TreeEts, ?META_FACTS),
      del_key(Fact1, TableName, ?LINKS_FAMILY, Fact2 )
.

del_fact(B, TreeEts)->
  del_fact(B, TreeEts, ?SIMPLE_HBASE_ASSERT)
.
del_fact(B, TreeEts,  1)->
      ?DEBUG("~p begin process of delete ~p",[{?MODULE,?LINE}, B]),
      Name = erlang:element(1,B),

      RealName = common:get_logical_name(TreeEts, Name),
      
      ProtoType = common:my_delete_element(1,B),
      ?DEBUG("~p delete stat ~p",[{?MODULE,?LINE}, RealName ]),

      converter_monitor:stat(try_del,  RealName , ProtoType, true ),
      ?DEBUG("~p delete index ~p",[{?MODULE,?LINE}, RealName ]),

      index_work_del(Name, ProtoType, TreeEts),
       ?DEBUG("~p delete key ~p",[{?MODULE,?LINE}, RealName ]),

      Res = del_key( generate_key(ProtoType), RealName),
       ?DEBUG("~p delete has result ~p",[{?MODULE,?LINE}, {Res,RealName} ]),

      converter_monitor:stat(del,  RealName , ProtoType, Res),
      Res
      
;
del_fact(_,_,_)->
    true.


del_rule(Rule, TreeEts)->
  del_rule(Rule, TreeEts, ?SIMPLE_HBASE_ASSERT)    
.
del_rule(Rule, TreeEts, 1)->
  TableName = common:get_logical_name(TreeEts, ?RULES_TABLE ),
  del_key(Rule, TableName);
del_rule(_,_,_)->
  true.


del_key(Key, TableName, Family, Col )->
      
	{Hbase_Res, _Host } = get_rand_host(),
	
	case catch  httpc:request( delete, { Hbase_Res ++ TableName ++ "/" ++ Key ++ "/" ++ Family ++ ":" ++ Col ,
						[]}, [{connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }],
                                [ {sync, true}, {headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process delete data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Key ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p for key ~p  ",[?LINE,Res, {Key, TableName}]),
                            false

	    end
.


del_key(Key, TableName ) when is_atom(TableName) ->

  del_key(Key, atom_to_list(TableName) )
;
del_key(Key, TableName ) when is_atom(Key) ->

  del_key(atom_to_list(Key), TableName )
;
del_key(Key, TableName )->
	   {Hbase_Res, _Host } = get_rand_host(),
            case catch  httpc:request( delete, { Hbase_Res++TableName++"/"++Key, [] },
				  [ {connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}  ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process delete data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Key ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p for key ~p  ",[?LINE, Res , {Key, TableName}]),
                            false

	    end
.

store_new_rule(Key, Body, TableName) when is_atom(Key)->
    store_new_rule(atom_to_list(Key), Body, TableName)
;
store_new_rule(Key, Body, TableName) when is_integer(Key)->
    store_new_rule(integer_to_list(Key), Body, TableName)
;
store_new_rule(Key, Body, LTableName)->
	    Val =  base64:encode( Body ),
	    Cell = base64:encode( ?FAMILY ++":"++ ?CODE_COLUMN ),
	    MakeCellSet = <<"<Cell column=\"",Cell/binary,"\">",Val/binary,"</Cell>" >>,
	    BKey= base64:encode(Key),
	    XmlBody = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
	    {Hbase_Res, Host } = get_rand_host(),
            case catch  httpc:request( post, { Hbase_Res++LTableName++"/"++Key++"/"++?FAMILY,
 				[ {"Content-Length", integer_to_list( erlang:byte_size(XmlBody) )},
				  {"Content-Type","text/xml"},
 				  {"Host", Host}
				],
                                   "application/x-www-form-urlencoded",
                                   XmlBody }, [ {connect_timeout,?DEFAULT_TIMEOUT },{timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}, {headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p ",[?LINE,Res]),
                            false

	    end
.

check_exist_table(TableName)->
	    {Hbase_Res, Host } = get_rand_host(),
	   
            case catch   httpc:request( 'get', { Hbase_Res,
				    [  {"Host", Host}]},
				    [{connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT }  ],
				    [ {sync, true} ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
			    	lists:member(TableName, string:tokens(Text1, "\n") );
                        Res ->
                            ?DEBUG("~p got from hbase ~p ",[?LINE,Res]),
                            false

	    end
    

.


check_exist_rule(Name) when is_atom(Name)->
 LName = atom_to_list(Name),
 check_exist_rule(LName)
;
check_exist_rule(Name) when is_integer(Name)->
 LName = integer_to_list(Name),
 check_exist_rule(LName)
;
check_exist_rule(LName)->
   
    {Hbase_Res, Host } = get_rand_host(),
    case catch  httpc:request( 'get', { Hbase_Res++?RULES_TABLE++"/"++LName++"/"++?FAMILY++":"++?CODE_COLUMN,
				    [ {"Accept","application/json"}, {"Host", Host}]},
				    [{connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT }  ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
% 				    "{\"Row\":[{\"key\":\"ZmFjdA==\",\"Cell\":[{\"column\":\"cGFyYW1zOmNvZGU=\",\"timestamp\":1361291589351,\"$\":
% 				    \"ZmFjdChYKSA6LSBmYWN0MShYKSAsIGZhY3QyKFgxKSAsIGZhY3QoWDEpLg==\"}]}]}"
				Json = jsx:decode(list_to_binary(Text1)),
				?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Json ] ),
				process_code_injson(Json);
% 				{"table":[{"name":"pay"},{"name":"table"}]}
		     Res ->
			    ?DEBUG("~p got ~p  nothing found ~n",[ {?MODULE,?LINE}, Res ] ),                           
                           false
    end
.

process_code_injson(Json)->
  % 			    	 [{<<"Row">>,
% 				      [[{<<"key">>,<<"ZmFjdA==">>},
% 					  {<<"Cell">>,
% 					    [[{<<"column">>,<<"cGFyYW1zOmNvZGU=">>},
% 					      {<<"timestamp">>,1361291589351},
% 					      {<<"$">>,
% 					      <<"ZmFjdChYKSA6LSBmYWN0MShYKSAsIGZhY3QyKFgxKSAsIGZhY3QoWDEpLg==">>}]]}]]}] 

	 %%TODO get HEAD_VERSION at first
      	 [{<<"Row">>,[[ _Key, {<<"Cell">>, Columns   }]]}] = Json,
      	 CodeList  = 
	       lists:map(fun(Elem)->
			    [_Column, _TimeStamp, {<<"$">>, Code }] = Elem,
			    base64:decode(Code)
			 end ,Columns),
	  [ HeadVersion |Tail ] = CodeList,
	  ?DEBUG("~p code list ~p ~n",[ {?MODULE,?LINE}, CodeList ] ),
	  HeadVersion
.
create_new_meta_table( LTableName )->
		 TableName = list_to_binary( LTableName ),
		 Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
			  "<TableSchema name=\"",TableName/binary,"\" IS_META=\"false\" IS_ROOT=\"false\">"
			  "<ColumnSchema name=\"description\"  VERSIONS=\"1\" BLOCKSIZE=\"65536\"  COMPRESSION=\"NONE\" />"
			  "<ColumnSchema name=\"links\"  VERSIONS=\"1\" BLOCKSIZE=\"65536\"  COMPRESSION=\"NONE\" />"
			  "<ColumnSchema name=\"stat\"  VERSIONS=\"1\" BLOCKSIZE=\"65536\"  COMPRESSION=\"NONE\" />"
			  "<ColumnSchema name=\"cache\"  VERSIONS=\"1\" BLOCKSIZE=\"65536\"  COMPRESSION=\"NONE\" />"
			  "</TableSchema>" >>,
		 {Hbase_Res, Host } = get_rand_host(),	  
                 case catch  httpc:request( post, { Hbase_Res ++ LTableName ++ "/schema",
				[ {"Content-Length", integer_to_list( erlang:byte_size(Body) )},
				  {"Content-Type","text/xml"},
				  {"Host", Host}
				],
                                  "application/x-www-form-urlencoded", Body },
                                  [ {connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }  ],
                                 [ {sync, true}, {headers_as_is, true } ] ) of
		      { ok, { { _NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } -> 
			       ?DEBUG("~p create new  table  ~p ~n~n~n",[{?MODULE,?LINE}, {TableName} ] ),
				true;
                      Res ->
                            ?WAIT("~p got from hbase ~p ",[?LINE,Res]),
                            []

		  end
.



create_new_fact_table( Name ) when is_atom(Name)->
    create_new_fact_table( atom_to_list(Name) );
create_new_fact_table( LTableName )->
		 TableName = list_to_binary( LTableName ),
		 Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
			  "<TableSchema name=\"",TableName/binary,"\" IS_META=\"false\" IS_ROOT=\"false\">"
			  "<ColumnSchema name=\"params\"  VERSIONS=\"1\" BLOCKSIZE=\"65536\"  COMPRESSION=\"NONE\" "
			  "  /></TableSchema>" >>,
		 {Hbase_Res, Host } = get_rand_host(),	  
                 case catch  httpc:request( post, { Hbase_Res ++ LTableName ++ "/schema",
				[ {"Content-Length", integer_to_list( erlang:byte_size(Body) )},
				  {"Content-Type","text/xml"},
				  {"Host", Host}
				],
                                  "application/x-www-form-urlencoded", Body },
                                  [ {connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }  ],
                                 [ {sync, true}, {headers_as_is, true } ] ) of
		      { ok, { { _NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } -> 
			       ?DEBUG("~p create new  table  ~p ~n~n~n",[{?MODULE,?LINE}, {TableName} ] ),
				true;
                      Res ->
                            ?WAIT("~p got from hbase ~p ",[?LINE,Res]),
                            []

		  end
.

store_new_fact(Name, LProtoType) when is_atom(Name)->

    store_new_fact(atom_to_list( Name ), LProtoType)
;
store_new_fact(LTableName, LProtoType)->
%     my  $POST = qq[<?xml version="1.0" encoding="UTF-8" standalone="yes" ?><CellSet><Row key="$key">$cell_set</Row></CellSet>];
%     my $str = qq[curl -X POST -H "Content-Type: text/xml" --data '$POST'  $BASE/$key_url/params];
%   my $key  = md5_hex( join(",",@arr) );
% %       POST /<table>/schema
% http://avias-db-2.ceb.loc:60050/table/key_name/family

	    Key = generate_key( LProtoType ),
	    MakeCellSet = make_cell_set( LProtoType ),
	    BKey= base64:encode(Key),
	    Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
	    ?DEBUG("~p new fact body with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Body ] ),
	    {Hbase_Res, Host } = get_rand_host(),
            case catch  httpc:request( post, { Hbase_Res++LTableName++"/"++Key++"/"++?FAMILY,
 				[ {"Content-Length", integer_to_list( erlang:byte_size(Body) )},
				  {"Content-Type","text/xml"},
 				  {"Host", Host}
				],
                                   "application/x-www-form-urlencoded",Body },
                                   [ {connect_timeout,?DEFAULT_TIMEOUT },{timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}, {headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p ~n",[?LINE,{Res,LTableName,LProtoType }]),
                            false

	    end
.
make_cell_set(ProtoType)->
    {Res, Index } = lists:foldl(fun(E, {In, Index} )->
			Val =  base64:encode(  unicode:characters_to_binary(E) ),
			Cell = base64:encode( ?FAMILY ++":"++ integer_to_list(Index) ),
			CellText = <<"<Cell column=\"",Cell/binary,"\">",Val/binary,"</Cell>" >>,
			NewIn = <<In/binary, CellText/binary>>,
			{NewIn, Index + 1}
		end, {<<>>,1} ,ProtoType),
      Res
.


store_meta_fact(Name, MetaTable, LProtoType) when is_atom(Name) ->
      store_meta_fact(atom_to_list(Name), MetaTable, LProtoType)
;
store_meta_fact(Name, MetaTable, LProtoType)->
%     my  $POST = qq[<?xml version="1.0" encoding="UTF-8" standalone="yes" ?><CellSet><Row key="$key">$cell_set</Row></CellSet>];
%     my $str = qq[curl -X POST -H "Content-Type: text/xml" --data '$POST'  $BASE/$key_url/params];
%   my $key  = md5_hex( join(",",@arr) );
% %       POST /<table>/schema
% http://avias-db-2.ceb.loc:60050/table/key_name/family

	    MakeCellSet = make_cell_normal("description", LProtoType ),
	    BKey= base64:encode(Name),
	    Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
	    {Hbase_Res, Host } = get_rand_host(), 
            case catch  httpc:request( post, { Hbase_Res++MetaTable++"/"++Name++"/description",
 				[ 
				  {"Content-Length", integer_to_list( erlang:byte_size(Body) ) },
				  {"Content-Type","text/xml"},
 				  {"Host", Host}
				],
                                   "application/x-www-form-urlencoded",
                                   Body }, [ {connect_timeout,?DEFAULT_TIMEOUT },
					      {timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}, { headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p ",[?LINE,Res]),
                            true
	    end.


make_cell_normal(Family, ProtoType)->
		
		lists:foldl(fun( { Name, Value }, In)->
				Val =  base64:encode( Value ),
				Cell = base64:encode( Family ++":"++ Name ),
				CellText = <<"<Cell column=\"",Cell/binary,"\">",Val/binary,"</Cell>" >>,
				NewIn = <<In/binary, CellText/binary>>,
				NewIn
			     end, <<>> ,ProtoType)
.

prototype_to_list(E)->
    lists:map(fun(Elem)-> inner_to_list(Elem)  end, E)

.

generate_key(ProtoType) when is_tuple(ProtoType)->
      generate_key(tuple_to_list(ProtoType))
;
generate_key(ProtoType)->
   String =  string:join(common:list_to_unicode(ProtoType), ","),
   hexstring( crypto:md5( unicode:characters_to_binary( String )  ) ).

   

inner_to_list(E) when is_list(E)->
  E;
inner_to_list(E) when is_atom(E)->
  atom_to_list(E);
  
inner_to_list(E) when is_integer(E)->
  integer_to_list(E);

inner_to_list(E) when is_float(E)->
  float_to_list(E);
inner_to_list(E) ->
  E.  

inner_to_atom(E) when is_atom(E)->
    E
;
inner_to_atom(E) when is_binary(E)->
    list_to_atom( binary_to_list(E)   )
;
inner_to_atom(E) when is_list(E)->
    list_to_atom(E)
;
inner_to_atom(E) when is_integer(E)->
   list_to_atom( integer_to_list(E) )
; 
inner_to_atom(E) when is_float(E)->
   list_to_atom( float_to_list(E) )
.

    
get_list_facts()->
      
	{Hbase_Res, Host } = get_rand_host(),
	case catch  httpc:request( get, { Hbase_Res,[ {"Accept","application/json"}, {"Host", Host}]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT } ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
% 			    	[ {<<"table">>,
% 				      [ 
%       				 [ {<<"name">>,<<"pay">>} ],
% 				         [ {<<"name">>,<<"table">>} ]
% 				      ]
%       			 }
% 				]
				[{ Tale, ListTables }] = jsx:decode(  list_to_binary( Text1 )   ),
				?DEBUG("~p got data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, ListTables ] ),
				lists:map(fun([Elem])-> Elem end, ListTables );
% 				{"table":[{"name":"pay"},{"name":"table"}]}
		     Res ->
			    ?DEBUG("~p got ~p try reach tables again ~n",[ {?MODULE,?LINE}, Res ] ),
                           
                           get_list_facts()
                           
	end


.


get_val_simple(E, Key)->
	  
	  case  lists:keysearch(Key, 1, E) of
	      	  {value, {_, Value} } -> binary_to_list(Value);
		  Undef-> 
		      ?WAIT("~p mistake with ~p", [E,Key]), <<"">>
	  end
.
get_val(E, Key)->
	  {value, {_, Value} } = lists:keysearch(Key, 1, E),
	  base64:decode(Value)
.

readlines(FileName) ->
    {ok, Device} = file:open(FileName, [read]),
    try get_all_lines(Device)
      after file:close(Device)
    end.

get_all_lines(Device) ->
    case io:get_line(Device, "") of
        eof  -> [];
        Line -> Line ++ get_all_lines(Device)
    end.

process_data([], _ProtoType)->
    [];
process_data(Text1, ProtoType)->
	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {ProtoType,Text1} ] ),
	Pro = jsx:decode(  list_to_binary( Text1  )  ),
	?DEBUG("~p got  ~n ~p ~n~n~n",[{?MODULE,?LINE}, Pro ] ),
	[ { Row, Keys } ] = Pro,

	
	MainResult = dict:new(),%%%storing there all results
	Result =
	lists:map(fun(Elem)->
% 		    io:format(" read ~p ", [ Elem ] ), 
		    
		    [  KeyRow, {_, RowCells }  ] = Elem,	    
		     lists:map( fun(E)-> 
				    Col = get_val(E, <<"column">>),
				    Val = get_val(E, <<"$">>),
				    ?DEBUG("get key ~p ~n",[ {Col,Val} ]),
				    {Col,Val}
				end, RowCells)
				
		  end, Keys),
	Columns = lists:seq(1, length(ProtoType) ),
	?DEBUG("~p process data after reduce ~p ~n",[{?MODULE,?LINE}, Result ] ),
	
	Got = lists:map(fun(Elem)->		   
		%%TODO REPLACE IT!!!!!!!!!!!
		      list_to_tuple( lists:map( fun(E)-> 
		      
					    get_val_simple(Elem, list_to_binary(?FAMILY++":"++ integer_to_list(E) ) )    
					    
				end, Columns) )
				
		  end, Result),
% %  	io:format("got result ~p ",[Got]),
	Got
.



hexstring(<<X:128/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~32.16.0b", [X]));
hexstring(<<X:160/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~40.16.0b", [X]));
hexstring(<<X:256/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~64.16.0b", [X]));
hexstring(<<X:512/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~128.16.0b", [X])).

    
    
delete_scanner(Scanner)->
    ?LOG("~p delete scanner ~p ~n",[ {?MODULE,?LINE}, Scanner ] ),
    Host = get_host(Scanner),
    delete_host(Scanner),
    case catch  httpc:request( delete, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT } ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    ?LOG("~p delete normal scanner ~p ~n",[ {?MODULE,?LINE}, Scanner ] ),
			    exit(normal);
			   
                    Res ->
			    ?LOG("~p delete scanner not good ~p try again ~n",[ {?MODULE,?LINE}, Res ] ),
			     exit(normal) %%TODO add count of    
    end
.
%TODO when Pat is atom ???

create_hbase_family_filter(Name)->
    jsx:encode( [   
		    { op, <<"EQUAL">>}, 
		    { type,<<"FamilyFilter">> },
		    { comparator,[ 
			  { value,  base64:encode(Name) }, 
			  { type, <<"BinaryComparator">> }
			]
		    }
		])
.


create_hbase_json_filter(
    {  Pat, Name  } ) 
	when is_float(Pat)->

      create_hbase_json_filter({ list_to_binary (float_to_list( Pat ) ),  Name})
;
create_hbase_json_filter( {Pat, Name} ) when is_integer(Pat)->

      create_hbase_json_filter({ list_to_binary (integer_to_list( Pat ) ), Name})
;
create_hbase_json_filter({Pat, Name}) when is_list(Pat)->

      create_hbase_json_filter({ list_to_binary ( Pat ),  Name})
;
create_hbase_json_filter({Pat, Name})->

	      jsx:encode( [   { latestVersion, true }, 
		    { ifMissing, false},
		    { qualifier,  base64:encode( Name ) },%?FAMILY++":"++
		    { family,  base64:encode(?FAMILY) },
		    { op, <<"EQUAL">>}, 
		    { type,<<"SingleColumnValueFilter">> },
		    { comparator,[ 
			  { value,  base64:encode(Pat) }, 
			  { type, <<"BinaryComparator">> }
			]
		    }
		  ])
.

cut_family(Family, Val ) when is_list(Family)->
    FamB = list_to_binary(Family ++ [$:]),
    binary:part(Val, {byte_size(FamB), byte_size(Val) - byte_size(FamB) })
  
.




