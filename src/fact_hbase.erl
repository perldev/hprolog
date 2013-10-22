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
weight_prolog_sort(Prefix)->
        Weights = common:get_logical_name(Prefix, ?META_WEIGHTS),
        Links = common:get_logical_name(Prefix, ?META_LINKS),
        FirstKey = ets:first(Links),
        case catch  process_ets_key(FirstKey, Links, Weights, []) of
            {'EXIT', _Reason}->
                nothing;
            NewList ->
                ets:delete_all_objects(Links),
                lists:foreach(fun(L)-> ets:insert(Links,L ) end, NewList)
        end        
.


process_ets_key('$end_of_table', _Links, _Weights, Acum)->
    Acum;
process_ets_key(Key, Links, Weights, Acum)->
    List = ets:lookup(Links, Key),
    List1 = lists:sort( fun(A, B)->
                                {_FactName, NameFact, _RuleNameA   } = A,
                                {_FactName1, NameFact1, _RuleNameB   } = B,
                                [ WeightsB ] = ets:lookup(Weights, NameFact),
                                [ WeightsA ] = ets:lookup(Weights, NameFact1),
                                ?DEBUG("~p compare weights ~p ~n",[{?MODULE,?LINE}, {WeightsB, WeightsA }]),
                                erlang:element(3,WeightsB) > erlang:element(3,WeightsA)                  
                        end, List),
    NewKey = ets:next(Links, Key),
    process_ets_key(NewKey, Links, Weights, [List1|Acum] )
.
% {Name, common:inner_to_int(Count),
%                                common:inner_to_int(Weight),  
%                                common:inner_to_int(Popularity) } = A,



create_new_namespace(Prefix)->
    case  check_exist_table(Prefix ++ ?META_FACTS) of
      false ->  create_new_meta_table(Prefix ++ ?META_FACTS),
		create_new_fact_table(Prefix ++ ?RULES_TABLE),
		true;
      _ ->
	  false
    end
.

%TODO add checking of result for this operation( loading code from hbase)
%%wheather code was  loaded successfully
load_rules2ets(Prefix)->
      
      Scanner  = generate_scanner(1024,<<>>),
      Family = create_hbase_family_filter("description"),

      MetaInfo = create_hbase_family_filter(?STAT_FAMILY),

      FamilyLinks = create_hbase_family_filter(?LINKS_FAMILY),
      CacheLinks = create_hbase_family_filter(?CACHE_FAMILY),
      ScannerCache  = generate_scanner(1024,  CacheLinks),
      ScannerMeta  = generate_scanner(?META_INFO_BATCH,  Family),      
      ScannerAiMeta  = generate_scanner(1024,  FamilyLinks),
      
      ScannerMetaInfo = generate_scanner(?META_INFO_BATCH,  MetaInfo),      
      ScannerUrlMetaInfo  = get_scanner(common:get_logical_name(Prefix, ?META_FACTS), ScannerMetaInfo),
      ScannerUrlCacheMeta  = get_scanner(common:get_logical_name(Prefix, ?META_FACTS), ScannerCache),
      ScannerUrlAiMeta  = get_scanner(common:get_logical_name(Prefix, ?META_FACTS), ScannerAiMeta),
      ScannerUrlMeta  = get_scanner(common:get_logical_name(Prefix, ?META_FACTS), ScannerMeta),
      ScannerUrl  = get_scanner(common:get_logical_name(Prefix, ?RULES_TABLE), Scanner),
      ?DEBUG("~p get scanner url ~p ~n",[{?MODULE,?LINE}, ScannerUrl]),

      
      get_and_load_meta_weights(ScannerUrlMetaInfo, common:get_logical_name(Prefix,?META_WEIGHTS) ),
      get_cache_meta_info(ScannerUrlCacheMeta, common:get_logical_name(Prefix, ?HBASE_INDEX) ), %%cach is inner struct       
      get_link_meta_info(ScannerUrlAiMeta, common:get_logical_name(Prefix, ?META_LINKS)  ),
      get_meta_facts(ScannerUrlMeta, common:get_logical_name(Prefix, ?META) ),
      get_and_load_rules(ScannerUrl, common:get_logical_name(Prefix,?RULES) ),
      weight_prolog_sort(Prefix),
      start_thrift_pool(?USE_THRIFT),      
      true
     
.


start_thrift_pool(0)->
    false;
start_thrift_pool(1)->
    case lists:member(thrift_connection_pool,  global:registered_names() ) of
          false -> thrift_connection_pool:start_link(?DEFAULT_COUNT_THRIFT);
          true  -> do_nothing
    end.



delete_all_rules(Prefix)->
       Scanner  = generate_scanner(1024,<<>>),
       RealTable = common:get_logical_name(Prefix, ?RULES_TABLE),
       ScannerUrl  = get_scanner(RealTable, Scanner),
       Names = get_list_of_rules(ScannerUrl, RealTable, [] ),
       ?DEBUG("~p delete rules ~p from ~p~n",[{?MODULE,?LINE}, Names, Prefix]),
       lists:foreach( fun(E)->  
                         del_key(binary_to_list(E), RealTable)
                       end, Names)
.
get_list_of_rules([], Table, In )->
    ?LOG("~p empty scanner ~p ~n",[{?MODULE,?LINE},Table]), 
    In
    
;
get_list_of_rules(Scanner, Table, In)->
         ?DEBUG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner } ] ),
        Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
                                    [ {connect_timeout,?DEFAULT_TIMEOUT },
                                      {timeout, ?DEFAULT_TIMEOUT }],
                                    [ {sync, true},{ body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                            NewList = get_rules_name( Text1, Table ),
                            get_list_of_rules(Scanner, Table, In ++ NewList );%% TODO another solution
                           
                    { ok, { {_NewVersion, 204, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                           In;
                    Res ->
                          ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
                          unlink ( spawn(?MODULE, delete_scanner,[Scanner]) ),
                          In   
        end
.


get_rules_name(<<"">>, _Table ) ->
      []
;   
get_rules_name([], _Table ) ->
      []
;    
get_rules_name(Code, Table ) when is_list(Code)->
      get_rules_name(  list_to_binary(Code), Table )
;    
get_rules_name(  Code,  Table )->
    Json = jsx:decode( Code ),
    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Json ] ),
    [{_Row, Rules }] = Json,
    lists:map(fun get_name/1 ,Rules )
.

get_name(Elem)->
      [{_Key, RuleName},{_Cell, _Columns }] = Elem,
      RealName  = base64:decode(RuleName),
      ?DEBUG("~p find rule  ~p ~n",[ {?MODULE,?LINE}, RealName ] ),
      RealName
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
				     {timeout, ?DEFAULT_TIMEOUT }
				     
				     ],
				    [ {sync, true},{ body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_cache_link( Text1, Table ),
			    get_cache_meta_info(Scanner, Table);%% TODO another solution
                    { ok, { {_NewVersion, 204, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                           [];
                    Res ->
			    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
			     unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            throw( {'EXIT', {hbase_exception, Res} } )      
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
			RowName = common:inner_to_atom( base64:decode(RowName64) ),
			lists:foreach( fun(E)-> 
					  CacheInfo  =  get_val(E, <<"column">>) ,
					  Col = common:inner_to_atom( cut_family( ?CACHE_FAMILY, CacheInfo )  ),
					  Val = unicode:characters_to_list( get_val(E, <<"$">> ) ) ,
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
				     {timeout, ?DEFAULT_TIMEOUT },
				     { body_format, binary }
				     ],
				    [ {sync, true} ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_meta_link( Text1, Table ),
			    get_link_meta_info(Scanner,  Table);%% TODO another solution
                     { ok, { {_NewVersion, 204, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                            [];
                    Res ->
			    ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
			     unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            throw({'EXIT', {hbase_exception, Res} })        
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
			RowName = common:inner_to_atom( base64:decode(RowName64) ),
			lists:foreach( fun(E)-> 
					  Cache  =  get_val(E, <<"column">>) ,
					  Col = common:inner_to_atom( cut_family( ?LINKS_FAMILY, Cache )  ),
					  Val = common:inner_to_atom( get_val(E, <<"$">>) ),
  					  ?DEBUG("~p insert link info ~p ~n",[ {?MODULE,?LINE}, {Col,Val} ] ), 
					  ets:insert(Table, { RowName, Col, Val  })
					end, RowCells) 
					
		      end, Links )
.

process_meta_weights(<<"">>, _Table )->
     []
; 
process_meta_weights([], _Table )->
     []
; 
process_meta_weights(Meta, Table ) when is_list(Meta)->
      process_meta_weights(  list_to_binary(Meta), Table )
;   
process_meta_weights(Meta, Table )->
      Json = jsx:decode( Meta ),
      ?DEBUG("~p got meta ~p ~n",[ {?MODULE,?LINE}, Json ] ),
      [{_Row, Facts }] = Json,
      lists:foldl(fun process_hbase_meta_weight_fact/2, Table, Facts )
.
process_hbase_meta_weight_fact(Fact, Table)->
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
      Popularity = get_column(Columns, <<"stat:facts_reqs">>),
      Weight = get_column(Columns, <<"stat:facts_w">>),
      Count = get_column(Columns, <<"stat:facts_count">>),

      Func = get_column(Columns,<<"description:hash_function">>),
      ?DEBUG("~p got meta info of fact ~p ~n",[{?MODULE,?LINE}, {Name, Count, Weight, Popularity } ]),
      ets:insert(Table, {Name, common:inner_to_int(Count),
                               common:inner_to_int(Weight),  
                               common:inner_to_int(Popularity) }),
      Table
.



get_and_load_meta_weights([], Table)->
    ?LOG("~p empty scanner~n",[{?MODULE,?LINE}])
;
get_and_load_meta_weights(Scanner, Table)->
         ?DEBUG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner } ] ),
%        httpc:set_options( [ {verbose, debug} ]),
        Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
                                    [ {connect_timeout,?DEFAULT_TIMEOUT },
                                      {timeout, ?DEFAULT_TIMEOUT }
                                      ],
                                    [ {sync, true},{ body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                            process_meta_weights( Text1, Table ),
                            get_and_load_meta_weights(Scanner, Table);%% TODO another solution
                           
                    { ok, { {_NewVersion, 204, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                           [];
                    Res ->
                            ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
                             unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            throw({'EXIT', {hbase_exception, Res} })            
        end
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
				      {timeout, ?DEFAULT_TIMEOUT }
				      ],
				    [ {sync, true},{ body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_meta( Text1, Table ),
			    get_meta_facts(Scanner, Table);%% TODO another solution
			   
                    { ok, { {_NewVersion, 204, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                           [];
                    Res ->
                            ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
                             unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            throw({'EXIT', {hbase_exception, Res} })           
	end
.

get_and_load_rules([], Table )->
    ?LOG("~p empty scanner ~p ~n",[{?MODULE,?LINE},Table]), []
    
;
get_and_load_rules(Scanner, Table)->
	 ?DEBUG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner } ] ),
	Host = get_host(Scanner),
        case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host}]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT }],
				    [ {sync, true},{ body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    process_code( Text1, Table ),
			    get_and_load_rules(Scanner, Table);%% TODO another solution
			   
                    { ok, { {_NewVersion, 204, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                           [];
                    Res ->
                            ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Res ] ),  
                             unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                            throw({'EXIT', {hbase_exception, Res} })         
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

process_meta(<<"">>, _Table )->
     []
; 
process_meta([], _Table )->
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
			       unicode:characters_to_list( base64:decode( Value64 ) );
			_-> I
		    end
		end,"", Columns)
.

process_code(<<"">>, _Table ) ->
      []
;   
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

      NewBinary = binary:replace(Code,[<<"=..">>], <<"#%=#">>, [ global ] ),
      CodeList = binary:split(NewBinary, [<<".">>],[global]),

      
      lists:foldl(fun compile_patterns/2,  Table,  CodeList)
.

compile_patterns(<<>>,  Table)->
    Table

;
compile_patterns( OnePattern,  Table )->
      NewBinary = binary:replace(OnePattern,[<<"#%=#">> ], <<"=..">>, [ global ] ),
      HackNormalPattern =  <<NewBinary/binary, " . ">>,
      ?DEBUG("~p begin process one pattern   ~p ~n",[ {?MODULE,?LINE}, HackNormalPattern ] ),
     {ok, Terms , L1} = erlog_scan:string( unicode:characters_to_list(HackNormalPattern) ),
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

start_fact_process( Aim, TreeEts, ParentPid)->
      spawn( ?MODULE, fact_start_link, [ Aim, TreeEts, ParentPid] )
.
%%TODO add rest call to find all facts and rules 
fact_start_link( Aim,  TreeEts, ParentPid )->
      monitor(process, ParentPid),
      ?DEBUG("~p begin find in facts ~p ~n",[{?MODULE,?LINE},  Aim ]),
      Name = element(1,Aim),%%from the syntax tree get name of fact
      ?WAIT("~p regis wait in fact ~p ~n",[{?MODULE,?LINE},{ Name, Aim } ]),
      NameSpace = common:get_logical_name( TreeEts),
      converter_monitor:stat('search', Name, NameSpace , Aim, true ),
      case check_exist_facts( Name, TreeEts) of %%and try to find in hbase
                    true-> 
                        fact_start_link_hbase(Aim,  TreeEts, ParentPid);
                    false -> 
			 ?DEBUG("~p delete on start ~p ~n",[{?MODULE,?LINE},  Name  ]),
			 ParentPid ! non_exist_exception,
			 exit(normal)
      end
     
.

fact_start_link_hbase( Aim,  TreeEts, ParentPid )->

      %TODO remove this converts
      Name = element(1,Aim), %%from the syntax tree get name of fact
      Search =  common:my_delete_element(1, Aim),%%get prototype
      ProtoType = tuple_to_list(Search ),
      CountParams = length(ProtoType),
      ?DEBUG("~p generate scanner ~p ~n",[{?MODULE,?LINE}, {Name,ProtoType } ]),
      process_flag(trap_exit, true),%%for deleting scanners
      NameTable =  common:get_logical_name(TreeEts, Name ),  
      case check_params_facts(Name, TreeEts) of
	  {CountParams, HashFunction} ->
		 case  check_index(ProtoType, Name, TreeEts) of
		       []->
			      start_process_loop_hbase(Name, ProtoType, TreeEts);

		       {Name,  PartKey }->
    			      ?DEBUG("~p find whole_key ~p ~n",[{?MODULE,?LINE}, { Name, PartKey } ]),
			       process_indexed_hbase(atom_to_list(NameTable),
                                                    ProtoType,
                                                    [PartKey],
                                                    TreeEts);
		       {IndexTable , PartKey } ->
			      ?DEBUG("~p got index ~p ~n",[{?MODULE,?LINE}, { {IndexTable, Name} , PartKey } ]),
		              PreRes = get_indexed_records(PartKey, atom_to_list(IndexTable) ),
		              %%TODO process exception timeouts etc
                                
		              
			      process_indexed_hbase(atom_to_list(NameTable), ProtoType,  PreRes,  TreeEts)
		end;
	  Res ->
	      ?DEBUG("~p count  params not matched ~p ~n",[{?MODULE,?LINE}, {Res, CountParams } ]),
	      ParentPid ! arity_exception	
      end
.


get_indexed_records(PartKey, IndexTable) ->

      get_indexed_records( PartKey, IndexTable,?USE_THRIFT )
.


get_indexed_records(PartKey, IndexTable, 1) when is_atom(IndexTable)->
      get_indexed_records( PartKey, atom_to_list(IndexTable), 1 )
;
get_indexed_records(PartKey, IndexTable, 1) ->

     ?DEBUG("~p got index ~n",[{?MODULE,?LINE}  ]),
     fact_hbase_thrift:get_key_custom(IndexTable, ?FAMILY, PartKey, default)    
;
get_indexed_records(PartKey, IndexTable, 0) when is_atom(IndexTable)->
      get_indexed_records( PartKey, atom_to_list(IndexTable) )
;
get_indexed_records(PartKey, IndexTable, 0)->
      {Hbase_Res, Host } = get_rand_host(),
      case catch  httpc:request( get, { Hbase_Res++IndexTable++"/"++PartKey++"/"++?FAMILY,
					  [ {"Accept","application/json"}, {"Host", Host}] 
					},
					  [ {connect_timeout,?DEFAULT_TIMEOUT },
					    {timeout, ?DEFAULT_TIMEOUT }],
					  [ {sync, true},{ body_format, binary } ] ) of
			  { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
				  ?LOG("~p got indexed keys ~p ~n",[ {?MODULE,?LINE}, Text1 ] ),
				  Res = process_key_data(Text1),
				  Res;
		           { ok, { {_NewVersion, 404, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                                 [];
			  Res ->
				  ?LOG("~p got unexpected ~p ~n",[ {?MODULE,?LINE}, Res ] ),
				   {hbase_exception, Res}  %%TODO add count of fail and fail exception may be no
      end.

      
%   [{<<"Row">>,
%                                 [[{<<"key">>,
%                                    <<"MDAwMDAwNTQ1NDgxMmUzZjVlYmRlZjk2YzA1Y2RiZjY=">>},
%                                   {<<"Cell">>,
%                                    [[{<<"column">>,<<"cGFyYW1zOjE=">>},
%                                      {<<"timestamp">>,1364553734408},
%                                      {<<"$">>,
%                                       <<"ZTVjMDc5YTg0MmM5MTg5YzRiZjkyMGFiMzZiZTQ3ZDU=">>}]]} ]]  }] 
process_key_data(<<"">>)->
    []
;
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
			    Val = unicode:characters_to_list( get_val(Elem, <<"$">> ) ) ,
			    Val
		      end, Vals )
.


%%TODO gather mistake of key invalid
process_indexed_hbase(Table, ProtoType, {hbase_exception, not_found}, TreeEts)->

    NameSpace = common:get_logical_name( TreeEts ),
%     converter_monitor:stat('search_index',  Table , NameSpace, ProtoType, false ),
    receive
        {PidReciverResult, get_pack_of_facts}->
            PidReciverResult !  [];
         Some ->     
            ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
    end    
;
process_indexed_hbase(Table, ProtoType, {hbase_exception, Reason}, TreeEts)->

    NameSpace = common:get_logical_name( TreeEts ),
%     converter_monitor:stat('search_index',  Table , NameSpace, ProtoType, false ),
    receive
        {PidReciverResult, get_pack_of_facts}->
            PidReciverResult !  {hbase_exception,Reason};
         Some ->     

            ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
    end    
;

process_indexed_hbase(Table, ProtoType, [], TreeEts)->
    receive
        {PidReciverResult, get_pack_of_facts}->
            PidReciverResult !  [];
         Some -> 
            ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
    end    
;
process_indexed_hbase(Table, ProtoType,  [NewKey| NewPreRes] , TreeEts)->
    	?WAIT("~p regis  wait  hbase   indexed fact  ~n",[{?MODULE,?LINE} ]),
        receive 
	    {PidReciverResult, get_pack_of_facts} ->
		  ?WAIT("~p GOT  wait in hbase indexed ~n",[{?MODULE,?LINE} ]),
                   Row =  hbase_get_key(ProtoType, Table, ?FAMILY, NewKey),
                   ?DEBUG("~p got row  ~p  ~n",[{?MODULE,?LINE}, Row ]),
		    PidReciverResult ! Row,
		   process_indexed_hbase(Table, ProtoType,  NewPreRes, TreeEts);
            {'EXIT', From, Reason} ->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got exit signal  ~p ~n",[{?MODULE,?LINE}, {From, Reason} ]);
            {'DOWN', MonitorRef, Type, Object, Info} ->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got monitor exit signal   ~p ~n",[{?MODULE,?LINE}, {Object, Info} ]);
            Some ->%%may be finish
                  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
	end,
	exit(normal)
.

hbase_get_key(Table,  Key)->
      { Hbase_Res, Host } = get_rand_host(),
       case catch  httpc:request( get, 
				    { Hbase_Res++Table++"/"++Key++"/",
				    [ {"Accept","application/json"}, {"Host", Host }]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT }
				    ],
				    [ {sync, true},{ body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->  
			    Result = process_key_data( Text1),
			    Result ;
			   
                    Res -> 
			    ?DEBUG("~p  got  through key   return ~p ~n  ~p  ~n",
				    [{?MODULE,?LINE},
				    { Table,  Key}, Res ]),
                            {hbase_exception, Res}
	end
.

hbase_get_key(ProtoType, Table, Family, Key)->
    hbase_get_key(ProtoType, Table, Family, Key, ?USE_THRIFT).
%%HACK avoid = [Key]
hbase_get_key(ProtoType, Table, Family, Key, 1)->
    fact_hbase_thrift:get_key(ProtoType, Table, Family, Key)
;    
hbase_get_key(ProtoType, Table, Family, Key, 0)->
      { Hbase_Res, Host } = get_rand_host(),
      Url = lists:flatten(Hbase_Res ++ Table++"/"++Key++"/"++Family),
      ?DEBUG("~p get indexed  key by url ~p",[{?MODULE,?LINE}, Url]),
       case catch  httpc:request( get, 
				    { Url,
				    [ {"Accept","application/json"}, {"Host", Host }]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT }
				       ],
				    [ {sync, true},
				      { body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->  
			    Result = process_data( Text1, ProtoType),
			    Result ;
			   
                    Res -> 
			    ?DEBUG("~p  got  key   return ~p ~n  ~p  ~n",
				    [{?MODULE,?LINE},
				    {ProtoType, Table, Family, Key}, Res ]),
%                             converter_monitor:stat('search_index',  Table , ProtoType, false ),
                            {hbase_exception, Res}        
	end
.

get_list_namespaces()->
   
      TableList = fact_hbase:get_table_list(),
      %HACK
      
      
      MetaTables = lists:filter(fun(E) ->  
                        Name = lists:reverse(E),
                        %%find all meta tables called meta
                        case Name of
                           [$a,$t,$e,$m|_Tail]-> true;
                           _ -> false
                        end
                        
                   end, TableList  ),
                   
      NameSpaces = lists:foldl( fun(E, Acum)->
                                    Name = lists:reverse(E),
                                    [$a,$t,$e,$m|Tail] = Name,
                                    [ lists:reverse(Tail) | Acum]
                                end,[],MetaTables),
      NameSpaces
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


start_process_loop_hbase(Name, ProtoType,  TreeEts)->
    start_process_loop_hbase(Name, ProtoType,  TreeEts, ?USE_THRIFT)
.


start_process_loop_hbase(Name, ProtoType,  TreeEts, 1)->
    ?DEBUG("~p start_process_loop_hbase ~n", [{?MODULE,?LINE}] ),
    fact_hbase_thrift:start_process_loop_hbase(Name, ProtoType,  TreeEts)
;
start_process_loop_hbase(Name, ProtoType,  TreeEts, 0)->
                              HbaseTable =  common:get_logical_name(TreeEts, Name ),  
                              %%TODO throw exception if prototype contain something except constants or variables
                              %%TODO process exception  timeouts etc
                              Scanner = ( catch start_recr( atom_to_list(HbaseTable), ProtoType  ) ),
                              process_loop_hbase( Scanner, ProtoType).


process_loop_hbase( {hbase_exception, Reason}, ProtoType)->
    receive 
            {PidReciverResult ,get_pack_of_facts} ->
                    PidReciverResult ! {hbase_exception, Reason};
             Some ->%%may be finish
                  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
     end,
     exit(normal)
;

process_loop_hbase( {Scanner, finish}, ProtoType)->
    exit(normal)
;
process_loop_hbase( {Scanner, []}, ProtoType)->
    receive 
	    {PidReciverResult ,get_pack_of_facts} ->
		   PidReciverResult ! [],
		   process_loop_hbase( {Scanner, finish}, ProtoType);
	    {'EXIT', From, Reason} ->
     	  	  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
		  ?LOG("~p got exit signal  ~p ~n",[{?MODULE,?LINE}, {From, Reason} ]);
            {'DOWN', MonitorRef, Type, Object, Info}->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got monitor exit signal   ~p ~n",[{?MODULE,?LINE}, {Object, Info} ]);
            Some ->%%may be finish
                  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
                 
    end,
    delete_scanner(Scanner),
    exit(normal)
;
process_loop_hbase( {Scanner, Res}, ProtoType)->
	?WAIT("~p regis  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
        receive 
	    {PidReciverResult ,get_pack_of_facts} ->
	  	    ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
		    ?DEBUG("~p got new request ~p ~n",[{?MODULE,?LINE},  Res ]),
		    PidReciverResult ! Res,
		    NewList = get_data( Scanner, ProtoType),
		    process_loop_hbase( {Scanner, NewList}, ProtoType);
            {'EXIT', From, Reason} ->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got exit signal  ~p ~n",[{?MODULE,?LINE}, {From, Reason} ]);
            {'DOWN', MonitorRef, Type, Object, Info}->
                  ?WAIT("~p GOT  exit in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got monitor exit signal   ~p ~n",[{?MODULE,?LINE}, {Object, Info} ]);
            Some ->%%may be finish
                  ?WAIT("~p GOT  wait in fact hbase ~p ~n",[{?MODULE,?LINE}, ProtoType ]),
                  ?LOG("~p got unexpected ~p ~n",[{?MODULE,?LINE}, Some ])
	end,
	delete_scanner(Scanner),
        exit(normal)
.





get_facts( Pid )-> %%in this will not work method of cutting logic results
        call(Pid, get_pack_of_facts)

.


call(Pid,  Atom )->
    ?DEBUG("~p call to hbase reducer  ~p ~n",[{?MODULE,?LINE},{Pid,Atom} ]),
    Pid !   { self(), Atom } ,
    ?WAIT("~p regis  wait in  inner fact call  ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),
    receive 
         {hbase_exception, Reason}->
              throw({'EXIT',hbase_exception, Reason  } );
	 Result ->
	    ?WAIT("~p GOT  wait in  inner fact call  ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),
	    ?DEBUG("~p  reducer result ~p  ~n",[{?MODULE,?LINE}, Result ]),
	    Result
	after ?FATAL_WAIT_TIME ->
              ?WAIT("~p GOT  TIMEOUT in  inner fact call  ~p ~n",[{?MODULE,?LINE},{Atom, Pid} ]),
	      ?DEBUG("~p  reducer didn't return result   ~n",[{?MODULE,?LINE}]),
	      throw({'EXIT',timeout_hbase, ?FATAL_WAIT_TIME  } )
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
         ?DEBUG("~p scanner filters ~p ~n",[{?MODULE,?LINE}, Filters ]),
	  Main = generate_scanner( ?LIMIT*Size , Filters),
	  ?DEBUG("~p Generate scanner for hbase ~p ~n",[{?MODULE,?LINE},Main ]),
	  Scanner =  get_scanner(Facts, Main),
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

generate_scanner(Limit, << ",",Filters/binary >>)->
    Filter = <<"{  \"type\":\"FilterList\",\"op\":\"MUST_PASS_ALL\",\"filters\":[",
		    Filters/binary, "] }" >>,
    <<"<Scanner batch=\"",Limit/binary, "\" ><filter>", Filter/binary ,"</filter></Scanner>" >>
;
generate_scanner(Limit, <<  Filter/binary >>)->
    <<"<Scanner batch=\"",Limit/binary, "\" ><filter>", Filter/binary ,"</filter></Scanner>" >>
.
%%TODO ADD SUPERVISOUR FOR all scanners


get_scanner(Facts, Scanner)->
	  
	{Hbase_Res, Host } = get_rand_host(),
	  

 	 ?LOG("~p send to ~p ~n",[ {?MODULE,?LINE}, {Scanner, 
				      Hbase_Res++Facts++"/scanner",
				      erlang:byte_size(Scanner) } ] ),

         case catch  httpc:request( 'post', { Hbase_Res++Facts++"/scanner",
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
                            throw({hbase_exception, Res})

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

get_data(Scanner, statistic) ->
    Host = get_host(Scanner),
    case catch  httpc:request( get, { Scanner,[ {"Accept","application/json"}, {"Host", Host }]},
                                    [ {connect_timeout,?DEFAULT_TIMEOUT },
                                      {timeout, ?DEFAULT_TIMEOUT } ],
                                    [ {sync, true},{ body_format, binary } ] ) of
            { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                %%Result = process_data(Text1, statistic);
                Text1;
            Error ->
                ?DEBUG("~p got ~p ~n",[ {?MODULE,?LINE}, Error ] ),
                unlink ( spawn(?MODULE,delete_scanner,[Scanner]) ),
                []
    end;
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
				    [ {sync, true},{ body_format, binary } ] ) of
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
	    SourceFact =  common:inner_to_list(ASourceFact),
	    ForeignFact =  common:inner_to_list(AForeignFact),
	    RuleName =  common:inner_to_list(ARuleName),
	    ?DEBUG("~p assert link ~p~n",[{ ?MODULE,?LINE },
					   { SourceFact,ForeignFact,RuleName } ]),
	    Key = SourceFact,
	    LTableName = common:get_logical_name(TreeEts, ?META_FACTS),
	    MakeCellSet = make_cell_normal(?LINKS_FAMILY, [ { ForeignFact,  RuleName  } ] ),
	    BKey= base64:encode(Key),
	    Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
	    {Hbase_Res, Host } = get_rand_host(),
	    
            case catch  httpc:request( 'post', { Hbase_Res++LTableName++"/"++Key++"/"++?LINKS_FAMILY,
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
                            throw({hbase_exception, Res })
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
          NameSpace = common:get_logical_name(TreeEts),

	  converter_monitor:stat(try_add, Name,  NameSpace , ProtoType, true ),
	  %TODO catch a lot exceptions
	  case check_params_facts( Name, TreeEts ) of
	      {CountParams, _HashFunction } ->
		  Params = prototype_to_list(ProtoType),
		  index_work_add(Name, Params, TreeEts),%%TODO spawn this
		  Res = store_new_fact(RealName, Params ),
                  converter_monitor:stat(add, Name , NameSpace , ProtoType, Res ),
 		  Res;  
	      {CountParams1, _HashFunction } ->
		  ?DEBUG("~p params have not matched with exists~p",[{?MODULE,?LINE}, {CountParams1,CountParams } ]),
		  false
		  ;
	      false -> 
		  ?DEBUG("~p create new table ~p ~n",[{?MODULE,?LINE}, RealName ]),
		  create_new_fact_table(RealName),
		  ?DEBUG("~p store meta info  ~n",[{?MODULE,?LINE} ]),

		  ets:insert( common:get_logical_name(TreeEts, ?META), {Name, CountParams, "md5"} ),
		  store_meta_fact(Name,  common:get_logical_name(TreeEts, ?META_FACTS) , [
					  {"count" ,integer_to_list( CountParams) }, 
					  {"hash_function","md5" },
					  {"facts_count", "1" }%%deprecated
					  ],
					  "description"  ),%TODO NEW HASH function
		  store_meta_fact(Name,  common:get_logical_name(TreeEts, ?META_FACTS) , [
					  {"facts_count", "1" },
					  {"facts_reqs","1"},
					  {"facts_w","1"}
					  ],
					  "stat"  ),
		  Res = store_new_fact(RealName , prototype_to_list(ProtoType) ),
                  converter_monitor:stat(add, Name , NameSpace , ProtoType, Res ),
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
                            throw({hbase_exception, Res })

	    end


.


%TODO  move to thrift
hbase_add_index(IndexTable, NameSpace,  Type, Params)->
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
      converter_monitor:stat(try_add_index, IndexTable, NameSpace, { Type,MD5Key, Key }, true ),
      Res = put_key_body(IndexTable, Key, MakeCellSet),
      converter_monitor:stat(add_index, IndexTable, NameSpace , { Type,MD5Key, Key }, Res ), 
      
      Res
.
%%%TODO IT'S WRONG!!!!
hbase_del_index(IndexTable, NameSpace, Type, Params)->
      Keys =  string:tokens(Type, ","),
      KeyList = lists:map(fun(E)->
		      Index = list_to_integer(E),
		      lists:nth(Index, Params )
		 end,
		 Keys
		 ),
      Key = generate_key( KeyList ),
      ?DEBUG("~p process delete index ~p",[{?MODULE,?LINE}, {Key,KeyList }  ]),
      converter_monitor:stat(try_del_index,  IndexTable , NameSpace, { Type, Key }, true ),
      case catch  del_key(Key, IndexTable ) of
        {hbase_exception, Res }->   
            converter_monitor:stat(del_index,  IndexTable, NameSpace,  { Type, Key }, false ),
            true
        ;    
        Res ->
            converter_monitor:stat(del_index,  IndexTable, NameSpace, { Type, Key }, true ),
            Res
     end
        
.

index_work_add(Name, Params, TreeEts)->
     Key = generate_key( Params ),
     Table = common:get_logical_name( TreeEts, ?HBASE_INDEX),
     NameSpace = common:get_logical_name( TreeEts),

     case  ets:lookup( Table, Name) of
	   [] -> [];
	   List ->
		  lists:foreach(fun({_InKey, IndexTable, KeyName})->
					hbase_add_index(IndexTable, NameSpace,  KeyName, Params)
				 end, List)
    end
.

index_work_del(Name,  Params, TreeEts) when is_tuple(Params)->
   index_work_del(Name,  tuple_to_list(Params), TreeEts) 
;
index_work_del(Name, Params, TreeEts)->
     Key = generate_key( Params ),
     RealName = common:get_logical_name(TreeEts, ?HBASE_INDEX),
     NameSpace = common:get_logical_name(TreeEts),
     case  ets:lookup(RealName, Name) of
	   [] -> [];
	   List ->
		  lists:foreach(fun({_InKey, IndexTable, KeyName})->
				      hbase_del_index(IndexTable,NameSpace,  KeyName, Params  )
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
       V2 = unicode:characters_to_binary( lists:flatten(PrologCode)++"." ),
      	?DEBUG("~p new rule table is  ~p ~n",[ {?MODULE,?LINE}, TableName ] ),

       case check_exist_rule( TableName, Name ) of
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
      ?DEBUG("~p del link ~p",[{?MODULE,?LINE}, {Fact1, TableName, ?LINKS_FAMILY, Fact2}]),
      del_key(Fact1, TableName, ?LINKS_FAMILY, Fact2 )
.

del_fact(B, TreeEts)->
  del_fact(B, TreeEts, ?SIMPLE_HBASE_ASSERT)
.
del_fact(B, TreeEts,  1)->
      ?DEBUG("~p begin process of delete ~p",[{?MODULE,?LINE}, B]),
      Name = erlang:element(1,B),

      RealName = common:get_logical_name(TreeEts, Name),
      NameSpace  = common:get_logical_name(TreeEts),
      
      ProtoType = common:my_delete_element(1,B),
      ?DEBUG("~p delete stat ~p",[{?MODULE,?LINE}, RealName ]),

      converter_monitor:stat(try_del, Name,  NameSpace , ProtoType, true ),
      ?DEBUG("~p delete index ~p",[{?MODULE,?LINE}, RealName ]),

       index_work_del(Name, ProtoType, TreeEts),
       ?DEBUG("~p delete key ~p",[{?MODULE,?LINE}, RealName ]),

        case catch  del_key( generate_key(ProtoType), RealName) of
        {hbase_exception, Res }->   
            converter_monitor:stat(del, Name,  NameSpace , ProtoType, false ),
            throw({hbase_exception, Res })
        ;    
        Res ->
            converter_monitor:stat(del, Name,  NameSpace , ProtoType, true ),
            Res
        end     
;
del_fact(_,_,_)->
    true.


delete_all_fact(Name, TreeEts )->
    delete_all_fact(Name, TreeEts,?SIMPLE_HBASE_ASSERT )
.

delete_all_fact(Name, TreeEts, 0 )->
    true;
delete_all_fact(Name, TreeEts, 1 )->
    NameSpace = common:get_logical_name(TreeEts),

    RealName = common:get_logical_name(TreeEts, Name),
    MetaTable = common:get_logical_name(TreeEts, ?META_FACTS),
    converter_monitor:stat(try_abolish,  Name ,NameSpace, {}, true ),
    TableNameIndex = common:get_logical_name(TreeEts, ?HBASE_INDEX),
    ListIndex = ets:lookup(TableNameIndex, Name),
    TableListIndex = lists:map(  fun({_Name, TableIndex, _})->
                                    TableIndex
                            end, ListIndex),
    Table2Delete =  [RealName| TableListIndex],
    case catch lists:foreach( fun delete_table/1, Table2Delete) of
         ok ->
                converter_monitor:stat(abolish,  Name ,NameSpace, {}, true ),
                del_key(Name, MetaTable );
         Exception ->
                del_key(Name, MetaTable ),
                converter_monitor:stat(abolish,  Name ,NameSpace, {}, false ),
                throw(Exception)
         
    end    

    
.    

   
    

del_rule(Rule, TreeEts)->
  del_rule(Rule, TreeEts, ?SIMPLE_HBASE_ASSERT)    
.
del_rule(Rule, TreeEts, 1)->
  TableName = common:get_logical_name(TreeEts, ?RULES_TABLE ),
  del_key(Rule, TableName);
del_rule(_,_,_)->
  true.
  
delete_table(TableName) when is_atom(TableName)->
    delete_table(atom_to_list(TableName)) 
;
delete_table(TableName)->
        {Hbase_Res, _Host } = get_rand_host(),
        case catch  httpc:request( delete, { Hbase_Res ++ TableName ++ "/schema"  ,
                                                []}, [{connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }],
                                [ {sync, true}, {headers_as_is, true } ] ) of
                         { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
                                ?DEBUG("~p process delete data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, TableName ] ),
                                true;
                        Res ->
                            ?WAIT("~p got from hbase ~p for table ~p  ",[?LINE,Res,  TableName]),
                             throw({hbase_exception, { {delete, TableName}, Res} })

        end

.

del_key(Key, TableName, Family, Col )->
    del_key(Key, TableName, Family, Col, ?USE_THRIFT).
    
del_key(Key, TableName, Family, Col , 1 )->
      fact_hbase_thrift:del_key(Key, TableName, Family, Col)
;
del_key(Key, TableName, Family, Col, 0 )->
      
	{Hbase_Res, _Host } = get_rand_host(),
	
	case catch  httpc:request( delete, { Hbase_Res ++ TableName ++ "/" ++ Key ++ "/" ++ Family ++ ":" ++ Col ,
						[]}, [{connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }],
                                [ {sync, true}, {headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process delete data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Key ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p for key ~p  ",[?LINE,Res, {Key, TableName}]),
                             throw({hbase_exception, Res })

	    end
.


del_key(Key, TableName ) when is_atom(TableName) ->

  del_key(Key, atom_to_list(TableName) )
;
del_key(Key, TableName ) when is_atom(Key) ->

  del_key(atom_to_list(Key), TableName )
;
del_key(Key, TableName)->
    del_key(Key, TableName, ?USE_THRIFT).
    
del_key(Key, TableName , 1 )->
     fact_hbase_thrift:del_key(TableName, Key)
;
del_key(Key, TableName , 0 )->
	   {Hbase_Res, _Host } = get_rand_host(),
            case catch  httpc:request( delete, { Hbase_Res++TableName++"/"++Key, [] },
				  [ {connect_timeout,?DEFAULT_TIMEOUT }, {timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}  ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process delete data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Key ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p for key ~p  ",[?LINE, Res , {Key, TableName}]),
                             throw({hbase_exception, Res })

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
                             throw({hbase_exception, Res })

	    end
.


get_table_list()->
	  {Hbase_Res, Host } = get_rand_host(),
	  case catch   httpc:request( 'get', { Hbase_Res,
				    [  {"Host", Host}]},
				    [{connect_timeout,?DEFAULT_TIMEOUT },
						{timeout, ?DEFAULT_TIMEOUT }  ],
				    [ {sync, true} ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
				 string:tokens(Text1, "\n");
                        Res ->
                            ?DEBUG("~p got from hbase ~p ",[?LINE,Res]),
                            throw({hbase_exception, Res })

	    end

.


check_exist_table(TableName)->
	    List =   get_table_list(),
	    lists:member(TableName, List ).

check_exist_rule(TableName, Name) when is_atom(Name)->
 LName = atom_to_list(Name),
 check_exist_rule(TableName, LName)
;
check_exist_rule(TableName, Name) when is_integer(Name)->
 LName = integer_to_list(Name),
 check_exist_rule(TableName, LName)
;
check_exist_rule(TableName, LName)->
   
    {Hbase_Res, Host } = get_rand_host(),
    Url =  Hbase_Res++TableName++"/"++LName++"/"++?FAMILY++":"++?CODE_COLUMN,
    ?DEBUG("~p check url ~p~n",[{?MODULE,?LINE},Url ]),
    case catch  httpc:request( 'get', { Url,
				    [ {"Accept","application/json"}, {"Host", Host}]},
				    [ {connect_timeout, ?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT }],
				    [ {sync, true}, { body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
% 				    "{\"Row\":[{\"key\":\"ZmFjdA==\",\"Cell\":[{\"column\":\"cGFyYW1zOmNvZGU=\",\"timestamp\":1361291589351,\"$\":
% 				    \"ZmFjdChYKSA6LSBmYWN0MShYKSAsIGZhY3QyKFgxKSAsIGZhY3QoWDEpLg==\"}]}]}"
				Json = jsx:decode(Text1),
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
                      { ok, { { _NewVersion, 201, _NewReasonPhrase}, _NewHeaders, Text1 } } -> 
                               ?DEBUG("~p create new  table  ~p ~n~n~n",[{?MODULE,?LINE}, {TableName} ] ),
                                true;
                      Res ->
                            ?WAIT("~p got from hbase ~p ",[?LINE,Res]),
                             throw({hbase_exception, Res })

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
                      { ok, { { _NewVersion, 201, _NewReasonPhrase}, _NewHeaders, Text1 } } -> 
                               ?DEBUG("~p create new  table  ~p ~n~n~n",[{?MODULE,?LINE}, {TableName} ] ),
                                true;
                      Res ->
                            ?WAIT("~p got from hbase ~p ",[?LINE,Res]),
                             throw({'EXIT',hbase, Res } )
		  end
.

store_new_fact(Name, LProtoType) when is_atom(Name)->

    store_new_fact(atom_to_list( Name ), LProtoType)
;
store_new_fact(Name, LProtoType) ->
    store_new_fact(Name , LProtoType, ?USE_THRIFT).
    

store_new_fact(LTableName, LProtoType, 1)->
    fact_hbase_thrift:stor_new_fact(LTableName, LProtoType)

;
store_new_fact(LTableName, LProtoType, 0)->
%     my  $POST = qq[<?xml version="1.0" encoding="UTF-8" standalone="yes" ?><CellSet><Row key="$key">$cell_set</Row></CellSet>];
%     my $str = qq[curl -X POST -H "Content-Type: text/xml" --data '$POST'  $BASE/$key_url/params];
%   my $key  = md5_hex( join(",",@arr) );
% %       POST /<table>/schema
% http://avias-db-2.ceb.loc:60050/table/key_name/family
            ?DEBUG("~p add fact to hbase ~p",[{?MODULE,?LINE}, {LTableName, LProtoType}]),
	    Key = generate_key( LProtoType ),
	    MakeCellSet = make_cell_set( LProtoType ),
	    BKey= base64:encode(Key),
	    Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
	    {Hbase_Res, Host } = get_rand_host(),
            ?DEBUG("~p new fact body with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Hbase_Res,LTableName, Key, ?FAMILY } ] ),
            case catch  httpc:request( post, { Hbase_Res++LTableName++"/"++Key++"/"++?FAMILY,
 				[ {"Content-Length", integer_to_list( erlang:byte_size(Body) )},
				  {"Content-Type","text/xml"},
 				  {"Host", Host}
				],
                                   "application/x-www-form-urlencoded",Body },
                                   [ {connect_timeout,?DEFAULT_TIMEOUT },{timeout, ?DEFAULT_TIMEOUT }  ],
                                [ {sync, true}, {headers_as_is, true } ] ) of
			 { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, Text1 ] ),
				true;
                        Res ->
                            ?WAIT("~p got from hbase ~p ~n",[?LINE,{Res,LTableName, LProtoType }]),
                             throw({hbase_exception, Res })

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


store_meta_fact(Name, MetaTable, LProtoType, Type) when is_atom(Name) ->
      store_meta_fact(atom_to_list(Name), MetaTable, LProtoType, Type)
;
store_meta_fact(Name, MetaTable, LProtoType, Type)->
%     my  $POST = qq[<?xml version="1.0" encoding="UTF-8" standalone="yes" ?><CellSet><Row key="$key">$cell_set</Row></CellSet>];
%     my $str = qq[curl -X POST -H "Content-Type: text/xml" --data '$POST'  $BASE/$key_url/params];
%   my $key  = md5_hex( join(",",@arr) );
% %       POST /<table>/schema
% http://avias-db-2.ceb.loc:60050/table/key_name/family

	    MakeCellSet = make_cell_normal(Type, LProtoType ),
	    BKey= base64:encode(Name),
	    Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
	    {Hbase_Res, Host } = get_rand_host(), 
            case catch  httpc:request( post, { Hbase_Res++MetaTable++"/"++Name++"/"++Type,
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
                             throw({hbase_exception, Res })
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
    lists:map(fun(Elem)-> common:inner_to_list(Elem)  end, E)

.

generate_key(ProtoType) when is_tuple(ProtoType)->
      generate_key(tuple_to_list(ProtoType))
;
generate_key(ProtoType)->
   String =  string:join(common:list_to_unicode(ProtoType), ","),
   hexstring( crypto:md5( unicode:characters_to_binary( String )  ) ).

   



    
get_list_facts()->
      
	{Hbase_Res, Host } = get_rand_host(),
	case catch  httpc:request( get, { Hbase_Res,[ {"Accept","application/json"}, {"Host", Host}]},
				    [ {connect_timeout,?DEFAULT_TIMEOUT },
				      {timeout, ?DEFAULT_TIMEOUT }],
				    [ {sync, true},
                                      { body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->
			    	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {Text1} ] ),
% 			    	[ {<<"table">>,
% 				      [ 
%       				 [ {<<"name">>,<<"pay">>} ],
% 				         [ {<<"name">>,<<"table">>} ]
% 				      ]
%       			 }
% 				]
				[{ Tale, ListTables }] = jsx:decode(   Text1  ),
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
	      	  {value, {_, Value} } -> unicode:characters_to_list(Value);
		  Undef-> 
		      ?WAIT("~p mistake with ~p", [E,Key]), <<"">>
	  end
.
get_val(E, Key)->
	  {value, {_, Value} } = lists:keysearch(Key, 1, E),
	  base64:decode(Value)
.


process_data(<<"">>, _ProtoType)->
    [];
process_data([], _ProtoType)->
    [];
process_data(Text, ProtoType) when is_list(Text)->
    process_data(list_to_binary(Text), ProtoType)
;
process_data(Text1, ProtoType)->
	?DEBUG("~p process data with ~n ~p ~n~n~n",[{?MODULE,?LINE}, {ProtoType,Text1} ] ),
	Pro = jsx:decode(   Text1   ), 
	?DEBUG("~p got  ~n ~p ~n~n~n",[{?MODULE,?LINE}, Pro ] ),
	[ { Row, Keys } ] = Pro,

	
	MainResult = dict:new(),%%%storing there all results
	Result =
	lists:map(fun(Elem)->	    
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
	?DEBUG("~p result facts ~p ~n",[{?MODULE,?LINE}, Got ] ),
	  
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

      create_hbase_json_filter({ unicode:characters_to_binary ( Pat ),  Name})
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
%META Facts must   chang manually
%   store_meta_fact(Name,  common:get_logical_name(TreeEts, ?META_FACTS) , [
% 					  {"facts_count", "1" },
% 					  {"facts_reqs","1"},
% 					  {"facts_w","1"}
meta_info({'meta', FactName, count, Val },  Prefix) when is_atom(FactName) ->
      MetaTable = common:get_logical_name(Prefix, ?META_FACTS),
      FactNameL = erlang:atom_to_list(FactName),
      common:inner_to_int(hbase_low_get_key(MetaTable, FactNameL,"stat", "facts_count"))

;
meta_info({'meta', FactName, requests, Val },  Prefix) when is_atom(FactName) ->
      MetaTable = common:get_logical_name(Prefix, ?META_FACTS),
      FactNameL = erlang:atom_to_list(FactName),
       common:inner_to_int(hbase_low_get_key(MetaTable, FactNameL, "stat", "facts_reqs"))
      
;
meta_info({'meta', FactName, weight, Val },  Prefix) when is_atom(FactName) ->
      MetaTable = common:get_logical_name(Prefix, ?META_FACTS),
      FactNameL = erlang:atom_to_list(FactName),      
       common:inner_to_int(hbase_low_get_key(MetaTable, FactNameL, "stat",  "facts_w"))
;
meta_info(_, _)  ->
 false
.


hbase_low_put_key(Table, Key, Family, Key2, Value)->
        hbase_low_put_key(Table, Key, Family, Key2, Value, ?USE_THRIFT)
.

hbase_low_put_key(Table, Key, Family, Key2, Value, 1)->
        fact_hbase_thrift:put_key(Table, Key, Family, Key2, Value)
;
hbase_low_put_key(Table, Key, Family, Key2, Value, 0)->

%  curl -X POST -H "Content-Type: text/xml" 
% --data '<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>
% <CellSet><Row key="bmV3X3Jvd19mcm9t"><Cell column="cGFyYW1zOmRhdGE=">aHJlbl90ZXN0</Cell></Row></CellSet>'
% http://hd-test-2.ceb.loc:60050/test_fact/row6/params:data
    { Hbase_Res, Host } = get_rand_host(),
    MakeCellSet = make_cell_normal(Family, [ { Key2,  Value  } ] ),
    BKey= base64:encode(Key),
    Body = <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?><CellSet><Row key=\"", 
		    BKey/binary, "\">", MakeCellSet/binary, "</Row></CellSet>" >>,    
    
    case catch  httpc:request( 'post', { Hbase_Res++Table++"/"++Key++"/"++Family,
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
                             throw({hbase_exception, Res })

    end
    

.

hbase_low_get_key(Table, Key, Family,  SecondKey)->
    hbase_low_get_key(Table, Key, Family,  SecondKey,?USE_THRIFT).

hbase_low_get_key(Table, Key, Family,  SecondKey,1)->
    fact_hbase_thrift:low_get_key(Table, Key, Family,  SecondKey)

;
hbase_low_get_key(Table, Key, Family,  SecondKey, 0)->
       { Hbase_Res, Host } = get_rand_host(),
       case catch  httpc:request( get, 
				    { Hbase_Res++Table++"/"++Key++"/"++Family++":"++SecondKey,
				    [ { "Accept","application/json"}, {"Host", Host }]},
				    [ { connect_timeout,?DEFAULT_TIMEOUT },
				      { timeout, ?DEFAULT_TIMEOUT }
				      
				    ],
				    [ {sync, true}, { body_format, binary } ] ) of
                    { ok, { {_NewVersion, 200, _NewReasonPhrase}, _NewHeaders, Text1 } } ->  
			     ?DEBUG("~p process key value ~p~n",[{?MODULE,?LINE}, Text1]),
			     onevalue(Text1);
                    { ok, { {_NewVersion, 404, _NewReasonPhrase}, _NewHeaders, _Text1 } } ->  
                             throw({hbase_exception, not_found});
                    Res -> 
			    ?DEBUG("~p  got  key   return ~p ~n  ~p  ~n",
				    [{?MODULE,?LINE},
				    {Table, Key, Family,  SecondKey}, Res ]),
                            throw({hbase_exception, Res})         
	end
.

		  
onevalue(Text)->

      Json = jsx:decode( Text  ),
%       [{<<"Row">>,
%                                                         [[{<<"key">>,<<"dGVzdF9mYWN0">>},
%                                                           {<<"Cell">>,
%                                                            [[{<<"column">>,<<"c3RhdDpmYWN0c19yZXFz">>},
%                                                              {<<"timestamp">>,1368882465730},
%                                                              {<<"$">>,<<"MQ==">>}]]}]]}]
      [ { _Row, [[ _KeyRow, { _Cell, [ OneValue ] } ]] } ] = Json, 
      get_val(OneValue, <<"$">>)      
.

