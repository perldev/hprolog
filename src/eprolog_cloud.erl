-module(eprolog_cloud).

-export([cloud_entity/3, stop_cloud/3, start_cloud/5, process_cloud/4, cloud_entity_counters/3]).
-include("prolog.hrl").
-include_lib("thrift/src/hbase_types.hrl").


-spec generate_key(tuple() ) -> list().

generate_key( _ )->
        {T1, T2,T3}  = now(),
        integer_to_list(T1) ++ integer_to_list(T2) ++ integer_to_list(T3)
.

%%experimental feature
-spec process_cloud(tuple(), [], any(), atom()  )-> true;
                   (tuple(), nonempty_list(), atom(), atom() )-> true|false.

                   
%%TODO adding functionality for big CLOUDS move it to separate tables of files
process_cloud(_Body, [], _RuleName, _Tree  )->
        true
;
process_cloud(Body, TableName, RuleName, TreeEts  )->
        [_Name| ProtoType ] = tuple_to_list(Body),
        Key = generate_key(ProtoType),
        NameSpace = common:get_logical_name(TreeEts),
        {true, ResultDict, Descriptor}  =  prolog:call({RuleName, Body, {'___ResultList'} }, ?TRACE_OFF, NameSpace ),
        prolog:finish(Descriptor),
        Value = jsx:encode( common:prolog_term_to_jlist(ProtoType) ),
        ProtoTypeEntityList = prolog_matching:find_var_no(ResultDict, {'___ResultList'} ),
        ?LOG("~p entities ~p ~n",[{?MODULE,?LINE}, ProtoTypeEntityList ]),
        lists:foreach(fun(Elem)->
                                LElem = common:inner_to_list(Elem),        
                                ?LOG("~p put elem to key ~p ~n",[{?MODULE,?LINE}, {TableName, LElem , ?FAMILY, Key, Value }]),
                                ResultAdd  = fact_hbase:hbase_low_put_key(TableName, LElem , ?FAMILY, Key, Value ),
                                ?LOG("~p RESULT OF GATHERING CLOUD IS ~p ~n",[{?MODULE,?LINE}, ResultAdd ])
                      end,
                      ProtoTypeEntityList
        ),
        true 
.

-spec stop_cloud(atom(), list(), atom())-> true|false.

stop_cloud(FactName, MetaTable, MetaTableEts)->
      case ets:lookup(MetaTableEts, FactName) of
           []->  true;
           [ #meta_info{cloud = []} ]->
                 true;
           [#meta_info{name = FactName, 
                       arity = Arity, 
                       hash_function = Hash_Function, 
                       cloud = CloudName
                       }]->
                 Res = fact_hbase:delete_table(CloudName),
                 %%TODO add transaction behaviour
                 fact_hbase:del_key( atom_to_list(FactName), MetaTable,  "description", ?CLOUD_KEY),
                 fact_hbase:del_key( atom_to_list(FactName), MetaTable,  "description", ?CLOUD_RULE),
                 ets:insert(MetaTableEts, 
                            #meta_info{name = FactName, 
                                       arity = Arity, 
                                       hash_function = Hash_Function,
                                       cloud = "",
                                       cloud_decomposition = undefined
                           }),                 
                 Res
      end 
.
        
-spec start_cloud(atom(), list(), atom(),  list(), atom())-> true|false.

start_cloud(FactName, CloudName, CloudRule, MetaTable, MetaTableEts)->
      case ets:lookup(MetaTableEts, FactName) of
           []->  false;
           [ #meta_info{name = FactName, 
                                   arity = Arity, 
                                   hash_function = Hash_Function,
                                   cloud = []
                        } ] ->
                 %%TODO  add transaction behaviour
                 LCloudRule = atom_to_list( CloudRule ),
                 Res = fact_hbase:create_new_fact_table( CloudName ),
                 fact_hbase:hbase_low_put_key(MetaTable, atom_to_list(FactName), "description", ?CLOUD_KEY, CloudName),
                 fact_hbase:hbase_low_put_key(MetaTable, atom_to_list(FactName), "description", ?CLOUD_RULE,LCloudRule ),
                 ets:insert(MetaTableEts, 
                            #meta_info{name = FactName, 
                                   arity = Arity, 
                                   hash_function = Hash_Function,
                                   cloud = CloudName,
                                   cloud_decomposition = CloudRule
                           }),      
                 Res;
           [#meta_info{name = FactName, 
                       cloud = CloudName }]->
                 throw({instantiation_error, { already_existed_cloud, FactName, CloudName } })
      end 
.
               
-spec cloud_entity(atom(), list(), any() )-> list().

cloud_entity(FactName, Entity, MetaTable )->
        MetaTableEts = common:get_logical_name(MetaTable, ?META),
        case ets:lookup(MetaTableEts, FactName) of
                [ #meta_info{ cloud = [] } ]->
                        false;
                [ #meta_info{ cloud = CloudTable } ]->
                       fact_hbase:custom_get_key( CloudTable, ?FAMILY, Entity , fun process_cloud_entity/1 )
        end.        

-spec cloud_entity_counters(atom(), list(), any() )-> list().
%%%TODO implement this
cloud_entity_counters(FactName, Entity, MetaTable)->
        MetaTableEts = common:get_logical_name(MetaTable, ?META),
        case ets:lookup(MetaTableEts, FactName) of
                [ #meta_info{ cloud = [] } ]->
                        false;
                [ #meta_info{ cloud = CloudTable } ]->
                        Res = fact_hbase:custom_get_key( CloudTable, ?FAMILY, Entity, fun process_cloud_entity/1),
                        Res
                
        end.
        
process_cloud_entity([])->
         []
;       
process_cloud_entity([Item])->
    Dict = Item#tRowResult.columns,
%     [{tCell,
%                                       <<"000028c5bd009fb38237eef14d971e14">>,
%                                       1374493085988}]
    dict:fold(fun(Key, Value, List)->     
                         ?LOG("~p process ~p ~n",[{?MODULE,?LINE}, Value]),
                         {tCell, Json,_TimeStamp} = Value,
                         NewValue = jsx:decode(Json),
                         NewValueProto = common:json_to_term_list(NewValue),
                         [NewValueProto|List] 
               end, [], Dict)
.