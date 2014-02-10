-module(eprolog_cloud).

-export([cloud_entity/3, stop_cloud/3, start_cloud/4, process_cloud/2, cloud_entity_counters/3]).
-include("prolog.hrl").
-include_lib("thrift/src/hbase_types.hrl").


-spec generate_key(tuple() ) -> list().

generate_key( _ )->
        {T1, T2,T3}  = now(),
        integer_to_list(T1) ++ integer_to_list(T2) ++ integer_to_list(T3)
.

%%experimental feature
-spec process_cloud(tuple(), [] )-> true;
                   (tuple(), nonempty_list() )-> true|false.

                   
%%TODO adding functionality for big CLOUDS move it to separate tables of files
process_cloud(Body, []  )->
        true
;
process_cloud(Body, TableName  )->
        [_Name| ProtoType ] = tuple_to_list(Body),
        Key = generate_key(ProtoType),
        Value = jsx:encode( common:prolog_term_to_jlist(ProtoType) ),
        lists:foreach(fun(Elem)->
                                ?LOG("~p put elem to key ~p ~n",[{?MODULE,?LINE}, Elem]),
                                Result  = fact_hbase:hbase_low_put_key(TableName, common:inner_to_list(Elem), ?FAMILY, Key, Value ),
                                ?LOG("~p RESULT OF GATHERING CLOUD IS ~p ~n",[{?MODULE,?LINE}, Result ])
                      end,
                      ProtoType
        ),
        true %%TODO remove this
.

-spec stop_cloud(atom(), list(), atom())-> true|false.

stop_cloud(FactName, MetaTable, MetaTableEts)->
      case ets:lookup(MetaTableEts, FactName) of
           []->  true;
           [{FactName, Arity, Hash_Function, []}]->
                 true;
           [{FactName, Arity, Hash_Function, CloudName}]->
                 Res = fact_hbase:delete_table(CloudName),
                 %%TODO add transaction behaviour
                 fact_hbase:del_key( atom_to_list(FactName), MetaTable,  "description", ?CLOUD_KEY),
                 ets:insert(MetaTableEts, {FactName, Arity, Hash_Function, ""} ),          
                 Res
      end 
.
        
-spec start_cloud(atom(), list(), list(), atom())-> true|false.

start_cloud(FactName, CloudName, MetaTable, MetaTableEts)->
      case ets:lookup(MetaTableEts, FactName) of
           []->  false;
           [{FactName, Arity, Hash_Function, []}]->
                 %%TODO  add transaction behaviour
                 Res = fact_hbase:create_new_fact_table( CloudName ),
                 fact_hbase:hbase_low_put_key(MetaTable, atom_to_list(FactName), "description", ?CLOUD_KEY, CloudName),
                 ets:insert(MetaTableEts, {FactName, Arity, Hash_Function, CloudName} ),        
                 Res;
           [{FactName, Arity, Hash_Function, CloudName}]->
                 throw({instantiation_error, { already_existed_cloud, FactName, CloudName } })
      end 
.
               
-spec cloud_entity(atom(), list(), any() )-> list().

cloud_entity(FactName, Entity, MetaTable )->
        MetaTableEts = common:get_logical_name(MetaTable, ?META),
        case ets:lookup(MetaTableEts, FactName) of
                [ {FactName, _Arity, _HashFunc, [] } ]->
                        false;
                [ {FactName, _Arity, _HashFunc, CloudTable }]->
                       fact_hbase:custom_get_key( CloudTable, ?FAMILY, Entity , fun process_cloud_entity/1 )
        end.        

-spec cloud_entity_counters(atom(), list(), any() )-> list().

cloud_entity_counters(FactName, Entity, MetaTable)->
        MetaTableEts = common:get_logical_name(MetaTable, ?META),

        case ets:lookup(MetaTableEts, FactName) of
                [ {FactName, _Arity, _HashFunc, [] } ]->
                        false;
                [ {FactName, _Arity, _HashFunc, CloudTable }]->
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