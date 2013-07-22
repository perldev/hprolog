-module(fact_hbase_thrift).

-export([start_recr/2]).

-include_lib("hbase_thrift/include/hbase_types.hrl").
-include("prolog.hrl").

start_recr("testpay" = Table, _ProtoType) ->
    ProtoType = [<<"bogdan">>, {'DESC'}], 
   
    case hbase_thrift_api:connect(default, []) of
        {ok, State} ->
            Filter = create_filter4all_values(ProtoType, <<>>),
	    io:format("filter: ~p~n", [Filter]),
            Size = length(ProtoType),
            {State1, {ok, ScannerId}} = generate_scanner(Table, Filter, State),
	    io:format("scanner: ~p~n", [ScannerId]), 
            List = get_data(?LIMIT*Size, ScannerId, ProtoType, State1),
	    {ScannerId, List};
        _Any ->
            timer:sleep(5000),
            start_recr(Table, ProtoType)
    end. 


get_data(Limit, Id, Prototype, State) ->
    case thrift_client:call(State, scannerGetList, [Id, Limit]) of
        {ok, Data} ->
            Res = process_data(Data, Prototype),
            io:format("result: ~p~n", [Res]);
        Error -> Error
    end.
    
process_data(Data, ProtoType) ->
    %Data = [{tRowResult,<<"90c68ea27aab41601cd822bc5e84214f">>,
    %      [{<<"params:2">>,{tCell,<<"1">>,1371731480251}}]}],
    Result = get_key_value(Data, []),
    Columns = lists:seq(1, length(ProtoType)),
    ?DEBUG("~p process data after reduce ~p ~n",[{?MODULE,?LINE}, Result]), 
    lists:map(fun(Elem) ->		   
	%%TODO REPLACE IT!!!!!!!!!!!
    F = fun(E) -> get_val_simple(Elem, list_to_binary(?FAMILY ++ ":" ++ integer_to_list(E))) end,
	        list_to_tuple(lists:map(F, Columns))
	    end, 
    Result).


get_key_value([], Acc) -> Acc;
get_key_value([#tRowResult{columns = Columns}|Rest], Acc) ->
    F = fun({Column, TCell}) -> {Column, TCell#tCell.value} end, 
    L = lists:map(F, Columns),
    get_key_value(Rest, [L|Acc]).


get_val_simple(E, Key)->
	case lists:keyfind(Key, 1, E) of
	    {_, Value} -> 
            binary_to_list(Value);
		false -> 
		    ?WAIT("~p mistake with ~p", [E,Key]), 
            <<"">>
	end.

%% gen scanner
generate_scanner(Table, Filter, State) ->
    Args = [ Table,
             #tScan{
                    startRow = "", 
                    columns = [], 
                    filterString = Filter}, []
    ],
    hbase_thrift_api:execute(State, scannerOpenWithScan, Args).

%% gen filters
create_filter4all_values([], Filters) ->
    Filters;
create_filter4all_values([{Var}|T], Filters) when is_atom(Var) ->
    create_filter4all_values(T, Filters);
create_filter4all_values([Value|T], Filter) when is_binary(Value), byte_size(Filter) =:= 0 ->
    NewFilter = create_one_filter(Value), 
    create_filter4all_values(T, NewFilter);
create_filter4all_values([Value|T], Filter) when is_binary(Value), is_binary(Filter) ->
    NewFilter = create_one_filter(Value),
    Compound = thrift_filters:compound_filters(default, Filter, NewFilter, <<"AND">>),
    create_filter4all_values(T, Compound).

create_one_filter(Value) ->
    {ok, Filter} = thrift_filters:init([]),
    Filter1 = Filter#filter{name = 'ValueFilter',
                            compare_operator = <<"=">>,
                            comparator = Value
    },
    thrift_filters:generate(Filter1).   
