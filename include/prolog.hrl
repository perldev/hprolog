
%%default count of pool to the thrift server
%% wheather using thrift
-define(USE_THRIFT, 1).
-define(SIMPLE_HBASE_ASSERT,   common:check_source(TreeEts) ).

%-define(THRIFT_RECONNECT_TIMEOUT,130000).
%-define(THRIFT_MAX_RECONNECT_COUNT, 10).
%-define(INTERVAL_INVOKE_SCANNER, 2000000). %microseconds


-define(DEFAULT_TIMEOUT, 210000).%%miliseconds
%%miliseconds
-define(FATAL_WAIT_TIME,210000).% 60000).
-define(META_INFO_BATCH, 3). %%for using as batch param for scanner meta info

%-define(MAX_ETS_BUFFER_SIZE, 3000000).
%-define(UPDATE_STAT_INTERVAL,120000).%%interval of updating meta stat of prolog statements





%-define(DEV_BUILD,1). %%this uncommented line means that hbase driver will use avias

-define(USE_HBASE,1).

%-define(LOAD_LOG,1).


%-define(ENABLE_LOG,1).

%%for connect prolog with web console
-define(WEB,1).

-define('WRITE_UNICODE'(Parent,X), common:console_write_unicode(Parent, X) ).


-ifdef(WEB).

-define('WRITE'(Parent,X), common:web_console_write(Parent, X)  ).
-define('WRITELN'(Parent,X), common:web_console_writenl(Parent, X) ).
-define(GET_CHAR(Parent),  common:web_console_get_char(Parent) ).
-define(NL(Parent),common:web_console_nl(Parent) ).   

-define(READ(Parent), common:web_console_read(Parent) ).
-define(READ_STR(Parent), common:web_console_read_str(Parent) ).

-else.
-define('WRITE'(Parent,X), common:console_write(Parent, X)  ).
-define('WRITELN'(Parent,X), common:console_writenl(Parent, X) ).
-define(GET_CHAR(Parent),  common:console_get_char(Parent) ).

-define(READ(Parent), common:console_read(Parent) ).
-define(READ_STR(Parent), common:console_read_str(Parent) ).



-define(NL(Parent),common:console_nl(Parent) ).     
       
-endif.




-define(AIM_COUNTER, aim_counter).




-define(IO_SERVER, io).



-define(COMPILE_TRACE, 1).

-define('DEV_DEBUG_MATCH'(Str, Pars ), true ).
-define(ROOT, console_root).
-define(PREFIX, prefix).



-ifdef(LOAD_LOG).

-define(LOG_APPLICATION,application:start(lager)).

-else.

-define(LOG_APPLICATION,true).

-endif.

-ifdef(ENABLE_LOG).




-define(TEST,1).
-define(DEV_TEST,1).
-define(TIMER, 1).
-define(LOGGING, 1).
-define(SYNC_DEBUG, 1). %%for debug process's calls
-define(SYNC_COUNT, 1). %%count


-endif.

-define(FACT_STAT(Ets), io:format("Facts  ~p ~n",[ ets:tab2list(Ets)  ]) ).

-define(SYSTEM_STAT(Ets, Pointers), io:format("System ~p prolog ~p  pointers is ~p ~n",[ length(processes()), 
                                                                                         ets:info(Ets,size), 
                                                                                         Pointers ]) ).




-define(PAUSE, io:get_line('pause : ')).

-define(LOCAL, 1).
-define(LOG_CONF,"log.conf").



-ifdef( COMPILE_TRACE).
-define( 'TRACE'(I, Tree,  Body, Context),  prolog_trace:trace(I, Tree,  Body, Context) ).
-define( 'TRACE2'(I, Tree, Res, Body), prolog_trace:trace2(I, Tree,  Res, Body) ).
-else.
-define( TRACE(I, Tree, Res, Body),  true ).
-define( 'TRACE2'(I, Tree, Res, Body), true   ).
-endif.

-define('CUT_LOG'(Str,Pars), lager:warning(Str, Pars) ).


-ifdef(USE_HBASE).
-define('INCLUDE_HBASE'(X),  fact_hbase:load_rules2ets(X) ).

-define('THRIFT_POOL'(Str, Pars ), true  ).% lager:warning(Str, Pars) ).
-define('THRIFT_LOG'(Str, Pars ),  true ).%lager:warning(Str, Pars) ).

-else.
-define('THRIFT_POOL'(Str, Pars ),   true ).

-define('INCLUDE_HBASE'(X),  true ).
-define('THRIFT_LOG'(Str, Pars ),   true).

-endif.

-ifdef(TIMER).
-define('TC'(Str, Pars ), lager:warning(Str, Pars) ).
-else.
-define('TC'(Str, Pars ), true ).
-endif.


-ifdef(SYNC_DEBUG).
-define('WAIT'(Str, Pars ), lager:warning(Str, Pars) ).
-else.
-define('WAIT'(Str, Pars ), true ).
-endif.








-ifdef(SYNC_COUNT).

-define('COUNT'(Str, Pars ), lager:warning(Str, Pars) ).
-else.
-define('COUNT'(Str, Pars ), true ).
-endif.

-ifdef(LOGGING).

-define('LOG'(Str, Pars ), lager:info(Str, Pars) ).
-else.
-define('LOG'(Str, Pars ), true ).
-endif.



-ifdef(DEV_TEST).
-define('DEV_DEBUG'(Str, Pars ), lager:debug(Str, Pars) ).
-else.
-define('DEV_DEBUG'(Str, Pars ), true ).
-endif.

-ifdef(TEST).

-define('DEBUG'(Str, Pars ), lager:debug(Str, Pars) ).
-else.
-define('DEBUG'(Str, Pars ), true ).
-endif.

-define(TEMP_SHELL_AIM, temp_shell_aim). 
-define(LAMBDA, 'lambda;lambda_function'). 




%HBASE COLUMNS
-define(LINKS_FAMILY, "links").
-define(CACHE_FAMILY, "cache").

-define(STAT_FAMILY, "stat").
-define(FAMILY,"params").
-define(CODE_COLUMN,"code").
-define(RULES_TABLE,"rules").
-define(META_FACTS,"meta").
-define(CLOUD_KEY, "cloud").
-define(RABBIT_SPEED, 700).
-define(SL_TIMER, 60000). %minute
% pay(Re,"75000.00",Currency,Date,From_okpo,To_okpo,From_account,To_account,FromName,ToName,FromMfo,ToMfo, Desc,Ip).

-define(LIMIT,500). %% LIMIT and GET_FACT_PACK must be equal 
-define(GET_FACT_PACK, 1). 
%%TODO realize algorithm for return first element, but in background continue process of receiveing data from hbase
%%for example we return first element...and save to the memory 1024 records and in next call return from memory


-define(UNDEF, undefined).
-define(COUNT_MAPPERS, 1).

-define(RESTART_CONVERTER, 120*1000).

-define(DYNAMIC_STRUCTS, op_structs).
-define(INNER, inner).
-define(RULES, rules_inner).
-define(META_LINKS, meta_facts_links).


-define(META_WEIGHTS, meta_weights).

-define(AI_RULES, ai_rules_inner).
-define(META, meta_facts).
-define(APPLICATION, application).


-define(STAT, stat).
-define(HBASE_INDEX, hbase_fact_index).
-define(SCANNERS_HOSTS_TABLE, hbase_scanners ).
-define(SCANNERS_HOSTS_LINK, hbase_scanners_hosts ).


-ifdef(DEV_BUILD).

-define(THRIFT_CONF,  { "hceb.loc", 9091 }  ). 

-define(HBASE_HOSTS, 
        [
         {"h-db/", "0" },
         {"http://0050/", "aoc:60050" },
         {"http:60050/", "avias:60050" },
         {"http050/", "avias.loc:60050" },
         {"http://av0050/", "avias-:60050" }
        ]).
-define(HBASE_HOSTS_COUNT,5).

-else.

-define(THRIFT_CONF,  { "h.loc", 9090 }  ).

-define(HBASE_HOSTS, 
	[
	 {"http:/.loc:60050/", "t-b.loc:60050" },
	 {"http://h.loc:60050/", "hd-te050" },
	 {"http://hd-tloc:60050/", "hd-t050" },
	 {"http://hd-eb.loc:60050/", "hd-tec:60050" }
         ]).
-define(HBASE_HOSTS_COUNT,4).

-endif.



-define(EMPTY,{}).



%%traces status for prolog kernel

-define(DEBUG_ON,  true_debug).
-define(DEBUG_OFF, false_debug ).
-define(TRACE_ON,  true_trace ).
-define(TRACE_OFF, false_trace).
-define(DEBUG_STATUS, debug_status).

-record(browscap, { browser = null, version = null, majorver = null, minorver = null, platform = null }).

-define(BUILTIN_PREDICATES,
[
'date_sub',     
'date_add',
'id',
'to_float',
'to_integer',
'to_list',
'to_atom', %%realize !!
'write_unicode',
'length',
'localtime',
'date_diff',
'add',
'reverse',
'soundex',
'cloud',
'start_cloud',
'stop_cloud',
'cloud_counters',

'member_tail',
'member',

'once',
'atom_length',
'copy_term',
'clause',
'meta',
'functor',
'arg',
'atom',
'copy_namespace',
'use_namespace',
'create_namespace',
'add_index_fact',
'drop_index_fact',
'abolish',
'integer',
'list',
'number',
'atomic',
'float',
'var',
'call',
'nonvar',
'compound',
'assertz',
'assert',
'asserta',
'string_tokens',%tokens(String, SeparatorList, X)
'get_char',
'read',
'read_str',
'write',
'writeln',
'nl',
'system_stat',
'fact_statistic',
'op',
'false',
'fail',
'true',
'date',
'is',
'=:=',
'=',
'\\=',
'==',
'>=',
'=<',
'>',
'<',
'=\\=',
'=..',
'\\+',
%%MATH function 
'sin',
'cos',
'tan',
'asin',
acos ,
atan ,
atan2, %(X, Y) ,
sinh ,
cosh ,
tanh ,
asinh ,
acosh ,
atanh ,
exp ,
log ,
log10 ,
pow ,%(X, Y) ,
sqrt ,
pi %()

]

).


-record(filter, {
    name,
    comparator_type, 
    comparator,
    family,
    qualifier,
    compare_operator,
    latest_version_boolean,
    filterIfColumnMissing_boolean,
    minColumn,
    minColumnInclusive_bool,
    maxColumn,
    maxColumnInclusive_bool,
    opts
    }).

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
    
