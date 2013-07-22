
-define(DEFAULT_TIMEOUT, infinity).%%miliseconds
-define(IO_SERVER, io).
-define(FATAL_WAIT_TIME, infinity).
-define(META_INFO_BATCH, 3). %%for using as batch param for scanner meta info
-define(AIM_COUNTER, aim_counter).

-define(DEV_BUILD,1). %%this uncommented line means that hbase driver will use avias


-define(USE_HBASE,1).
-define(UPDATE_STAT_INTERVAL,120000).%%interval of updating meta stat of prolog statements

-define(SIMPLE_HBASE_ASSERT,1).

-define(ENABLE_LOG,1).
-define(COMPILE_TRACE, 1).

-define('DEV_DEBUG_MATCH'(Str, Pars ), true ).
-define(ROOT, console_root).
-define(PREFIX, prefix).

-ifdef(ENABLE_LOG).

-define(TEST,1).
-define(DEV_TEST,1).
-define(TIMER, 1).
-define(LOGGING, 1).
-define(SYNC_DEBUG, 1). %%for debug process's calls
-define(SYNC_COUNT, 1). %%count
-define(LOG_APPLICATION,   application:start(log4erl), log4erl:conf(?LOG_CONF) ).

-else.

-define(LOG_APPLICATION,  true ).

-endif.

-define(FACT_STAT(Ets), io:format("Facts  ~p ~n",[ ets:tab2list(Ets)  ]) ).

-define(SYSTEM_STAT(Ets, Pointers), io:format("System ~p prolog ~p  pointers is ~p ~n",[ length(processes()), 
                                                                                         ets:info(Ets,size), 
                                                                                         Pointers ]) ).




-define(PAUSE, io:get_line('pause : ')).

-define(LOCAL, 1).
-define(LOG_CONF,"log.conf").

%-define(WEB,1).

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

-ifdef( COMPILE_TRACE).
-define( 'TRACE'(I, Tree,  Body, Context),  prolog_trace:trace(I, Tree,  Body, Context) ).
-define( 'TRACE2'(I, Tree, Res, Body), prolog_trace:trace2(I, Tree,  Res, Body) ).
-else.
-define( TRACE(I, Tree, Res, Body),  true ).
-define( 'TRACE2'(I, Tree, Res, Body), true   ).
-endif.


-ifdef(USE_HBASE).
-define('INCLUDE_HBASE'(X),  fact_hbase:load_rules2ets(X) ).
-else.
-define('INCLUDE_HBASE'(X),  true ).
-endif.

-ifdef(TIMER).
-define('TC'(Str, Pars ), log4erl:info(Str, Pars) ).
-else.
-define('TC'(Str, Pars ), true ).
-endif.


-ifdef(SYNC_DEBUG).
-define('WAIT'(Str, Pars ), log4erl:debug(Str, Pars) ).
-else.
-define('WAIT'(Str, Pars ), true ).
-endif.








-ifdef(SYNC_COUNT).

-define('COUNT'(Str, Pars ), log4erl:info(Str, Pars) ).
-else.
-define('COUNT'(Str, Pars ), true ).
-endif.

-ifdef(LOGGING).

-define('LOG'(Str, Pars ), log4erl:debug(Str, Pars) ).
-else.
-define('LOG'(Str, Pars ), true ).
-endif.



-ifdef(DEV_TEST).
-define('DEV_DEBUG'(Str, Pars ), log4erl:debug(Str, Pars) ).
-else.
-define('DEV_DEBUG'(Str, Pars ), true ).
-endif.

-ifdef(TEST).

-define('DEBUG'(Str, Pars ), log4erl:debug(Str, Pars) ).
-else.
-define('DEBUG'(Str, Pars ), true ).
-endif.

-define(TEMP_SHELL_AIM, temp_shell_aim). 
-define(LAMBDA, 'lambda;lambda_function'). 


-ifdef(LOCAL).
-define(HOST, "hd-test-2.ceb.loc:60050").
-define(HBASE_RES,"http://hd-test-2.ceb.loc:60050/").
-else.
-define(HOST, "127.0.0.1:60050").
-define(HBASE_RES,"http://127.0.0.1:60050/").
-endif.


%HBASE COLUMNS
-define(LINKS_FAMILY, "links").
-define(CACHE_FAMILY, "cache").

-define(STAT_FAMILY, "stat").
-define(FAMILY,"params").
-define(CODE_COLUMN,"code").
-define(RULES_TABLE,"rules").
-define(META_FACTS,"meta").

-define(RABBIT_SPEED,700).
-define(SL_TIMER,60000). %minute
% pay(Re,"75000.00",Currency,Date,From_okpo,To_okpo,From_account,To_account,FromName,ToName,FromMfo,ToMfo, Desc,Ip).

-define(LIMIT,1). %% LIMIT and GET_FACT_PACK must be equal 
-define(GET_FACT_PACK, 1). 
%%TODO realize algorithm for return first element, but in background continue process of receiveing data from hbase
%%for example we return first element...and save to the memory 1024 records and in next call return from memory


-define(UNDEF, undefined).

-define(RESTART_CONVERTER, 120*1000).

-define(ERWS_LINK, erws_link_trace).
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

-define(HBASE_HOSTS, 
        [
         {"http://avias-db-2.ceb.loc:60050/", "avias-db-2.ceb.loc:60050" },
         {"http://avias-db-2.ceb.loc:60050/", "avias-db-2.ceb.loc:60050" },
         {"http://avias-db-2.ceb.loc:60050/", "avias-db-2.ceb.loc:60050" },
         {"http://avias-db-2.ceb.loc:60050/", "avias-db-2.ceb.loc:60050" },
        {"http://avias-db-2.ceb.loc:60050/", "avias-db-2.ceb.loc:60050" }
        ]).
-define(HBASE_HOSTS_COUNT,5).
-else.

-define(HBASE_HOSTS, 
	[
	 {"http://hd-test-2.ceb.loc:60050/", "hd-test-2.ceb.loc:60050" },
	 {"http://es-1.ceb.loc:60050/", "es-1.ceb.loc:60050" },
	 {"http://es-2.ceb.loc:60050/", "es-2.ceb.loc:60050" },
	 {"http://es-3.ceb.loc:60050/", "es-3.ceb.loc:60050" }
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
'to_float',
'to_integer',
'to_list',
'write_unicode',
'length',
'localtime',
'date_diff',


'inner_retract___',
'meta',
'functor',
'arg',
'atom',
'copy_namespace',
'use_namespace',
'create_namespace',
'abolish',
'integer',
'list',
'atomic',
'float',
'var',
'call',
'nonvar',
'assertz',
'assert',
'asserta',
'retract',
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
'=..'
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
