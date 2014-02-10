-module(eprolog_sup).  
-behaviour(supervisor).  
-export([start_link/0]).  
-export([init/1]).  
-include_lib("prolog.hrl").  
  
start_link() ->  
        supervisor:start_link({local, ?MODULE}, ?MODULE, []).  
      
init([]) ->  

        Statistic = {"monitor",
        {
             converter_monitor, start_link, [  ]},
             permanent, infinity, worker , [ converter_monitor ]
        },
        ThriftPool = {"thrift_connection_pool",
             {thrift_connection_pool, start_link, [  ] },
             permanent, infinity, worker , [ thrift_connection_pool ]
        },
        {ok, { {one_for_one, 5, 10}, [ThriftPool,  Statistic ] } }.  
