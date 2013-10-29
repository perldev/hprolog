-module(eprolog_app).  
-behaviour(application).  
-export([start/2, stop/1, start/0]).  

-include("prolog.hrl").

start(_StartType, _StartArgs) ->  
        eprolog_sup:start_link().  
        
   
        
start()->
  inets:start(),
  crypto:start(),
  application:start(lager),
  ok = application:start(public_key),
  ok = application:start(ssl),
  ok = application:start(compiler),
  ok = application:start(syntax_tools),
  application:start(eprolog)

.
      
stop(_State) ->  
        ok.  