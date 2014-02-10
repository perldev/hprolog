-module(simple_background_process).

-export([ start_link/4, temporary/3,restartable/3 ]).


-spec start_link(temporary|restartable, atom(), atom(), list()  )-> {ok, pid()}.

start_link(Type,  Module,  Func, Params)->
       Pid = spawn_link(simple_background_process, Type, [Module, Func, Params]  ),
       Pid ! start,
       {ok, Pid}.


-spec restartable(atom(), atom(), list() ) -> true.
restartable(Module, Func, Params)->
        Res = receive 
                start->
                        erlang:apply(Module,Func,Params)
              end,
        case Res of
             true ->
                exit(normal);
             _ ->
                exit(Res)
        end.
        
-spec temporary(atom(), atom(), list() ) -> true.
temporary(Module, Func, Params)->
        receive 
                start->
                        erlang:apply(Module,Func,Params)
        end,
        exit(normal).