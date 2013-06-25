-module(prolog_test).
-compile(export_all).
-include("prolog.hrl").

-define('UNIT_TEST_LOG'(Str, Pars ), log4erl:warn( Str, Pars ) ).
-define(TEST_LIST,[
		    {test1, 
		    { [ {'/',1,{'Y1'}},
			{'/',2,{'Y2'}},
			{'/',3,{'Y3'}},
			{'/',4,{'Y4'}},
			{'/',5,{'Y5'}},
			{'/',6,{'Y6'}},
			{'/',7,{'Y7'}},
			{'/',8,{'Y8'}}]
		    }
		    },
%%%%%%%%%%%%%%%%%%%	    
		    
		    {test2, 
		    {[{'/',1,y1},
		      {'/',2,y2},
		      {'/',3,y3},
		      {'/',4,y4},
		      {'/',5,y5},
		      {'/',6,y6},
		      {'/',7,y7},
		      {'/',8,y8}
		      ],
		      [ 
			{'/',2,y2},
			{'/',3,y3},
			{'/',4,y4},
			{'/',5,y5},
			{'/',6,y6},
			{'/',7,y7},
			{'/',8,y8}]
		    }
		    },
%%%%%%%%%%%%%%%%%%%%%%%		    
		     {test3, 
		    {
		     [{'/',1,4},
		      {'/',2,2},
		      {'/',3,7},
		      {'/',4,3},
		      {'/',5,6},
		      {'/',6,8},
		      {'/',7,5},
		      {'/',8,1}
		      ]
		      
		    }
		    },
%%%%%%%%%%%%%%%%%%%%%%%%%%		    
		   {test4, 
		    {
		      {some,1,2,3,45,5}, 
		      5
		      
		    }
		    },
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%		    
		    {
		      test5, 
		      {
		         {some_struct,4,5,7,8}
		      }
		    },
		    {
		     test6, 
		      {
		         
		         {'+', 1, 1 }
		      
		      }
		    },
		    {
		     test7, 
		      { {fact1,4,6},{fact2,8}, {'+',{fact1,4,6},{fact2,8}} }
		    },
		    {
		    test8, 
		        {4,6,7}     
		    },
		    {
		    test9, 
		        { 'bob','pat' }     
		    },
		    {
		    test10, 
		        { 'pam','jim' }     
		    },
		    {test11,
		      {8,[1,5,8,6,3,7,2,4] }		    
		    },
		    {test12,
		      {[3,4,5,6,7],[1,5,b],[3,4,5,6,7,1,5,b] }		    
		    },
		      {test13,
		      { false }		    
		    },
		    {test14,
		      { true }		    
		    },
		    {test15,
                      some              
                    },
                    {test16,
                      some              
                    },
                    {test17,
                      some              
                    },
                    {test18,
                      some              
                    }   
		    
	
		    
%%%%%%%%%%%%%%%%%%%%%%%%%%%

		   ]).


bug1()->
      prolog_test:start(),  
      TempAim = {some_test,{'X'} },
      ets:new(tree_processes,[ public, set, named_table,{ keypos, 2 } ] ), 
      ets:insert(tree_processes,  {system_record,prefix, ""}),
      debugger:quick(prolog, aim, [finish, root_console ,TempAim,  dict:new(), 
                                                        now(), tree_processes, root_console ]).
                                                      
		   
start()->
      inets:start(),
      crypto:start(),
      application:start(log4erl),
      log4erl:conf(?LOG_CONF),
      prolog:create_inner_structs(""),
      prolog:compile("","test.pl").
      
test_all()->
    process_flag(trap_exit, true),
    lists:map(fun({Name, Wait })->
		      erlang:apply(?MODULE, Name, [Wait])
		   end,?TEST_LIST)
.
  
   
got_wait(Index)->
   {value, {Name, Wait } } =  lists:keysearch(Index,1 ,?TEST_LIST),
   Wait
.
   
test1()->
  test1( got_wait(test1) ).
  
test1(Wait)->
    Aim = { pattern, {'X'} },
    sent_aim("test1 - got list to variable X", Aim, Wait )
.
test2()->
  test2( got_wait(test2) ).
  
test2(Wait)->
    Aim = { tail, [     {'/',1,y1 },
			{'/',2,y2 },
			{'/',3,y3 },
			{'/',4,y4 },
			{'/',5,y5 },
			{'/',6,y6 },
			{'/',7,y7 },
			{'/',8,y8 }], {'X111'} },
    sent_aim("test2 - got tail of list", Aim, Wait )
.
test3()->
  test3( got_wait(test3) ).
  
test3(Wait)->
    Aim = { call_chess, [
			  {'/',1,{'Y1'}},
			  {'/',2,{'Y2'}},
			  {'/',3,{'Y3'}},
			  {'/',4,{'Y4'}},
			  {'/',5,{'Y5'}},
			  {'/',6,{'Y6'}},
			  {'/',7,{'Y7'}},
			  {'/',8,{'Y8'}}
			  ]
			  },
    sent_aim("test3 - problem of 8 queens", Aim, Wait )
.
test4()->
  test4( got_wait(test4) ).
  
test4(Wait)->
    Aim = { match3,{'X1'},{'X'} },
    sent_aim("test4 - matching structs", Aim, Wait )
.
test5()->
  test5( got_wait(test5) ).
  
test5(Wait)->
    Aim = { match4,{'X'} },
    sent_aim("test5 - matching structs arguments", Aim, Wait )
.
test6()->
  test6( got_wait(test6) ).
  
test6(Wait)->
    Aim = { match,{'X'} },
    sent_aim("test6  - matching complain structs work", Aim, Wait )
.

test7()->
  test7( got_wait(test7) ).
  
test7(Wait)->
    Aim = { fact_strong,{'X'},{'Y'},{'Z'} },
    sent_aim("test7 complain rule with matching struct and return it", Aim, Wait )
.


test8()->
  test8( got_wait(test8) ).
  
test8(Wait)->
    Aim = { fact_strong1,{'X'},{'Y'},{'Z'} },
    sent_aim("test8 complain rule with matching struct", Aim, Wait )
.
test9()->
  test9( got_wait(test9) ).
  
test9(Wait)->
    Aim = { predecessor, {'X'},'pat' },
    sent_aim("test9 -classic parent example first simple ", Aim, Wait )
.
test10()->
  test10( got_wait(test10) ).
  
test10(Wait)->
    Aim = { predecessor, 'pam','jim' },
    sent_aim("test10 - classic parent example with recursion ", Aim, Wait )
.
test11()->
    test11(got_wait(test11)).
test11(Wait)->
    Aim = { common_queens , 8, [ 1,5,8,6,3,7,2,4 ] },
    sent_aim("test11 -classic chess for common case", Aim, Wait )
.    
test12()->
    test12(got_wait(test12)).
test12(Wait)->
    Aim = { add ,[3,4,5,6,7],[1,5,b], {'X111'} },
    sent_aim("test12 -rule of concat lists", Aim, Wait )
.    
test13()->
    test13(got_wait(test13)).
    
test13(Wait)->
    Aim =  test ,
    sent_aim("test13 - simple aim without arguments but use ! for fail", Aim, Wait )
.    
test14()->
    test14(got_wait(test14)).
test14(Wait)->
    Aim = test1,
    sent_aim("stest14 -simple aim without arguments ", Aim, Wait )
.    
test15()->
    test15(got_wait(test15)).
test15(Wait)->
    Aim = { female, {'X'} },
    sent_aim("test15 - facts unifieng ", Aim, Wait )
.    
test16()->
    test16(got_wait(test16)).
test16(Wait)->
    Aim = { fact, {'X'},{'Y'},{'Z'} },
    sent_aim("test16 - combine rules ", Aim, Wait )
.    
test17()->
    test17(got_wait(test17)).
test17(Wait)->
    Aim = { parent, pam,jim },
    sent_aim("test17 - fail fact search ", Aim, Wait )
.  

test18()->
    test18(got_wait(test18)).
test18(Wait)->
    Aim = { fact_fork, {'X'},{'Y'},{'Z'} },
    sent_aim("test17 - fail fact search ", Aim, Wait )
.  



%%for auto testing
sent_aim(TestName, Goal1, Wait) when is_atom(Goal1)->
	      Goal = {Goal1, true},
	      ?UNIT_TEST_LOG("start test : ~p~n", [TestName]),
	      TempAim = Goal,
	      ?UNIT_TEST_LOG(" TempAim : ~p~n", [TempAim]),
	      ets:new(tree_processes,[ public, set, named_table,{ keypos, 2 } ] ), 
              ets:insert(tree_processes, {system_record,?PREFIX, ""}),
	      prolog_trace:trace_on(?TRACE_OFF,tree_processes ),
	      ?UNIT_TEST_LOG("~p create buffer for test ~p ~n",
				[ {?MODULE,?LINE}, ets:tab2list(tree_processes)]),
	      StartTime = erlang:now(),
	      BackPid = spawn_link(prolog, conv3, [ {true}, Goal,  dict:new(), 
				    erlang:self(), now() , tree_processes]),
	      Result = check_test(TempAim, Goal, BackPid, StartTime, Wait, TestName ),
	      ets:delete(tree_processes),
	      Result
;
sent_aim(TestName, Goal, Wait)->
	      ?UNIT_TEST_LOG("start test : ~p~n", [TestName]),
% 	      {TempAim, _ShellContext } =  prolog_shell:make_temp_aim(Goal), 
 	      TempAim = Goal,
	      ?UNIT_TEST_LOG(" TempAim : ~p~n", [TempAim]),
	      ets:new(tree_processes,[ public, set, named_table, { keypos, 2 } ] ), 
	      ets:insert(tree_processes, {system_record,?PREFIX, ""}),
	      prolog_trace:trace_on(?TRACE_OFF,tree_processes ),
	      ?UNIT_TEST_LOG("~p create buffer for test ~p ~n",
				[ {?MODULE,?LINE}, ets:tab2list(tree_processes)]),
	      [_UName|Proto] = tuple_to_list(TempAim),
	      StartTime = erlang:now(),
% 	      aim(PrevIndex, ProtoType, Context, Index, TreeEts )

	      Res = erlang:apply(prolog, aim, [finish, root_console ,TempAim,  dict:new(), 
				                      now(), tree_processes ]),
	      Result = check_test(TempAim, Goal, Res, StartTime, Wait, TestName ),
	      ets:delete(tree_processes),
	      Result
.


check_test(  TempAim , Goal, Res, StartTime, Wait, TestName)->
        ?UNIT_TEST_LOG("~p exit aim ~p~n",[{?MODULE,?LINE}, {Res, Wait} ])
.
finish_backpid(BackPid)->
		   unlink(BackPid),
		   exit(BackPid, finish).