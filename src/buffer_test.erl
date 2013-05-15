-module(buffer_test).
-compile(export_all).

new(Name)->
  ets:new(Name,[named_table, bag, public]).


  
test(Count)->
  
   spawn(buffer_test, fill,
	 [  buffer_test:new(test), 
	  [
		  { fuzzy_choice, [   [a,s,d,f,g,h,j,k,l,u,t ]   ]  },
		  { fuzzy_str,[] },
		  { fuzzy_num,[] },
		  { fuzzy_str,[] }
	 ],
	 Count 
	] )

.

  
  
fill(Name,Profile, Count)->
    
    fill(Name, Profile, 0,Count)
.
fill(Name, Profile, Count,Count)->
  ok;
fill(Name, Profile, In, Count)->
  Value =  lists:map(fun({E,Params})->
			    erlang:apply(buffer_test, E, Params )
		       end, Profile),
  Tuple = list_to_tuple(Value),
  
  
  ets:insert(Name, Tuple ),  
  NewI = In+1,
  fill(Name, Profile,  NewI, Count).



fuzzy_choice(List)->
    {MSecs, Secs, MiSecs} = erlang:now(),
    %this is not the perfect fast procedure but it work in thread cause this
    % im do not want to rewrite it 
    NumKey =  (MiSecs rem 10) + 1,
    lists:nth(NumKey, List)
.    

fuzzy_str()->
    {MSecs, Secs, MiSecs} = erlang:now(),
    %this is not the perfect fast procedure but it work in thread cause this
    % im do not want to rewrite it 
    Res = lists:flatten( io_lib:format("~.36B~.36B~.36B",[ MSecs, Secs, MiSecs ]) ), %reference has only 14 symbols
    Res
.    
fuzzy_num()->
    {MSecs, Secs, MiSecs} = erlang:now(),
    %this is not the perfect fast procedure but it work in thread cause this
    % im do not want to rewrite it 
    Res = lists:flatten( io_lib:format("~.B~.B~.B",[ MSecs, Secs, MiSecs ]) ), %reference has only 14 symbols
    list_to_integer(Res)
.    