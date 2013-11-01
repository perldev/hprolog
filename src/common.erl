-module(common).
-compile(export_all).
-include("prolog.hrl").

-export([console_write/2, web_console_write/2, console_write_unicode/2,
          web_console_read/1, web_console_writenl/2,
         console_get_char/1, check_source/1, console_read/1, console_nl/1,generate_id/0, get_logical_name/1,member_tail/2 ]).

         
         
member_tail(Item, [])->
    false
;
member_tail(Item, [Item|Tail])->
    Tail;
member_tail(Item, [_|Tail])->
    member_tail(Item, Tail).
    

check_source(TreeEts)->
    case ets:lookup(TreeEts, hbase) of
        [] -> 0;
        [{system_record, hbase, A}]-> A
    end
.

         
         
return_count(TreeEts)->

    case catch ets:lookup(TreeEts, aim_counter) of
        []->0;
        [{_,_, Count}]-> Count
    end
.

regis_io_server(TreeEts, Io)->
    ets:insert(TreeEts, {system_record, ?IO_SERVER, Io} )
.

parse_as_term(S)->
      {ok, Terms , _L1} = erlog_scan:string( S ),
      erlog_parse:term(Terms).

%TODO adding parsing predicates
console_read_str(_)-> 
       io:put_chars("\""),
       io:get_line(""),
       io:put_chars("\"").
       
console_read(_)->
       erlog_io:read('').
console_get_char(_)->
       io:get_chars("", 1).
console_write(_, X)->
       io:format("~p",[X]).
      
console_write_unicode(_,X)->
     io:format("~ts",[X]).

     
console_writenl(_, X)->      
    io:format("~p~n",[X]).
    
console_nl(_)->
    io:format("~n",[]).

%TODO adding parsing predicates


web_console_read_str(TreeEts)->
        [{system_record, _, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
        Parent ! {result, read, erlang:self() },
        receive 
             {read, List }->
%                   io:format("~p read from web ~p",[{?MODULE,?LINE},List ]),
                  list_to_binary(List)
        end

.

web_console_read(TreeEts)->
	[{system_record, _, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),

	Parent ! {result, read, erlang:self() },
	receive 
	     {read, List }->
		  Term = list_to_binary(List),
% 		  (catch 
		  parse_as_term(Term) 
% 		  )
	     %TODO add after statement
	end.
      
       
web_console_get_char(TreeEts)->
	[{system_record, _, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
        Parent ! {result, get_char, erlang:self() },
	receive
	      {char, Char } ->
		  inner_to_atom(Char)
	%TODO add after statement
        end.
        
%TODO add parsing term into string, now we use erlang term
web_console_write(TreeEts, X)->
      Str  = io_lib:format("~ts",[to_web_string(X)]),
      [{system_record, _, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
      Parent ! {result, write, Str}.
      
web_console_writenl(TreeEts, X)->    
      Str =  io_lib:format("~ts<br/>",[ X ]),
      [{system_record, _, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
      Parent ! {result, write, Str}.
      
web_console_nl(TreeEts)->
      [{system_record, _, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
      Parent ! {result, write,"<br/>"}. 
    
    
to_web_string(X) when is_list(X)->
    lists:flatten( lists:map(fun to_string/1, X) );   
to_web_string(X) when is_number(X)->
    io_lib:format("~p",[X]);
to_web_string(X) when is_atom(X)->
    io_lib:format("~p",[X]);    
to_web_string(X) when is_tuple(X)->
      PrologCode =  erlog_io:write1(X),
      PrologCode.   
      
to_string(X) when is_integer(X)->
    X;
to_string(X) when is_tuple(X)->
    erlog_io:write1(X);   
to_string(X) when is_float(X)->
    io_lib:format("~p",X);
to_string(X) when is_list(X)->
    io_lib:format("[~ts] ",[to_web_string(X)]).

	

    

inner_to_list(E) when is_list(E)->
  E;
inner_to_list(E) when is_atom(E)->
  atom_to_list(E);
inner_to_list(E) when is_tuple(E)->
  false;
inner_to_list(E) when is_integer(E)->
  integer_to_list(E);

inner_to_list(E) when is_float(E)->
  float_to_list(E);
inner_to_list(E)  when is_binary(E)->
  unicode:characters_to_list(E);
inner_to_list(E) ->
  E.  

  
inner_to_int(E) when is_float(E)->
  erlang:round(E) 
  
;
inner_to_int(E) when is_tuple(E)->
    false;
inner_to_int(E) when is_list(E)->
  case catch list_to_integer(E) of
      {'EXIT', _}-> false;
      Number-> Number
  end
;
inner_to_int(E) when is_atom(E)->
   inner_to_int(atom_to_list(E))
; 
inner_to_int(E) when is_binary(E)->
    inner_to_int(binary_to_list(E));
inner_to_int(E) ->
  E.

inner_to_float(E) when is_integer(E)->
  erlang:float(E) 
;
inner_to_float(E) when is_tuple(E)->
    false;
inner_to_float(E) when is_list(E)->
  case string:to_float(E ++ ".0") of
      {'error', _}-> false;
      {Number, _Tail}-> Number
  end
;
inner_to_float(E) when is_atom(E)->
   inner_to_float(atom_to_list(E))
; 
inner_to_float(E) when is_binary(E)->
    inner_to_float(binary_to_list(E));
inner_to_float(E) ->
  E.  
  
  
  

inner_to_atom(E) when is_atom(E)->
    E
;
inner_to_atom(E) when is_binary(E)->
    list_to_atom( binary_to_list(E)   )
;
inner_to_atom(E) when is_list(E)->
    list_to_atom(E)
;
inner_to_atom(E) when is_integer(E)->
   list_to_atom( integer_to_list(E) )
; 
inner_to_atom(E) when is_float(E)->
   list_to_atom( float_to_list(E) )
.




get_namespace_name(NameSpace, RealFactName)->
    lists:sublist(RealFactName, length(NameSpace)+1, length( RealFactName )- length(NameSpace) )
.

get_logical_name(Prefix)->
    [{system_record, _, RealPrefix}] =  ets:lookup(Prefix, ?PREFIX),
    RealPrefix
.

get_logical_name(Prefix, Name) when is_list(Name), is_list(Prefix) ->
     Prefix ++ Name 
;
get_logical_name(Prefix, Name) when is_list(Prefix)->
    list_to_atom( Prefix ++ erlang:atom_to_list(Name) )
;
get_logical_name(Prefix, Name) when is_atom(Prefix)->
    [{system_record, _, RealPrefix}] =  ets:lookup(Prefix, ?PREFIX),
    get_logical_name(RealPrefix, Name)

;
get_logical_name(Prefix, Name) when is_integer(Prefix)->
    [{system_record, _, RealPrefix}] = ets:lookup(Prefix, ?PREFIX),
    get_logical_name(RealPrefix, Name)
.

my_delete_element(Index,ProtoType)->
    List = tuple_to_list(ProtoType),
    my_delete_element(Index, 1, List, [])
.

my_delete_element(Index, Index, [_Head|List], Result)->
  my_delete_element(Index, Index+1, List, Result)
;
my_delete_element(Index, Index1, [Head|List], Result)->
  my_delete_element(Index, Index1+1, List, [Head|Result])
;

my_delete_element(_Index, _Index1, [ ], Result)->
  list_to_tuple( lists:reverse(Result) )
.
generate_id()->
    {MSecs, Secs, MiSecs} = erlang:now(),
    %this is not the perfect fast procedure but it work in thread cause this
    % im do not want to rewrite it 
    Res = lists:flatten( io_lib:format("~.10B~.10B~.10B",[ MSecs, Secs, MiSecs ]) ), %reference has only 14 symbols
    list_to_integer(Res)
.

generate_ref()->
    {MSecs, Secs, MiSecs} = erlang:now(),
    %this is not the perfect fast procedure but it work in thread cause this
    % im do not want to rewrite it 
    Res = lists:flatten( io_lib:format("~.36B~p~.36Be",[ MSecs, Secs, MiSecs ]) ), %reference has only 14 symbols
    Res
.



inner_float_to_list(Fl)->
    [ O ] = io_lib:format("~.2f",[Fl+0.0]),O
.

get_now_str(Now)->
     { { Year, Month, Day }, { Hour,Minute,_Second } }=calendar:now_to_local_time(Now),
     get_str_date(  {  { Year, Month, Day }, {Hour,Minute }  } )
.
process_card(Card)->
 	[Head|Tail] = Card,
	[F,S] = string:right(Tail,2),
	[Head,$*,F,S]
.

list_to_unicode(List)->
      lists:map(fun to_unicode/1, List )
.
to_unicode(E) when is_integer(E)->
      integer_to_list(E);
to_unicode(E) when is_float(E)->
      inner_float_to_list(E);   
to_unicode(E) when is_atom(E)->
      atom_to_list(E);          
to_unicode(E) when is_binary(E)->
      unicode:characters_to_list(E);
to_unicode(E) when is_list(E)->
      E.

prepare_log(Base) ->
        File = make_filename(Base),
        case filelib:ensure_dir(File) of
                ok ->
                        error_logger:tty(true),
                        error_logger:logfile(close),
                        case error_logger:logfile({open, File}) of
                                ok -> error_logger:tty(false);
                                {error, allready_have_logfile} -> error_logger:tty(false);
                                {error, Reason} ->
                                        error_logger:error_msg("can't open logfile (~p): ~p~n",
                                        [File, Reason])
                        end;
                Reason ->
                        error_logger:error_msg("can't create path for logfile (~p): ~p~n", [File, Reason])
        end
.
make_filename(Base) ->
	
	{{Y,M,D},{H,Mi,S}}=calendar:now_to_local_time(now()),
	L = lists:flatten( io_lib:format("~.10B~.10B~.10B_~.10B~.10B~.10B", [Y,M,D,H,Mi,S]) ),
        Str = Base ++ "_" ++ L ++ ".log",
        lists:flatten(Str)
.



search_text(_Phone,_Hash,[ ])->
       error

;
search_text(Phone,Hash,[ Head | Tail])->
      Key=string:left(Phone,Head),
      Res=dict:find(Key,Hash),
      search_text(Res,Phone,Hash,Tail )
.
search_text(error,_Phone,_Hash,[ ] )->
        error
;
search_text({ok,Text},_Phone,_Hash,_Tail )->
        Text
;
search_text(error,Phone,Hash, [ Head | Tail] )->
        Key = string:left(Phone,Head),
        Res = dict:find(Key,Hash),
        search_text(Res,Phone,Hash,Tail )
.


search_text_default(_Phone,Hash,[ ])->
      {_,Text}=dict:find(default,Hash),
     Text

;
search_text_default(Phone,Hash,[ Head | Tail])->
      Key=string:left(Phone,Head),
      Res=dict:find(Key,Hash),
      search_text_default(Res,Phone,Hash,Tail )
.
search_text_default(error,_Phone,Hash,[ ] )->
       {_,Text}=dict:find(default,Hash),
        Text
;
search_text_default({ok,Text},_Phone,_Hash,_Tail )->
        Text
;
search_text_default(error,Phone,Hash, [ Head | Tail] )->
        Key=string:left(Phone,Head),
        Res=dict:find(Key,Hash),
        search_text_default(Res,Phone,Hash,Tail )
.




get_date_string(Date)->
       {  {Year,Month,Day} , {Hour,Minute,_Second} }=Date,
       lists:concat( 
	[fill_null_integer2list(Day),".",fill_null_integer2list(Month),".",integer_to_list(Year)," ",fill_null_integer2list(Hour),":",fill_null_integer2list(Minute)]
	)
.
fill_null_integer2list(Item) when Item<10 ->
    [$0|integer_to_list(Item)]

;
fill_null_integer2list(Item)->
 integer_to_list(Item)

.



date_diff(First, Second, Type)->
   D1 =  get_date_params(First),
   D2 =  get_date_params(Second),
   time_difference(D1, D2, Type)
.

time_difference({ {Year, Month, Day }, {Hour,Minute}  }, 
                Params = { {_Year1, _Month1, _Day1 }, {_Hour1, _Minute1, _Second1} }, Type )->
                time_difference( { {Year, Month, Day }, {Hour,Minute, 0}  } , Params, Type)
   
;
time_difference(Params = { {_Year, _Month, _Day }, {_Hour,_Minute, _Second}  }, 
                         { {Year1, Month1, Day1 }, {Hour1, Minute1} }, Type )->
                         
               time_difference( Params , { {Year1, Month1, Day1 }, {Hour1, Minute1, 0} }, Type)          
  
;
time_difference({ {Year, Month, Day }, {Hour,Minute}  }, 
                         { {Year1, Month1, Day1 }, {Hour1, Minute1} }, Type )->
                         
               time_difference( { {Year, Month, Day }, {Hour,Minute, 0}  }, 
                                { {Year1, Month1, Day1 }, {Hour1, Minute1, 0} }, Type)          
  
;
time_difference(Date1 = { {Year, Month, Day }, {Hour,Minute, Second}  }, 
                Date2 = { {Year1, Month1, Day1 }, {Hour1, Minute1, Second1} }, 'day' )->
    { Days, _Time} = calendar:time_difference(Date1, Date2),
    Days
;
time_difference(Date1 = { {Year, Month, Day }, {Hour,Minute, Second}  }, 
                Date2 = { {Year1, Month1, Day1 }, {Hour1, Minute1,Second1 } }, 'minute' )->
    { Days, { DHour, DMinute, _} } = calendar:time_difference(Date1, Date2),
    DHour*60 + DMinute + ( Days*24*60)
;
time_difference(Date1 = { {Year, Month, Day }, {Hour,Minute, Second}  }, 
                Date2 = { {Year1, Month1, Day1 }, {Hour1, Minute1, Second1} }, 'hour' )->
    { Days, { DHour, _DMinute, _} } = calendar:time_difference(Date1, Date2),
    DHour +  Days*24
;
time_difference(Date1 = { {Year, Month, Day }, {Hour,Minute, Second}  }, 
                Date2 = { {Year1, Month1, Day1 }, {Hour1, Minute1, Second1} }, 'second' )->

    { Days, {DHour, DMinute, DSeconds} } = calendar:time_difference(Date1, Date2),
    DHour*3600 + DMinute*60 + ( Days* 86400) + DSeconds
.

get_date()->
    Date = default_date(),
    { {Year,Month,Day}, {Hour,Min, Seconds} } = Date,
    lists:concat( 
        [ int2month(Month)," ",
          fill_null_integer2list(Day)," ",
          integer_to_list(Year)," ",
          fill_null_integer2list(Hour),":",fill_null_integer2list(Min),":",fill_null_integer2list(Seconds)]
        )
     

.

%%if not available  return current_date
get_date(Date)->
      case  catch get_date_params(Date) of
       { {Year,Month,Day}, {Hour, Minute} } ->
           { {Year,Month,Day}, {Hour,Minute} };
        { {Year,Month,Day}, {Hour, Minute, Seconds} } ->
           { {Year,Month,Day}, {Hour,Minute, Seconds} };    
       _ ->
            default_date()
      end
.
%  Jul 05 2013 14:15:29
get_date_params([Mon1,Mon2,Mon3,32, Day1,Day2,32,Year1,Year2,Year3,Year4,32, Hour1,Hour2,$:,Minute1, Minute2,$:, Second1, Second2 ] )->
   Year= [Year1,Year2,Year3,Year4],
   Month = month2int([Mon1,Mon2,Mon3]),
   Day = [ Day1, Day2 ],
   Hour = [ Hour1,Hour2],
   Minute = [Minute1, Minute2],
   Seconds = [Second1, Second2],
  
 { { list_to_integer(Year), Month, list_to_integer(Day) }, 
   { list_to_integer(Hour),list_to_integer(Minute),  list_to_integer(Seconds)  } }
;
%  Jul 05 2013 14:15
get_date_params([Mon1,Mon2,Mon3,32, Day1,Day2,32,Year1,Year2,Year3,Year4,32, Hour1,Hour2,$:,Minute1, Minute2 ] )->
   Year= [Year1,Year2,Year3,Year4],
   Month = month2int([Mon1,Mon2,Mon3]),
   Day = [ Day1, Day2 ],
   Hour = [ Hour1,Hour2],
   Minute = [Minute1, Minute2],
   { { list_to_integer(Year), Month, list_to_integer(Day) }, {list_to_integer(Hour),list_to_integer(Minute),  0 }  }
;

get_date_params(Date)->
%  ("1231-32-23 33:43"," ")
   [ Date,Time ] =   string:tokens(Date," "),
   [ Year,Month,Day ] = string:tokens(Date,"-"),
   [  Hour,Minute ] = string:tokens(Time,":"),
   { { list_to_integer(Year), list_to_integer(Month),list_to_integer(Day) }, {list_to_integer(Hour),list_to_integer(Minute)} }
.
default_date()->
      {{Year,Month,Day},{Hour,Min, Seconds}} = calendar:local_time(),
      CreateTime={ {Year,Month,Day},{Hour,Min, Seconds} },
      CreateTime
.

get_str_date({ {Year,Month,Day},{Hour,Min} } )->
  io_lib:format("~.B-~.B-~.B ~.B:~.B",[
  Year,  Month, Day,
  Hour,Min
  ])

.
%Jul 17 2012 08:56:90
get_date(In, year)->
	Params = date_params(In),
	 { Out, _Some } = string:to_integer( lists:nth(3, Params) ),
	 case is_integer(Out) of
	  true-> Out;
	  _ ->false
	  end
;
get_date(In, month)->
	 Params = date_params(In),
	 Month = lists:nth(1, Params), 
	 case  month2int(Month) of
	      false -> false;
	      Out -> Out
	 end
;
get_date(In, day)->
    Params = date_params(In),

    { Out, _Some } = string:to_integer(lists:nth(2, Params)),
    case is_integer(Out) of
	  true-> Out;
	  _ ->false
    end
	  
	
;
get_date(In, hour)->
  Params = date_params(In),
  Time = lists:nth(4, Params),
  hour(Time)
;
get_date(In, minute)->
  Params = date_params(In),
  Time = lists:nth(4, Params),
  minute(Time)
;
get_date(In, second)->
  Params = date_params(In),
  Time = lists:nth(4, Params),
  second(Time)
;
get_date(_In, _)->
  false.

second([_H1,_H2,$:,_M1,_M2,$:,S1,S2])->
     {Out,_} = string:to_integer([S1,S2]),
     case is_integer(Out) of
          true-> Out;
          _ ->false
    end
    
;
second(_)->
    false
.  
  
minute([_H1,_H2,$:,M1,M2|Tail])->
     {Out,_} = string:to_integer([M1,M2]),
     case is_integer(Out) of
	  true-> Out;
	  _ ->false
    end
    
;
minute(_)->
    
    false
.
hour([H1,H2,$:|Tail])->
      {Out,_} = string:to_integer([H1,H2]),
     case is_integer(Out) of
          true-> Out;
          _ ->false
    end;
    
hour(_)->
    false
.
  
date_params(In) when is_binary(In)->
   date_params(binary_to_list(In)) 
;  
date_params(In)->
    string:tokens(In, " ").
    
month2int("Jan")->
  1;
month2int("Feb")->
  2;
month2int("Mar")->
  3;
month2int("Apr")->
  4;
month2int("May")->
  5;
month2int("Jun")->
  6;
month2int("Jul")->
  7;
month2int("Aug")->
  8;
month2int("Sep")->
  9;
month2int("Oct")->
  10;
month2int("Nov")->
  11;
month2int("Dec")->
  12;
month2int(_)->
  false.
  
  
int2month(1)->
    "Jan";
int2month(2)->
    "Feb";
int2month(3)->
    "Mar";
int2month(4)->
    "Apr";
int2month(5)->
    "May";
int2month(6)->
    "Jun";
int2month(7)->
    "Jul";
int2month(8)->
    "Aug";
int2month(9)->
    "Sep";
int2month(10)->
    "Oct";
int2month(11)->
    "Nov";
int2month(12)->
    "Dec";
int2month(_)->
    false.



  
  
