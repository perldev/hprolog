-module(common).
-compile(export_all).
-include("prolog.hrl").


regis_io_server(TreeEts, Io)->
    ets:insert(TreeEts, {?IO_SERVER, Io} )
.
parse_as_term(S)->
      {ok, Terms , _L1} = erlog_scan:string( S ),
      erlog_parse:term(Terms).

%TODO adding parsing predicates
console_read(_)->
       erlog_io:read('').
console_get_char(_)->
       io:get_chars("", 1).
console_write(_, X)->
      io:format("~p",[X]).
      
console_writenl(_, X)->      
    io:format("~p~n",[X]).
    
console_nl(_)->
	io:format("~n",[]).

%TODO adding parsing predicates

web_console_read(TreeEts)->
	[{_, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),

	Parent ! {result, read, erlang:self() },
	receive 
	     {read, List }->
		  Term = inner_to_list(List),
		  (catch parse_as_term(Term) )
	     %TODO add after statement
	end.
      
       
web_console_get_char(TreeEts)->
	[{_, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
        Parent ! {result, get_char, erlang:self() },
	receive
	      {char, Char } ->
		  inner_to_atom(Char)
	%TODO add after statement
        end.
        
%TODO add parsing term into string, now we use erlang term
web_console_write(TreeEts, X)->
      Str  = io_lib:format("~ts",[to_web_string(X)]),
      [{_, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
      Parent ! {result, write, Str}.
      
web_console_writenl(TreeEts, X)->    
      Str =  io_lib:format("~ts<br/>",[ X ]),
      [{_, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
      Parent ! {result, write, Str}.
      
web_console_nl(TreeEts)->
      [{_, Parent}] = ets:lookup(TreeEts, ?IO_SERVER ),
      Parent ! {result, write,"<br/>"}. 
    
    
to_web_string(X) when is_list(X)->
    lists:flatten( lists:map(fun to_string/1, X) );   
to_web_string(X) when is_number(X)->
    io_lib:format("~p",X);
to_web_string(X) when is_atom(X)->
    io_lib:format("~p",X);    
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
  
inner_to_list(E) when is_integer(E)->
  integer_to_list(E);

inner_to_list(E) when is_float(E)->
  float_to_list(E);
inner_to_list(E)  when is_binary(E)->
  unicode:characters_to_list(E);
inner_to_list(E) ->
  E.  
  
inner_to_int(E) when is_list(E)->
  case catch list_to_integer(E) of
      {'EXIT', _}-> 0;
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

get_logical_name(Prefix, Name) when is_list(Name), is_list(Prefix) ->
     Prefix ++ Name 
;
get_logical_name(Prefix, Name) when is_list(Prefix)->
    list_to_atom( Prefix ++ erlang:atom_to_list(Name) )
;
get_logical_name(Prefix, Name) when is_atom(Prefix)->
    [{_, RealPrefix}] =  ets:lookup(Prefix, ?PREFIX),
    get_logical_name(RealPrefix, Name)

;
get_logical_name(Prefix, Name) when is_integer(Prefix)->
    [{_, RealPrefix}] = ets:lookup(Prefix, ?PREFIX),
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
    [$0,integer_to_list(Item)]

;
fill_null_integer2list(Item)->
 integer_to_list(Item)

.

%%if not available  return current_date
get_date(Date)->
      case  catch get_date_params(Date) of
       { {Year,Month,Day}, {Hour,Minute} } ->
           { {Year,Month,Day}, {Hour,Minute} };
       _ ->
            default_date()
      end

.
get_date_params(Date)->

%  ("1231-32-23 33:43"," ")
   [ Date,Time ] =   string:tokens(Date," "),
   [ Year,Month,Day ] = string:tokens(Date,"-"),
   [  Hour,Minute ] = string:tokens(Time,":"),
   { { list_to_integer(Year), list_to_integer(Month),list_to_integer(Day) }, {list_to_integer(Hour),list_to_integer(Minute)} }


.
default_date()->
      {{Year,Month,Day},{Hour,Min,_}}=calendar:local_time(),
      CreateTime={ {Year,Month,Day},{Hour,Min} },
      CreateTime
.

get_str_date({ {Year,Month,Day},{Hour,Min} } )->
  io_lib:format("~.B-~.B-~.B ~.B:~.B",[
  Year,  Month, Day,
  Hour,Min
  ])

.
%Jul 17 2012 08:56
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
get_date(_In, _)->
  false.

minute([_H1,_H2,$:,M1,M2])->
     {Out,_} = string:to_integer([M1,M2]),
     case is_integer(Out) of
	  true-> Out;
	  _ ->false
    end
    
;
minute(_)->
    
    false
.
hour([H1,H2,$:,_M1,_M2])->
      {Out,_} = string:to_integer([H1,H2]),
     case is_integer(Out) of
	  true-> Out;
	  _ ->false
    end
    
;
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
  
