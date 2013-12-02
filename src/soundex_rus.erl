%% -*- coding: utf-8 -*-
-module(soundex_rus).

-export([start/1, start/2]).


-spec start(string()) -> string().
start(Str) ->
	start(Str,[lower,token])
.

-spec start(string(), [atom()]) -> string().
start(Str, [H|T]) ->
	case H of
		lower -> start(str:to_lower(Str),T);
		token -> start(str:tokens(Str),T);
		_ -> start(Str, T)
	end
;
start(Lists, []) ->
	[soud(X) || X <- Lists]
.

-spec soud(string()) -> string().
soud([H|Str]) ->
	[H]++soudex(Str)
.

-spec soudex(string()) -> string().
soudex(Str) ->
	F = [
		 {"[уеыаоэяиюьъaehiouwy]+",""}
		,{"[бпвфbfpv]+","1"}
		,{"[сцзкгхcgjkqsxz]+","2"} 
		,{"[тдdt]+","3"}
		,{"[лйl]+","4"}
		,{"[мнmn]+","5"}
		,{"[рr]+","6"}
		,{"[жшщч]+","7"}
	],
	Data = lists:foldl(fun({Pattern, Replacement}, Subject) -> re:replace(Subject, Pattern, Replacement, [global, {return, list}, unicode]) end, npr_sgl(Str),F),
	normalized(del_dubl(Data))
.

-spec normalized(string()) -> string().
normalized(Str) when length(Str) < 4 -> normalized(Str++"0");
normalized(Str) when length(Str) == 4 -> Str;
normalized(Str) when length(Str) > 4 -> string:substr(Str, 1,4).


-spec del_dubl(string()) -> string().
del_dubl([H|T]) ->
	[H] ++ del_dubl(T, H)
;
del_dubl(_) ->
	[]
.

-spec del_dubl(string(), number()) -> string().
del_dubl([H| T], Chr) when (is_integer(H)) and (H == Chr) ->
	del_dubl(T, Chr)
;
del_dubl([H|_T] = Str, Chr) when (is_integer(H)) and (H /= Chr) ->
	del_dubl(Str)
;
del_dubl([], _) -> [].


-spec npr_sgl(string()) -> string().
npr_sgl(Str) -> 
	F = [
		 {"^с(бвгджзлмнркпстфхцчшщ)","з"}
		,{"тс","ц"}
		,{"тьс","ц"}
		,{"стн","сн"}
		,{"стл","сл"}
		,{"здн","зн"}
		,{"рдц","рц"}
		,{"рдч","рч"}
		,{"стц","сц"}
		,{"здц","зц"}
		,{"нтск","нск"}
		,{"ндск","нск"}
		,{"ндц","нц"}
		,{"нтств","нств"}
		,{"стск","сск"}
		,{"лнц","нц"}
		,{"вств","ств"}
		],
	lists:foldl(fun({Pattern, Replacement}, Subject) -> re:replace(Subject, Pattern, Replacement, [global, {return, list}, unicode]) end, Str,F)
.