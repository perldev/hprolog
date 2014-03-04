%% -*- coding: utf-8 -*-
-module(str).

-export([to_lower/1, tokens/1]).

-spec tokens(string()) -> [string()].
tokens(Str) ->
	%NormalString = re:replace(Str, "[\\,|\\<|\\.|\\>|\\/|\\?|\\;|\\:|\\'|\"|\|\\||\\[|\\{|\\]|\\}|\\~|\\`|\\!|\\@|\\#|\\$|\\%|\\^|\\&|\\*|\\(|\\)|\\_|\\%|\\(|\\)|\\-|\\=]+"," ",[global, {return, list},unicode]),
	%string:tokens(NormalString, " "),

	Data = re:replace(Str, "-", "", [global, {return, list},unicode]),
	case catch re:run(Data, "[а-я|-]+|[0-9]+|[a-z|-]+", 
                                  [unicode, global, {capture, all, list}])
                                 of
                {'EXIT', Reason} -> throw({exception, {Data,Str, Reason} }); 
                ResData ->
                tokens_match(ResData)
        end
.


tokens_match({match,List})->
        lists:concat(List);
tokens_match(_)->
        [].



-spec to_lower(string()) -> string().
to_lower(Str) ->
	[char_to_lower(X) || X <- Str]
.

-spec char_to_lower(char()) -> char().
char_to_lower($А) -> $а;
char_to_lower($Б) -> $б;
char_to_lower($В) -> $в;
char_to_lower($Г) -> $г;
char_to_lower($Д) -> $д;
char_to_lower($Е) -> $е;
char_to_lower($Ё) -> $е;
char_to_lower($ё) -> $е;
char_to_lower($Ж) -> $ж;
char_to_lower($З) -> $з;
char_to_lower($И) -> $и;
char_to_lower($Й) -> $й;
char_to_lower($К) -> $к;
char_to_lower($Л) -> $л;
char_to_lower($М) -> $м;
char_to_lower($Н) -> $н;
char_to_lower($О) -> $о;
char_to_lower($П) -> $п;
char_to_lower($Р) -> $р;
char_to_lower($С) -> $с;
char_to_lower($Т) -> $т;
char_to_lower($У) -> $у;
char_to_lower($Ф) -> $ф;
char_to_lower($Х) -> $х;
char_to_lower($Ц) -> $ц;
char_to_lower($Ч) -> $ч;
char_to_lower($Ш) -> $ш;
char_to_lower($Щ) -> $щ;
char_to_lower($Ъ) -> $ъ;
char_to_lower($Ы) -> $ы;
char_to_lower($Ь) -> $ь;
char_to_lower($Э) -> $э;
char_to_lower($Ю) -> $ю;
char_to_lower($Я) -> $я;
char_to_lower($A) -> $a;
char_to_lower($B) -> $b;
char_to_lower($C) -> $c;
char_to_lower($D) -> $d;
char_to_lower($E) -> $e;
char_to_lower($F) -> $f;
char_to_lower($G) -> $g;
char_to_lower($H) -> $h;
char_to_lower($I) -> $i;
char_to_lower($J) -> $j;
char_to_lower($K) -> $k;
char_to_lower($L) -> $l;
char_to_lower($M) -> $m;
char_to_lower($N) -> $n;
char_to_lower($O) -> $o;
char_to_lower($P) -> $p;
char_to_lower($Q) -> $q;
char_to_lower($R) -> $r;
char_to_lower($S) -> $s;
char_to_lower($T) -> $t;
char_to_lower($U) -> $u;
char_to_lower($V) -> $v;
char_to_lower($W) -> $w;
char_to_lower($X) -> $x;
char_to_lower($Y) -> $y;
char_to_lower($Z) -> $z;
char_to_lower(Ch) -> Ch.
