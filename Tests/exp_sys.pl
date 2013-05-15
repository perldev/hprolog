%% Простая экспертная система
%% использование переопределенных операций

:-op(600 ,fx , can).
:-op(600 ,fx , have).
:-op(600 ,fx , that).
:-op(1000 ,xfy , and).

X and Y :-  X,Y.

that human :- that mlek and have head and have think.
that leon :- that mlek and have tail and have teeth.
that sparrow :- that bird and have 'small size'.
that nightingale :- that bird and can sing.
that bird :- that animal and can fly.
that mlek :- that animal and can 'drink milk'.
that animal :- can breath.

%%% тут надо иметь стандартный предикат для ввода символов%%%
readchar(Ch):- get_char(Ch),get_char(_).

can X :- canmem(X,no),!,fail.
can X :- canmem(X,yes),!.
can X :- write('is it can :'), writeln(X),
         readchar(Ch),(Ch='y';Ch='Y'),assert(canmem(X,yes)),!.
can X :- assert(canmem(X,no)),!,fail.

have X :- havemem(X,no),!,fail.
have X :- havemem(X,yes),!.
have X :- write('is it have :'), writeln(X),
         readchar(Ch),(Ch='y';Ch='Y'),assert(havemem(X,yes)),!.
have X :- assert(havemem(X,no)),!,fail.

main:- retractall(havemem(_,_)),
   retractall(canmem(_,_)),
   that X, write('Your answer is:'),write(X);
   writeln('sorry...').


test_all:-main.

%%:-main.