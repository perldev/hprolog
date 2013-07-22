%% использование фактов
%% простые правила

car(chrysler, 130000, 3, red, 12000).
car(ford, 90000, 4, gray, 25000).
car(datsun, 8000, 1, red, 30000).

truck(ford, 80000, 6, blue, 8000).
truck(datsun, 50000, 5, orange, 20000).
truck(toyota, 25000, 2, black, 25000).

figur(point(1,2)).
figur(line(point(1,2), point(1,2))).
figur(point(2,2)).
figur(circle(point(1,2),100)).

# :- op(xyz,fact(Y,U)).


%%      сын,отец   
father('victor', 'andrey').
father('petr', 'andrey').
father('andreyVictorovich','victor').
father('vitalina','victor').
father('taras','victor').

man('andreyVictorovich').
man('victor').
man('petr').
man('taras').
man('andrey').
wom('vitalina').
man('andrey').

son(Son,X):-father(Son,X),man(Son).
dot(Dt,X):-father(Dt,X),wom(Dt).

broth(B,B2):-father(B,X), father(B2,X),B\=B2.
uncle(U,S):- father(U,X),broth(X,S).

%%%%%%%%%
test1(QQ):-writeln('1 ---family tree:'),
   broth(X,Y), write('brother'=X:Y), nl,
   dot(X1,_), write('doughter'=X1), nl,
   !,
   son(Son,'andrey'), writeln('son'=Son),
   %!,
   writeln('2 ---unification:'),
   figur(Z), figur(L), L=line(Z,MM),
   writeln(Z).
%    
test2(_):-figur(X),writeln(X),1=2;true.

s(1).
s(2).
s(3).
s(4).

test3(_):- s(X), write(X),false;nl.


member([X|_],X).
member([_|L],X):-member(L,X).

some_test(X1):- member([1,2,3,4],X1),assertz(aa(X1)),system_stat,write(X1),X1=4.

test5:- writeln('assertz'),
           member([1,2,3,4],X1),assertz(aa(X1)),system_stat,write(X1),X1=4,
           aa(Y1),write(Y1),Y1=4,
           !,
           nl,writeln('asserta'),
           member([1,2,3,4],X),asserta(a(X)),X=4,
           a(Y),write(Y),Y=1.

test_all:-
  test1(_), writeln('---ok---'),!,
  test2(X1), writeln('---ok---'),!,
  test3(X2), writeln('---ok---'),!,
  test5,write('---').
  