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

%%      сын,отец   
father('виктор', 'андрей').
father('петр', 'андрей').
father('андрейВикторович','виктор').
father('виталина','виктор').
father('тарас','виктор').

man('андрейВикторович').
man('виктор').
man('петр').
wom('виталина').
man(andray).

son(Son,X):-father(Son,X),man(Son).
dot(Dt,X):-father(Dt,X),wom(Dt).

broth(B,B2):-father(B,X), father(B2,X),B\=B2.
uncle(U,S):- father(U,X),broth(X,S).

%%%%%%%%%
test1(QQ):-writeln('1 ---family tree:'),
   broth(X,Y), write('братья'=X:Y), nl,
   dot(X1,_), write('дочь'=X1), nl,
   !,
   son(Son,'андрей'), writeln('son'=Son),
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

test5:- writeln('assertz'),
           member([1,2,3,4],X1),assertz(aa(X1)),X1=4,
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
  