%Рекурсивные предикаты

mas(a,0,1).
mas(a,1,2).
mas(a,2,33).
mas(a,3,4).

testgoal:-  !, writeln('Вывод элементов массива   a   '),
     mas(a,_,Elem), write('Elem='),write(Elem),
     nl, fail ; true.
testgoal.

%:-trace(testgoal), testgoal.

maxvector(M, V):- max1(M,0,3,0,V).
max1(_,N1,N2,V1,V1):-N1>N2, !.
max1(M,N1,N2,V1,V2):-mas(M,N1,V), V>V1,  N11 is N1+1, max1(M,N11,N2,V,V2).
max1(M,N1,N2,V1,V2):-mas(M,N1,V), V=<V1, N11 is N1+1, max1(M,N11,N2,V1,V2).

maxvector2(M, V):- max2(M,3,0,V).
max2(_,N2,V1,V1):-N2<0.
max2(M,N2,V1,V2):-mas(M,N2,V), V>V1, N11 is N2-1, max2(M,N11,V,V2).
max2(M,N2,V1,V2):-mas(M,N2,V), V=<V1, N11 is N2-1, max2(M,N11,V1,V2).

maxvector3(M, V):- max3(M,3,V).
max3(M,0,Max):-mas(M,0,Max).
max3(M,N2,Max):-mas(M,N2,V),!, N11 is N2-1, max3(M,N11,V2),(V2>V,!, Max=V2; V2=<V,Max=V).

maxvector4(M, V):- max4(M,3,V).
max4(M,0,Max):-mas(M,0,Max).
max4(M,N2,Max):-mas(M,N2,V), N11 is N2-1, max4(M,N11,V2),V2>V, Max=V2.
max4(M,N2,Max):-mas(M,N2,V), N11 is N2-1, max4(M,N11,V2),V2=<V, Max=V.

maxvector5(M,V):-mas(M,Ii,V), not((( mas(M,I,X),X>V ))).

maxvector6(M,V):- assert(m(0)), false.
maxvector6(M,V):- mas(M,Ii,V), retract(m(X)),(V>X,assert(m(V));V=<X,assert(m(X))),false.
maxvector6(M,V):-!,retract(m(V)).


summ(M,0,S):-mas(M,0,S).
summ(M,N,S):-mas(M,N,S0), N1 is N-1, summ(M,N1,S1), S is S0+S1.


summ2(X,-1,S,S):-  true.
summ2(M,N,S,Sr):-  writeln(N:S),
		    mas(M,N,S0), 
		    N1 is N-1,
		    S1 is S+S0,
		    summ2(M,N1,S1,Sr).



             
mt(a, 1,1, 100).
mt(a, 1,2, 500).
mt(a, 2,1, 300).
mt(a, 2,2, 100).

max_str(M,1,Y,Max):-mt(M,1,Y,Max).
max_str(M,N2,Y,Max):-mt(M,N2,Y,V),!, N11 is N2-1, max_str(M,N11,Y,V2),(V2>V,!, Max=V2; V2=<V,Max=V).

max_col(N,X,1,Max):-max_str(N,X,1,Max).
max_col(N,X,Y,Max):-max_str(N,X,Y,V),!, Y1 is Y-1, max_col(N,X,Y1,V2),(V2>V,!, Max=V2; V2=<V,Max=V).


%%%%%%%%%%%%%
test1:- maxvector(a, X), writeln('max'=X),!,
         maxvector2(a, X1), writeln('max'=X1),!,
         maxvector3(a, X2), writeln('max'=X2),!,
         maxvector4(a, X3), writeln('max'=X3),!,
         maxvector5(a, X4), writeln('max'=X4),!,
         maxvector6(a, X5), writeln('max'=X5).
         
test2:- summ(a,3,X), writeln('sum'=X),!,
        summ2(a,3,0,X1), writeln('sum'=X1).
        
test_all:- test1, writeln('---ok---'),
       test2, writeln('---ok---').


