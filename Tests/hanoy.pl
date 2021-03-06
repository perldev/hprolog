
hanoy(1,A,B,_,[A-B]):-!.
hanoy(N,A,B,C,L):-
                  N1 is N-1,
                  hanoy(N1,A,C,B,RL),
                  hanoy(N1,C,B,A,RR),
                  app(RL,[A-B|RR],L).

subst(1,E,[_|R],[E|R]):-!.
subst(N,E,[X|L],[X|L2]):-N1 is N-1, subst(N1,E,L,L2).

moves([],_):-!.
moves([A-B|T],L):-
                  elem(A,L,[X|P1]), subst(A,P1,L,L2),
                  elem(B,L,P2), subst(B,[X|P2],L2,L3),
                  writeln(L3),
                  moves(T,L3).

app([],Aq,Aq).
app([Aq|Sw],Dw,[Aq|Cw]):- app(Sw,Dw,Cw).

elem(1,[X|_],X):-!.
elem(N,[_|T],X) :- N1 is N-1, elem(N1,T,X).

make2(0,S,S):-!.
make2(N,T,S):- N1 is N-1, make2(N1,[N|T],S).
                  
test_all:-
   N=10,
   %%% writeln('Count:'), read(N),
   hanoy(N,1,2,3,X),
   %writeln(X),
   make2(N,[],Z),
   Res=[Z,[],[]], writeln(Res),
   moves(X,Res).

