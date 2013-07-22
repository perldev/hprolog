%Работа со списками


len([],0).
len([_|S],L):-len(S,L1), L is L1+1.

len2([],S,S).
len2([_|A],S,S1):-S2 is S+1, len2(A,S2,S1).

app([],Aq,Aq).
app([Aq|Sw],Dw,[Aq|Cw]):- app(Sw,Dw,Cw).

app2(A,[],A).
app2(D,[A|S],[A|C]):- app2(D,S,C).

sub(S,L):- app(_W,S2,S), app(L,_Q,S2).

rev([],[]).
rev([Ax|L],Sx):- rev(L,S1), app(S1,[Ax],Sx).


del(_,[],[]).
del(A,[A|L],L1):-del(A,L,L1).
del(A,[H|L1],[H|L]):-del(A,L1,L).

make(0,[]):- !.
make(N,S):- N1 is N-1, make(N1,L),app(L,[N],S).

make2(0,S,S):-!.
make2(N,T,S):- N1 is N-1, make2(N1,[N|T],S).


permut([],[]):-!.
permut(X,[H|T]):- app(L1,[H|L2],X), app(L1,L2,T1),permut(T1,T).

sort_perm(X,X1):-permut(X,X1), ordered(X1).

ordered([]):-!.
ordered([_]):-!.
ordered([H,H2|T]):-H=<H2,ordered([H2|T]).
     
insert(X,[],[X]).
insert(X,[H|T],[H|T1]):-X>H, insert(X,T,T1).
insert(X,[H|T],[X,H|T]):-X=<H.

sort_in([],[]).
sort_in([H|T],T2):-sort_in(T,T1),insert(H,T1,T2).

part(X,[],[X],[]).
part(X,[H|T],[H|T1],T2):-X>H, part(X,T,T1,T2).
part(X,[H|T],T1,[H|T2]):-X=<H,part(X,T,T1,T2).

quik([],[]).
quik([A],[A]).
quik([H|T],T2):-part(H,T,L,B),
    quik(L,L1),!,quik(B,L2),!, app(L1,L2,T2).
    
sortbub(L,L2):- %%writeln(loop),
    bub(L,L1),!, sortbub(L1,L2).
sortbub(L,L):-writeln(end).

bub([A,B|L],[B,A|L]):- A>B.
bub([A|L],[A|L2]):-bub(L,L2).

rand_list(0,S,S).
rand_list(N,S,R):- N1 is N-1, random(X),Y is round(X * 100), rand_list(N1,[Y|S], R),!.

%:- make(5,Z),permut(Z,_),fail;true.%, writeln(X)

remdub([],[]).
remdub([H|T],[H|T1]):- del(H,T,T2),!, remdub(T2,T1).
remdub([H|T],[H|T1]):- remdub(T,T1).

flatt([],[]):-!.
flatt(X,[X]):-atomic(X),!.
flatt([X|T],R) :-flatt(X,L1), flatt(T,L2), app(L1,L2,R).


%%%%%%%%%%%%
test1:-
       make2(10,[],X),
       writeln(X),
       len(X,L1), writeln(len=L1),!,
       len2(X,0,L2), writeln(len=L2).

test2:-
       make2(3,[],Xx),
       writeln(Xx),
       sub(Xx,L1), writeln(sub = L1),false;
       true.

test3:-
       make2(7,[],X),
       writeln(X),
       rev(X,X1), writeln(rev=X1),
       sort_in(X1,X2),writeln(sorted=X2),
       sortbub(X1,X3),writeln(sorted=X3),
       quik(X1,X4),writeln(sorted=X4),!,
       !,sort_perm(X1,X5),writeln(sorted=X5).


test_all:-
    test1, writeln('---ok---'),!,
    test2, writeln('---ok---'),!,
    test3, writeln('---ok---'),!.



