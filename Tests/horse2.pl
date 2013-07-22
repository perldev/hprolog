%%%%% ОБХОД ДОСКИ КОНЕМ


good(X,Y,M):-X>0,Y>0,X=<M,Y=<M.

step(X,Y,X1,Y1):-X1 is X+2, Y1 is Y+1.
step(X,Y,X1,Y1):-X1 is X+2, Y1 is Y-1.
step(X,Y,X1,Y1):-X1 is X-2, Y1 is Y+1.
step(X,Y,X1,Y1):-X1 is X-2, Y1 is Y-1.
step(X,Y,X1,Y1):-X1 is X+1, Y1 is Y+2.
step(X,Y,X1,Y1):-X1 is X+1, Y1 is Y-2.
step(X,Y,X1,Y1):-X1 is X-1, Y1 is Y+2.
step(X,Y,X1,Y1):-X1 is X-1, Y1 is Y-2.


% 
% step(X,Y,X1,Y1):-member([(-2),(-1),1,2],Dx),
%                  member([(-2),(-1),1,2],Dy),
%                  myabs(Dx,ADx),myabs(Dy,ADy), ADx=\=ADy,
%                  X1 is X+Dx, Y1 is Y+Dy.
%                  

find(_,_,_,0,R,R):- true.
find(X,Y,MN,N,R,R2):-
          X_Y=X/Y,
          not(member(R,X_Y)),
          step(X,Y,X1,Y1),
          good(X1,Y1,MN),
          N1 is N-1,
          find(X1,Y1,MN,N1,[X_Y|R],R2).

member([X|_],X):- true.
member([_|L],X):-member(L,X).

elem(0,[],_):-!.
elem(1,[X|_],X):-!.
elem(N,[_|T],X) :- nonvar(N),!,N1 is N-1, elem(N1,T,X).
elem(N,[_|T],X) :- elem(N1,T,X), N is N1+1.

range(X,Y,X):-X=<Y.
range(X,Y,Z):-X<Y, X1 is X+1, range(X1,Y,Z).

printr(S,M):-
           range(1,M,Y),nl,range(1,M,X),
           X/Y=X_Y,elem(I,S,X_Y),
           write(' '),write(I), %%writef('%4R',[I]),
           X=M,fail;!.

goal1:-
       %%%write('enter size='),read(M),
       M=5,
       Ms is M*M,
       find(1,1,M,Ms,[],X),
       writeln(X),
       printr(X,M).
       %%%,fail;true.
       
%test_all :- goal1.



%%%%%%%%%%%%%%%%%%%%%%%%%%% V2 %%%%%%%%%%%%%%%%%%%%%%%

pos(X,Y,P,M):- X1 is X-M/2, Y1 is Y-M/2, P is M*M -(X1*X1+Y1*Y1).

insert(X,[],[X]):- !.
insert(X,[H|T],[H|T1]):-[PX,_]=X , [PH,_]=H ,PX>PH,!, insert(X,T,T1).
insert(X,[H|T],[X,H|T]):- !.

allsteps(MN,X,Y,S) :-
       assert(r([])),
       step(X,Y,X1,Y1),good(X1,Y1,MN),
       retract(r(L)),pos(X1,Y1,P,MN) ,insert([P,X1/Y1],L,L1), assert(r(L1)),false;
       retract(r(S)),!.

finds(X,Y,_,1,R,[X/Y|R]):-!.
finds(X,Y,MN,N,R,R2):-
	  write(R), write("-"),write(R2),nl,
          allsteps(MN,X,Y,All),
          %% setof([P,Xn/Yn],(step(X,Y,Xn,Yn),good(Xn,Yn,MN),pos(Xn,Yn,P,MN)), All), %% это не реализовано
          member(All, [_,X1/Y1]),
          not(member(R, X1/Y1)),
          N1 is N-1,
          write(X1),write("-"),write(Y1),nl,
          finds(X1,Y1,MN,N1,[X/Y|R],R2).

goal2(M):-
       Ms is M*M,
       finds(1,1,M,Ms,[],X),
       printr(X,M).
       
test2 :- goal2(8).
