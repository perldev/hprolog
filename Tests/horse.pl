%%%%% наунд дняйх йнмел


good(X,Y,M):-X>0,Y>0,X=<M,Y=<M.

step(X,Y,X1,Y1):-X1 is X+2, Y1 is Y+1.
step(X,Y,X1,Y1):-X1 is X+2, Y1 is Y-1.
step(X,Y,X1,Y1):-X1 is X-2, Y1 is Y+1.
step(X,Y,X1,Y1):-X1 is X-2, Y1 is Y-1.
step(X,Y,X1,Y1):-X1 is X+1, Y1 is Y+2.
step(X,Y,X1,Y1):-X1 is X+1, Y1 is Y-2.
step(X,Y,X1,Y1):-X1 is X-1, Y1 is Y+2.
step(X,Y,X1,Y1):-X1 is X-1, Y1 is Y-2.

% myabs(X,Y):- X=<0,!,Y is 0-X.
% myabs(X,X):- X>0.
% 
% step(X,Y,X1,Y1):-memb([(-2),(-1),1,2],Dx),
%                  memb([(-2),(-1),1,2],Dy),
%                  myabs(Dx,ADx),myabs(Dy,ADy), ADx=\=ADy,
%                  X1 is X+Dx, Y1 is Y+Dy.
%                  
not(X):- call(X),!,fail;true.
writeln(X):- write(X),nl.

find(_,_,_,0,R,R).
find(X,Y,MN,N,R,R2):-
          not(memb(R,X/Y)),
          step(X,Y,X1,Y1),
          good(X1,Y1,MN),
          N1 is N-1,
          find(X1,Y1,MN,N1,[X/Y|R],R2).

memb([X|_],X).
memb([_|L],X):-memb(L,X).

elem(0,[],_):-!.
elem(1,[X|_],X):-!.
elem(N,[_|T],X) :- nonvar(N),!,N1 is N-1, elem(N1,T,X).
elem(N,[_|T],X) :- elem(N1,T,X), N is N1+1.

range(X,Y,X):-X=<Y.
range(X,Y,Z):-X<Y, X1 is X+1, range(X1,Y,Z).

printr(S,M):-
           range(1,M,Y),nl,range(1,M,X),
           elem(I,S,X/Y), write(' '),write(I), %%writef('%4R',[I]),
           X=M,fail;!,true.

goal1:-
       %%%write('enter size='),read(M),
       M=5,
       Ms is M*M,
       find(1,1,M,Ms,[],X),
       writeln(X),
       printr(X,M).
       %%%,fail;true.
       
test_all :- goal1.


%%%%%%%%%%%%%%%%%%%%%%%%%%% V2 %%%%%%%%%%%%%%%%%%%%%%%
% pos(X,Y,P,M):- P is M - (abs(X-M/2)^2+abs(Y-M/2)^2).
% 
% finds(_,_,_,0,R,R).
% finds(X,Y,MN,N,R,R2):-
%           not(memb(X/Y,R)),
%           setof(P-Xn/Yn,(step(X,Y,Xn,Yn),good(Xn,Yn,MN),pos(Xn,Yn,P,MN)), All),
%           memb(_-X1/Y1, All),
%           N1 is N-1,
%           finds(X1,Y1,MN,N1,[X/Y|R],R2).
% 
% 
% 
% 
% 
% goal(M):-
%  Ms is M*M,
%  range(1,M,Y1),nl,range(1,M,X1),
%  find(X1,Y1,M,Ms,[],X), %print(X),
%  printr(X,M),
%  nl,
%  %%%get_char(' ');
%  writeln('---end').
% 


%findall(X, find(X1,Y1,1,[],X), S),length(S,N),writef('x=%t y=%t N=%t\n',[X1,Y1,N]),

