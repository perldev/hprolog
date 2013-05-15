% Представление графов, 
% поиск пути 

%%%:-dynamic link/3.

link(a,b,5).
link(b,s,3).
%link(s,s,4).
link(s,d,5).
link(b,c,1).
link(c,d,4).
link(a,d,77).
link(d,x,77).

path0(A,A,[]).
path0(A,B,[A-C|L]):-link(A,C,_), path0(C,B,L).

path(A,B,[A-B]):-link(A,B,_).
path(A,B,[A-C|L]):-link(A,C,_), path(C,B,L).

path2(A,A,S,[A|S]):-!.
path2(A,B,S,L):-link(A,C,_), not(memb(A,S)),path2(C,B,[A|S],L).

path2(A,A,S,Cst,p(Cst,[A|S])):-!.
path2(A,B,S,Cst,L):-link(A,C,Val), Cst1 is Cst+Val, not(memb(A,S)),path2(C,B,[A|S],Cst1,L).

minpath(A,B,R):- path2(A,B,[],0,R),R=p(Cost,_), not((path2(A,B,[],0,p(Cost2,_)), Cost2<Cost )).

% pathc(A,A,S,[A|S],V,V).
% pathc(A,B,S,Sr,V,Vr):-
%    setof(X-Y,link(Y,B,X),L), memb(C1-B1,L),
%    not(memb(B1,S)),
%    V1 is V+C1, pathc(A,B1,[B|S],Sr,V1,Vr).


memb(A,[A|_]).
memb(A,[_|T]):-memb(A,T).




%%%%%%%%%%%%%%
test1 :- writeln('path1:'),
         path0(a,d,A),writeln(A),fail;
         writeln('path2:'),
         path(a,d,A1),writeln(A1),fail;
         writeln('acircle path3:'),
         assert(link(s,s,4)),
         path2(a,d,[],A2),writeln(A2),fail;
         retract(link(s,s,4)),
         true.
         
test2:- writeln('cost path:'),
        path2(a,d,[],0,A),writeln(A),fail;
        writeln('min path:'),
        minpath(a,d,X),writeln(X).
         
test_all:- test1,  writeln('---ok---'),!,
           test2,  writeln('---ok---'),!.
           
           