% Представление деревьев

add(X,nil,t(nil,X,nil)).
add(X,T,T):-T=t(_,X,_).
add(X,t(L,Y,R),t(L1,Y,R)):-
    X<Y, add(X,L,L1).
add(X,t(L,Y,R),t(L,Y,R1)):-
    X>=Y, add(X,R,R1).

add2(X, nil, t(nil,X,nil)).
add2(X, t(L,Y,R), t(t(L,Y,R1), X, R2 )):- X>=Y,!,
        add2(X, R, t(R1, X, R2)),!.
add2(X, t(L,Y,R), t(L1, X, t(L2,Y,R) )):- X<Y,!,
        add2(X, L, t(L1, X, L2)).
add2(X, t(L,Y,R), t(L1,Y,R)):- X<Y,  add2(X,L,L1).
add2(X, t(L,Y,R), t(L,Y,R1)):- X>=Y, add2(X,R,R1).


del(X, t(nil,X,R),R).
del(X, t(L,X,nil),L).
del(X, t(L,X,R),t(L,Y,R1)):- delleft(R,Y,R1).
del(X, t(L,Y,R),t(L,Y,R1)):- X>Y, del(X,R,R1).
del(X, t(L,Y,R),t(L1,Y,R)):- X<Y, del(X,L,L1).

delleft(t(nil,Y,R),Y,R).
delleft(t(L,Y,R),X,t(L1,Y,R)):-delleft(L,X,L1).

tab_(0).
tab_(N):-write(' '), N1 is N-1, tab_(N1).

out(nil,_).
out(t(L,X,R),N):-
   N1 is N+2,
   out(R,N1),
   tab_(N), writeln(X),
   out(L,N1).

% menu:-
%       retractall(one(_)), assert(one(nil)),!,
%       repeat, retract(one(T)),
%       write('T= '), writeln(T),out(T,0),
%       write('add:'),read(X),
%       add(X,T,T1),
%       %%add2(X,T,T1),
%       assert(one(T1)),
%       writeln('еще?'),get_char('n').
%       
      
%%%%%%%%%%%%%%%%%%%%%%%
puts([],T,T):-!.
puts([X|L],T,Tr) :- add(X,T,T1), puts(L,T1,Tr).

puts2([],T,T):-!.
puts2([X|L],T,Tr) :- add2(X,T,T1), puts2(L,T1,Tr).

test1:-
       writeln('put to tree:'),
       puts([5,2,2.5,4,3,1,6,10,9],nil,Tree),
       writeln(Tree),
       out(Tree,0),
       writeln('dell from tree:'),
       del(9,Tree,Tree1),
       out(Tree1,0),
       writeln('------'),
       del(2,Tree1,Tree2),
       out(Tree2,0).

test2:-
       writeln('put to tree:'),
       puts2([5,2,2.5,4,3,1,6,10,9],nil,Tree),
       writeln(Tree),
       out(Tree,0),
       writeln('dell from tree:'),
       add2(9,Tree1,Tree),
       out(Tree1,0),
       writeln('------'),
       add(2,Tree2,Tree1),
       out(Tree2,0).

test_all:- test1,  writeln('---ok---'),!,
           test2,  writeln('---ok---'),!.
           
           