:-
test1.


has_large_good_animal(X):- 
	owner(X, Animal),
	large(Animal ),
	good( Animal ).

	
some4(X, [ X1,X,X3| [X3,X,X5] ], X5 ).
	

is_partners(X, Y):-
    pay(Account, X, Y, Account2, T, Sum, Currency, Desc,Type,Tt,Ref).

	
owner(anett,dark).
owner(bogdan,jhon).
owner(bogdan,tom).
owner(tomy,jhon).
owner(tomy,dark).

animal(X):- dog(X) .


dog(tom).
dog(jhon):-!.
dog(dark):-!.
dog(bonny):-!.
 
large_hasowner(X):-  large_dog(X), owner(O, X).

good(jhon).
good(tom).

large(tom).
large(jhon).

large_dog(X):- 
dog(X), large(X).

animals_of(X):- owner(X, Animal).



has_large_animal(X):- owner(X,Animal), large(Animal).


ac(1).
ac(2).
ac(3).

bc(1).
bc(2).
bc(3).

cc(1).
cc(2).
cc(3).



rule_some(X ):-
    Y is 2 + 1, cc(X)
.


dc1(X, Y, Z) :- ac(X), bc(Y), cc(Z).



a(1).
a(2).
a(3).

b(1).
b(2).
b(3).

c(1).
c(2).
c(3).
c(4).
c(5).
c(6).

d(4).
d(5).
d(6).

e(4).
e(5).
e(6).

f(4).
f(5).
f(6).






avarage(X):-
    f(Y),
    avarage([Y], X1),
    count(X1, Count ),
    sum(X1, Sum ),
    X is Sum/Count.
    
count([Head| Tail], Count  ):-
    count(Tail, 1, Count  )
.

count([Head| Tail], Accum, Count  ):-
  NewAccum is   1 + Accum,
  count(Tail, NewAccum, Count  )
.
count([],Accum, Accum).  

    
sum([Head| Tail], Sum  ):-
    sum(Tail, Head, Sum  )
.

sum([Head| Tail], Accum, Sum  ):-
  NewAccum is   Head + Accum,
  sum(Tail, NewAccum, Sum  )
.

sum([],Accum, Accum).


avarage(Y, X):-
    f(Y1),
    not_in( Y, Y1),
    avarage([Y1 | Y], X)
.   
avarage(Y, Y).

    
not_in([Head| Tail],Search ):-
  Head  \= Search,
  not_in(Tail, Search).
  
not_in([],Search).

    
 



is_ari_is(X,Y):- 
	      f(X),
	      c(Y),
	      Y1 is X + Y,
	       Y1 == 10 .
	      
is_ari_is_t(X,Y):- 
	      f(X),
	      c(Y),
	      Y1 is X + Y.
	      
is_ari_is_f(X,Y):- 
	      f(X),
	      c(Y),
	      Y is X + Y.
	      
is_ari_is_f1(X,Y):- 
	      f(X),
	      c(Y),
	      Y is X + Y,
	      e(X)
	      .
	      
is_ari(X,Y):- f(X),
	      c(Y),
	      10 == (X+Y).

parent(pam, bob).
parent(tom, bob).
parent(tom, liz).
parent(bob, ann).
parent(bob, pat).
parent(pat, jim).

predecessor(X, Y) :- parent(X, Y).
predecessor(X, Y) :-
	parent(X, Z),
	predecessor(Z, Y).


male(tom).
male(bob).
male(jim).
male(pam).

mother(X, Y) :-
    female(X),!,
    parent(X, Y).


female(pam).
female(liz).
female(ann).
female(pat).

count_sms(From,To,Text,Service) :- Service =:= [53,50] , 
use_mobile_bank(From,Count) , 
NewCount is Count + 1 ,
retract(use_mobile_bank(From,Count)) , 
assert(use_mobile_bank(From,NewCount)) ; Service =:= [53,50] , assert(use_mobile_bank(From,1)).


pay(1,"100", "UAH", "Bavin", "Andrey", 1000 ).
pay(2,"150", "UAH", "Bavin", "Bogdan" , 1310).
pay(3,"200", "UAH", "Bogdan", "Andrey", 4000 ).
pay(4,"300", "UAH", "Bavin", "Alex", 5002 ).
pay(5,"300", "UAH", "Alex", "Bavin", 5001 ).
pay(6, "300", "UAH", "Alex", "Bogdan", 5002 ).
pay(7,"300", "UAH", "Bavin", "Alex", 5003 ).
pay(8,"300", "UAH", "Alex", "Bavin", 5004 ).
pay(9, "300", "UAH", "Alex", "Bogdan", 5005 ).


prev_pay(From, To, Time1, Time, Ref):-
    pay(Ref, Sum1, Currenc1, From, To, Time ), Time<Time1;
    pay(Ref, Sum1, Currenc1, To, From, Time ), Time<Time1.
 
get_first(From, To, Time1, Time,  Ref):-
      not( prev_pay(From, To, Time1, Time, Ref) )
.

get_first(From, To, Time1, Time,  Ref):-
      prev_pay(From, To, Time1, Time, Ref),!, 
      get_first(From, To, Time, Time2, Ref).


first(Ref, Ref, Time):-
  pay(Ref, Sum1, Currenc1, From, To, Time ),
  not( not_first(From, To, Time ) )
.

first(Ref, Ref1, Time):-
  pay(Ref, Sum1, Currenc1, From, To, StartTime ),
  get_first(From, To, StartTime, Time,  Ref1)
.





  
not_first(From, To, Time ):-
    pay(Ref, Sum1, Currenc1, From, To, Time1 ),
    Time1 < Time;
    pay(Ref, Sum1, Currenc1, To, From, Time1 ),
    Time1 < Time
.

  
  
  
  
  
  
  
  






married(tom, pam).
married(bob, liz).
married(jim, ann).


human(X):-
  female(X);male(X).

divorced(X):-
  female(X),not(married(Y,X)). 

whole_family(X):-
    married(Husband, Wife),
    parent(Husband, X),
    parent(Wife, X).

    
add([],X,X).
add([X|R1],R2,[X|R3] ):-
  add(R1,R2,R3).
    
rule([],X1,X2,X2).

rule(List,X1,X2,X1).

check_not(X):- not( dog(X) ).
    
rule([ X1 , X3 | [ F |  X1 ] ],  X3).   


offspring(X, Y) :- parent(Y, X).






father(X, Y) :-
    parent(X, Y),
    male(X).

grandparent(X, Y) :-
    parent(X, Z),
    parent(Z, Y).


