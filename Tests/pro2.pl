pay(1, "cafe1","10:00","Victor","Bogdan", 600, 1).
pay(2, "cafe2","12:00","Victor","Alex", 1600, 1).
pay(3, "cafe3","12:30","Bogdan","Sergey", 700, 2).
pay(4, "cafe4","13:00","Victor","Bogdan", 900, 1).
pay(5, "cafe1","11:00","Victor","Sergey", 300, 2).
pay(6, "cafe2","10:00","Bogdan","Pavel", 150, 1).
pay(7, "cafe1","12:00","Sergey","Pavel", 600, 2).
pay(8, "cafe5","11:00","Victor","Pavel", 300, 1).
pay(9, "cafe5","11:00","Victor","Pavel", 400, 2).
pay(10, "place10","13:34","Pavel", "Alex", 500, 2 ).
pay(11, "place10","13:34","Pavel", "Alex", 500, 2 ).

is_pay_before("Victor", "Bogdan",2 ).
is_pay_before("Victor", "Pavel",2 ).
is_pay_before("Victor", "Sergey",1 ).

is_pay_before("Pavel", "Victor",2 ).
is_pay_before("Pavel", "Bogdan",1 ).
is_pay_before("Pavel", "Alex",2 ).
is_pay_before("Pavel", "Sergey",1 ).


is_pay_before("Bogdan", "Pavel",2 ).
is_pay_before("Bogdan", "Sergey",1 ).
is_pay_before("Bogdan", "Victor",2 ).

is_pay_before("Alex", "Pavel", 2 ).

is_pay_before(X,Y,0).



custom_op_fact(X,Y):- X > Y.


is_sub([],[]).
is_sub([],[X2|L2]).
is_sub([X|L1],[X|L2]) :- is_sub(L1,L2).
is_sub([X1|L1],[X2|L2]) :- is_sub([X1|L1],L2).



frod(100).
frod(6).

add([],R1,R1).

add( [X1|R1], R2, [X1|R3] ):-
  add(R1,R2,R3).


have_frod(Sender):-
    pay( Ref, Place, _Time, Sender, _Reciever, _Sum, _S ),
    frod( Ref )
    
.
have_frod(Reciever):-
    pay( Ref1, Place, _Time, _R, Reciever, _Sum, _S ),
    frod( Ref1 )
    .
    
have_frod(Sender):-
    pay( Ref, Place, _Time, Sender, Reciever , _Sum, _S ),
    pay( Ref1, Place1, _Time1, _Sender1, Reciever , _Sum1, _S1 ),
    frod( Ref1 ).
    

have_frod(Reciever):-
    pay( Ref, Place, _Time, Sender, Reciever, _Sum, _S ),
    pay( Ref1, Place1, _Time1, Sender, _Reciever1 , _Sum1, _S1 ),
    frod( Ref1 ).
    
    
have_frod(Sender):-
    pay( Ref, Place, _Time, Sender, Sender1 , _Sum, _S ),
    pay( Ref1, Place1, _Time1, Sender1, _Reciever , _Sum1, _S1 ),
    frod( Ref1 )
    
.
    

have_frod(Reciever):-
    pay( Ref, Place, _Time, Reciever1, Reciever, _Sum, _S ),
    pay( Ref1, Place1, _Time1, _Sender, Reciever1 , _Sum1, _S1 ),
    frod( Ref1 )
    .          

    

not_pay_before(From, To):-
       is_pay_before(From, To, X ),!,
       X==0
.

    
is_frod(Place, _Time, Sender, Reciever, _Sum, Signed, 1 ):-
	have_frod(Sender),write(1),nl,
	have_frod(Reciever),write(1),nl,
	Signed >0 
.   

is_frod(Place, _Time, Sender, Reciever, _Sum, Signed, 0.5 ):-
	not_pay_before(Sender, Reciever)
.    

    
check(false).

    


black_client("Pavel").

is_public_company("Sergey").


is_typical_place(Sender, Place):-
    pay(_Ref, Place, _Time, Sender, _Reciever, _Sum, _S ).



not_first(Ref):-
	pay(Ac, AcOkpo, ToOkpo, ToAcc, Bank, Sum, Currency, Desc, Type, Ip, Ref),
	pay(Ac, AcOkpo, ToOkpo, ToAcc, Bank, Sum1, Currency1, Desc1, Type1, Ip, Ref1 ),
	Ref \= Ref1.
	
    
    











