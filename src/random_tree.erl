-module(random_tree).


-export([entropy/1, uniquecounts/1, buildtree/5, buildtree/4, 
        search_best/5, has_attr/2, devide/2, test1/1, check_tree/3, check/2]).

-record(learn_record, 
        {
             attr,
             result   
        }
).



test1(File)->
        Result = csv:parse_file(File,";"),
%         ListSplit = [<<" ">>,<<",">>,<<".">>,
%                      <<"-">>,<<"!">>,<<"?">>,
%                      ],
        Rows = lists:map(fun process_csv/1  ,Result),
        Tree =  buildtree(Rows, 0, 100, 1),
        Tree        
.

check(Tree, Text)->
        Attrs = tree_tokens(Text),
        io:format("~p ~n",[Attrs]),
        check_tree("",Tree, Attrs)
.

check_tree(Prefix, false, _Text)->
        false
;
check_tree(Prefix, true, _Text)->
        true
;
check_tree(Prefix, Tree, Attrs)->
      {Call, True, False} = Tree,
      
%      io:format("~p ~n",[Attrs]),
      case   has_attr(Call, Attrs) of
             true ->
                io:format("~s ~ts  true ~n",[Prefix, Call]),
                check_tree([$-|Prefix], True, Attrs);        
             false ->
                io:format("~s ~ts  false ~n",[Prefix, Call]),
                check_tree([$-|Prefix], False, Attrs)
      end.

% tree_tokens(Text)->
%         soundex_rus:start(Text).

tree_tokens(Text)->
        soundex_rus:start(Text).

process_csv([[First], [Second]])->
        Attrs =  tree_tokens(unicode:characters_to_list(Second) ) ,
         #learn_record{result = binary_to_integer(First), attr = Attrs};
process_csv([[First], Second])->
        Attrs =  tree_tokens(unicode:characters_to_list(Second) ) ,
         #learn_record{result = binary_to_integer(First), attr = Attrs}.         

my_log(E)->
        math:log(E) / math:log(2).
        
  
entropy(Rows)->       
         {Yes, No} =  uniquecounts( Rows ),
         Size = length(Rows),
         P1 = Yes/Size,
         P2 = No/Size,
         case {Yes, No}  of
                {0, _}->  0 -  P2*my_log(P2);
                {_, 0} -> 0 -  P1*my_log(P1);
                 _ ->  0 - P2*my_log(P2) - P1*my_log(P1)
         end
.      
       
uniquecounts(Rows )->
          lists:foldl( fun(Record, {Yes, No})->
                       case Record#learn_record.result of
                                1 -> {Yes + 1, No};
                                _ -> {Yes, No + 1}
                        end
                     end,{0,0}, Rows
                   )         
.

to_record( [ First, Second  ] )->
         Attrs =  tree_tokens( Second ) ,
         #learn_record{result = First, attr = Attrs}.


buildtree( Rows, Index, MaxDeep, UniqId )->
        Records = lists:map(fun to_record/1, Rows),
        Cols = get_word_facts_freq(Records),
        Tree =  buildtree(Cols, Records, Index, MaxDeep, true ),
        %%TODO  you can avoid this using variaty of index
        RuleName =  list_to_atom("random_tree_check"  ++ UniqId),
        {':-', { RuleName,{'Attrs'} }, Tree}.
        

buildtree(_Cols, Rows, MaxDeep, MaxDeep, true)->
      {Yes, No} =  uniquecounts(Rows),
      case  Yes > No of
          true -> true;
          false -> false
      end
;
buildtree(_Cols, Rows, MaxDeep, MaxDeep, false)->
      {Yes, No} =  uniquecounts(Rows),
      case Yes > No of
          true -> false;
          false-> true
      end
;
buildtree(Cols, Rows, CurrentDeep, MaxDeep, Res)->
           Entropy = case catch  entropy(Rows) of
                      {'EXIT', T}->
                        throw({exception,Rows, T});
                      E -> E
                     end,
           case Entropy < 0.0001 of
               true -> Res;
               _ ->     Size = length(Rows),
                        io:format(" begin split ~p ~n ",[{Entropy,  Size, MaxDeep, length(Cols) }]),
                        {Col, Set1, Set2, NewCols } =  search_best(Cols, Size, Entropy, Rows, 0),
                        io:format("  Leap  for ~p is existed spams ~p, not spam ~p ~n ",[Col, length(Set1), length(Set2) ]),           
                        case  {Set1, Set2} of
                                {[], _} ->  { Col, true, false};
                                {_, []} ->  { Col, true, false};     
                                _ ->
                                        io:format("~p ~n",[{Col, CurrentDeep, Entropy}]),
                                        True = buildtree(NewCols, Set1, CurrentDeep + 1, MaxDeep, true),
                                        False = buildtree(NewCols, Set2, CurrentDeep + 1, MaxDeep, false),
                                        CallFunction = call_function_tree_has(Col),
                                        {';', { ',', CallFunction, True }, False } 
                        end
           end
.

call_function_tree_has(Col)->
       %% its prolog tree call(random_tree:has_attr(Elem, Attrs), Result)
       { call, {':',  random_tree, { has_attr,  Col, {'Attrs'} } }, {'Result'} }.
        
search_best(Cols, Size,  Entropy,  Rows,  Best )->
        search_best(Cols, Size,  Entropy,  Rows, {}, Best, Cols )
.

search_best([], _Size,  _Entropy,  _Rows, Current, _Best, Cols )->
   {Head, Set1, Set2} = Current,
   NewList =  lists:delete(Head, Cols),
   {Head, Set1, Set2, NewList}
;
search_best([Head| Attrs], Size,  Entropy,  Rows, Current, Best, Cols )->
       {Set1, Set2} = devide(Rows, Head),        
       Gain = calc_gain(Entropy, Size,  Set1, Set2 ),
       
%        io:format("  gain  is ~p ~n ",[{Gain,Head}]),
       case Gain > Best of
            true ->  
                 search_best(Attrs, Size,  Gain,  Rows, {Head, Set1, Set2}, Gain, Cols  );
            false -> search_best(Attrs, Size,  Entropy,  Rows, Current, Best, Cols )      
       end
.
% random_tree:check_tree(Class, "Как говорят в народе \"знание это свет, а не знание - тьма\" и благодаря тайных действий наших политиков, мы все, к сожалению, всё время находимся в тьме. Прискорбно, что от их ничего не скроешь  вот: http://sorv.in/=1f8  здесь публично выложена информация о нас, о наших близких, друзьях ,проще говоря о всех. Можете воспользоваться этим сервисом в своих целях но и  есть возможность скрыть свою страницу из публичного доступа.").

calc_gain(_Entropy, _Size,  [], _Set2 )->
       -10000 %%%big number is not existed
;
calc_gain(_Entropy, _Size,  _Set1, [] )->
       10000 %%%big number if attr existed
;
calc_gain(Entropy, Size,  Set1, Set2 )->
       P = length(Set1)/Size,
       Gain  = Entropy - P*entropy(Set1)-(1-P)*entropy(Set2)
.


devide(Rows, Head)->
       lists:foldl(    fun(A, {Yes, No } ) -> 
                                List = A#learn_record.attr,
                                case has_attr(Head, List) of
                                     true  ->   { [A|Yes], No };
                                     false ->   { Yes, [A|No] }
                                end
                       end,{[], []} ,Rows).
                       
          
-spec has_attr(list(), list())-> true|false.          

has_attr(_Head,[])->
        false;
has_attr(Head,[Head|_Tail])->
        true;
has_attr(Head,[_|Tail])->
        has_attr(Head, Tail).
        

% for COL in Cols:
% #      if    Cols[COL] < 100:
% #       print "%s is too small freq %i " % (COL,Cols[COL] )
% #       continue
% 
%      # print "process %s -%i " % (COL, i+1)
%       (set1,set2) = divideset(rows, COL)
%       size1 = len(set1)
%       size2 = len(set2)
%       # print "making deviding on %i and %i record" % ( size1, size2 )
% #      if size1/size2 < 0.0001 or size2/size1 < 0.0001: 
% #            continue
%       p = float( len(set1) )/len(rows)     
%       gain = current_score - p*scoref(set1, "result")-(1-p)*scoref(set2, "result")
%       if gain>best_gain and len(set1)>0 and len(set2)>0:
%           best_gain=gain
%           best_criteria = COL
%           best_sets = (set1,set2)




get_word_facts_freq(Rows)->
        lists:foldl( fun(Item, List)->
%                       case Item#learn_record.result of                  
%                            1->     
                                Attrs = Item#learn_record.attr,
                                lists:foldl(fun(E, Accum)->
                                               case has_attr(E, Accum) of
                                                  false-> [E|Accum];
                                                  true->Accum
                                                end
                                            end, List, Attrs)
%                            0->
%                                 List
%                       end
                    end,
                    [],
                    Rows    
                 )
.
       
       
% def buildtree(rows, max_deep =0, current_deep = 0, scoref = entropy):
%   print "current deep %i and max %i " % (current_deep, max_deep)
%   if max_deep != 0 and  current_deep == max_deep: # for max 
%       return decisionnode(results = uniquecounts(rows, "result"))
%   
%   if len(rows)==0: return decisionnode( )
%   
%   
%   current_score = scoref(rows, "result")
%   
%   best_gain=0.0
%   best_criteria = None
%   best_sets= None
%   
%   Cols = get_word_facts_freq(rows)
%   i = 0  
%   SIZE = len(rows)
%   for COL in Cols:
% #      if    Cols[COL] < 100:
% #       print "%s is too small freq %i " % (COL,Cols[COL] )
% #       continue
% 
%      # print "process %s -%i " % (COL, i+1)
%       (set1,set2) = divideset(rows, COL)
%       size1 = len(set1)
%       size2 = len(set2)
%       # print "making deviding on %i and %i record" % ( size1, size2 )
% #      if size1/size2 < 0.0001 or size2/size1 < 0.0001: 
% #            continue
%       p = float( len(set1) )/len(rows)     
%       gain = current_score - p*scoref(set1, "result")-(1-p)*scoref(set2, "result")
%       if gain>best_gain and len(set1)>0 and len(set2)>0:
%           best_gain=gain
%           best_criteria = COL
%           best_sets = (set1,set2)
%           
%   if best_gain>0:
%     
%       trueBranch = buildtree(best_sets[0], max_deep, current_deep + 1)
%       falseBranch = buildtree(best_sets[1], max_deep, current_deep + 1)      
%       ret = decisionnode(col = best_criteria, tb = trueBranch, fb = falseBranch)
%       
%       return ret
%   else:
%       ret = decisionnode(results = uniquecounts(rows, "result"))
%       return ret
       
       
% def divideset(rows, value):
%     split_function = lambda row: row.has_key(value) 
%       
%     set1=[row for row in rows if split_function(row)]
%     
%     set2=[row for row in rows if not split_function(row)]
%     
%     return (set1,set2)         
       
% uniquecounts(rows, col = 0)->
%   
%   results={}
%   
%   for row in rows:
%     r = row[col] 
%     if r not in results: 
%       results[r] = 0
%     results[r] += 1
%     
%   return results    
       


% def entropy(rows, col = 0):
%   log2 = lambda x:log(x)/log(2)
%   results = uniquecounts(rows, col)
%   ent = 0.0
%   for r in results.keys( ):
%     p = float(results[r])/len(rows)
%     ent = ent-p*log2(p)
%   return ent