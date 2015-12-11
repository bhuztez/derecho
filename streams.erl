-module(streams).

-export([parse_transform/2]).


parse_transform(Forms, _Options) ->
    Forms1 = (catch [form(Form) || Form <- Forms]),

    case is_list(Forms1) of
        true ->
            io:format(
              "~s~n",
              [erl_prettypr:format(erl_syntax:form_list(Forms1))]);
        false ->
            io:format("~p~n", [Forms1]),
            exit(crash)
    end,
    Forms1.

form({function, Line, Name, Arity, Clauses}) ->
    {function, Line, Name, Arity, [clause(Clause) || Clause <- Clauses]};
form(Form) ->
    Form.

clause({clause, Line, Heads, Guards, Body}) ->
    {clause, Line, Heads,
     [[expr(Expr) || Expr <- Guard] || Guard <- Guards],
     [expr(Expr) || Expr <- Body]}.

expr({nil, _}=Nil) ->
    Nil;
expr({var, _, _}=Var) ->
    Var;
expr({atom, _, _}=Atom) ->
    Atom;
expr({integer, _, _}=Int) ->
    Int;
expr({string, _, _}=Str) ->
    Str;
expr({char, _, _}=Char) ->
    Char;
expr({tuple, Line, Elems}) ->
    {'tuple', Line, [expr(Elem) || Elem <- Elems]};
expr({cons, Line, Head, Tail}) ->
    {cons, Line, expr(Head), expr(Tail)};
expr({match, Line, Pattern, Expr}) ->
    {match, Line, Pattern, expr(Expr)};
expr({'case', Line, Expr, Clauses}) ->
    {'case', Line, expr(Expr), [clause(Clause) || Clause <- Clauses]};
expr({'if', Line, Clauses}) ->
    {'if', Line, [clause(Clause) || Clause <- Clauses]};
expr({'receive', Line, Clauses}) ->
    {'receive', Line, [clause(Clause) || Clause <- Clauses]};
expr({'receive', Line, Clauses, Timeout, Exprs}) ->
    {'receive', Line, [clause(Clause) || Clause <- Clauses],
     expr(Timeout), [expr(Expr) || Expr <- Exprs]};
expr({'try', Line, Exprs, Clauses, Exceptions, Body}) ->
    {'try', Line, [expr(Expr) || Expr <- Exprs],
     [clause(Clause) || Clause <- Clauses],
     [clause(Clause) || Clause <- Exceptions],
     [expr(Expr) || Expr <- Body]};
expr({named_fun, Line, Name, Clauses}) ->
    {named_fun, Line, Name, [clause(Clause) || Clause <- Clauses]};
expr({op, Line, Op, Left, Right}) ->
    {op, Line, Op, expr(Left), expr(Right)};
expr({'fun', Line, {clauses, Clauses}}) ->
    {'fun', Line, {clauses, [clause(Clause) || Clause <- Clauses]}};
expr({'fun', _, _} = Fun) ->
    Fun;
expr({'call', _,
      {remote, _, {atom, _, streams}, {atom, _, query}},
      [Fun, Inputs, Outputs]}) ->
    compile_query(Fun, Inputs, Outputs);
expr({'call', Line, Fun, Args}) ->
    {'call', Line, Fun, [expr(Arg) || Arg <- Args]};
expr({bin, Line, Elements}) ->
    {bin, Line, [expr(Elem) || Elem <- Elements]};
expr({bin_element, Line, Expr, Size, Spec}) ->
    {bin_element, Line, expr(Expr), Size, Spec};
expr({lc, Line, Expr, Qualifiers}) ->
    {lc, Line, expr(Expr), [generate(Q) || Q <- Qualifiers]};
expr({bc, Line, Expr, Qualifiers}) ->
    {bc, Line, expr(Expr), [bgenerate(Q) || Q <- Qualifiers]}.

generate({generate, Line, Pattern, Expr}) ->
    {generate, Line, expr(Pattern), expr(Expr)};
generate(Expr) ->
    expr(Expr).

bgenerate({b_generate, Line, Pattern, Expr}) ->
    {b_generate, Line, expr(Pattern), expr(Expr)};
bgenerate(Expr) ->
    expr(Expr).


convert_list({nil, _}) ->
    [];
convert_list({cons, _, H, T}) ->
    [H|convert_list(T)].


make_list([]) ->
    {nil, 1};
make_list([H|T]) ->
    {cons, 1, H, make_list(T)}.



compile_query(Fun, Inputs, Outputs) ->
    Inputs1 = convert_list(Inputs),
    Outputs1 = convert_list(Outputs),
    Count = length(Inputs1),
    {Output, Queries, NextStream, _} =
        flatten_subquery(Fun, lists:seq(1, Count), Count+1, dict:new()),

    InQueries =
        [{tuple, L, [{integer,L,N}, {atom,L,input}, S]}
         || {S={_,L,_},N} <- lists:zip(Inputs1, lists:seq(1, Count))],

    Outputs2 =
        case is_integer(Output) of
            true ->
                [Output];
            false ->
                Output
        end,

    OutQueries =
        [{tuple, L, [{integer,L,Ou}, {atom,L,output}, S, make_list([{integer,L,In}])]}
         || {In, S={_,L,_}, Ou} <- lists:zip3(Outputs2, Outputs1, lists:seq(NextStream, NextStream+length(Outputs1)-1))],

    Queries1 = InQueries ++ Queries ++ OutQueries,
    make_list(Queries1).



bind_vars([], [], Streams) ->
    Streams;
bind_vars([{var, _, Var}|Vars], [N|Inputs], Streams) ->
    bind_vars(Vars, Inputs, dict:store(Var, N, Streams)).

resolve_vars([], _Streams) ->
    [];
resolve_vars([{var, _, Var}|Vars], Streams) ->
    [dict:fetch(Var, Streams)|resolve_vars(Vars, Streams)].


match_var({var, _, Var}, Output, Streams) when is_integer(Output) ->
    dict:store(Var, Output, Streams);
match_var({tuple, _, Vars}, Output, Streams) ->
    bind_vars(Vars, Output, Streams).


flatten_subquery(
  {'fun', _,
   {clauses, [{clause, _, Vars, [], Exprs}]}},
  Inputs, NextStream, Streams) ->
    Streams1 = bind_vars(Vars, Inputs, Streams),
    flatten_queries(Exprs, NextStream, Streams1).


flatten_queries([{tuple, _, Vars}], NextStream, Streams) ->
    {resolve_vars(Vars, Streams), [], NextStream, Streams};
flatten_queries([Query], NextStream, Streams) ->
    flatten_query(Query, NextStream, Streams);
flatten_queries([Query|Rest], NextStream, Streams) ->
    {_, Queries, NextStream1, Streams1} = flatten_query(Query, NextStream, Streams),
    {Output, Queries1, NextStream2, Streams2} = flatten_queries(Rest, NextStream1, Streams1),
    {Output, Queries ++ Queries1, NextStream2, Streams2}.


flatten_query({var, _, Var}, NextStream, Streams) ->
    {dict:fetch(Var, Streams), [], NextStream, Streams};
flatten_query({match, _, Pattern, Query}, NextStream, Streams) ->
    {Output, Queries, NextStream1, Streams1} =
        flatten_query(Query, NextStream, Streams),
    Streams2 = match_var(Pattern, Output, Streams1),
    {Output, Queries, NextStream1, Streams2};
flatten_query(
  {'call', _,
   {remote, _, {atom, _, streams}, {atom, Line, groupby}},
   [Fun, Key, Input]}, NextStream, Streams) ->
    {Stream, Queries, NextStream1, _} = flatten_query(Input, NextStream, Streams),
    true = is_integer(Stream),
    {Output, Queries1, NextStream2, Streams2} = flatten_subquery(Fun, [Stream], NextStream1, dict:new()),
    Queries2 =
        Queries ++
        [{tuple, L, [N, Op, {atom,Line,op_groupby}, In, {tuple, L, [Key, Mod, Params]}]}
         || {tuple, L, [N, Op, Mod, In, Params]} <- Queries1 ],
    {Output, Queries2, NextStream2, Streams2};
flatten_query(
  {'call', _,
   {remote, _, {atom, _, streams}, {atom, Line, aggregate}},
   [Fun, Input]}, NextStream, Streams) ->
    {Stream, Queries, NextStream1, Streams1} = flatten_query(Input, NextStream, Streams),
    true = is_integer(Stream),
    Queries1 =
        Queries ++
        [{tuple, Line, [{integer,Line,NextStream1}, {atom,Line,operator}, {atom,Line,op_aggregate}, make_list([{integer,Line,Stream}]), Fun]}],
    {NextStream1, Queries1, NextStream1 + 1, Streams1}.
