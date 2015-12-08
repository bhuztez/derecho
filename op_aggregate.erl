-module(op_aggregate).

-behaviour(gen_operator).

-export(
   [start_link/3,
    init/1,
    handle_tuple/4,
    handle_punct/4]).


start_link(Inputs, Outputs, Parameters) ->
    gen_operator:start_link(Inputs, Outputs, ?MODULE, Parameters).


init(_Parameters) ->
    {ok, none}.


handle_tuple(Tuple, _, none, #{init := Init}) ->
    case Init(Tuple) of
        {ok, State} ->
            {ok, [], {ok, State}};
        _ ->
            {ok, [], none}
    end;
handle_tuple(Tuple, _, {ok, State}, #{update := Update}) ->
    {ok, [], {ok, Update(Tuple, State)}}.


handle_punct(Punct, _, none, _) ->
    {ok, [Punct], none};
handle_punct(true, _, {ok, State}, #{final := Final}) ->
    {ok, [Final(State)], [true], none};
handle_punct(Punct, _, {ok, State}, _) ->
    {ok, [Punct], {ok, State}}.
