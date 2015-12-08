-module(op_groupby).

-behaviour(gen_operator).

-export(
   [start_link/3,
    init/1,
    handle_tuple/4,
    handle_punct/4]).


start_link(Inputs, Outputs, Parameters) ->
    gen_operator:start_link(Inputs, Outputs, ?MODULE, Parameters).


init(_Parameters) ->
    {ok, dict:new()}.


handle_tuple(Tuple, From, State, {Key, Mod, Params}) ->
    case maps:find(Key, Tuple) of
        error ->
            {ok, [], State};
        {ok, Value} ->
            case dict:find(Value, State) of
                error ->
                    case Mod:handle_tuple(Tuple, From, none, Params) of
                        {ok, Msgs, none} ->
                            {ok, Msgs, State};
                        {ok, Msgs, Substate} ->
                            {ok, Msgs, dict:store(Value, Substate, State)}
                    end;
                {ok, Substate} ->
                    {ok, Msgs, Substate1} = Mod:handle_tuple(Tuple, From, Substate, Params),
                    {ok, Msgs, dict:store(Value, Substate1, State)}
            end
    end.


handle_punct({'=:=', Key, Value}=Punct, From, State, {Key, Mod, Params}) ->
    case dict:find(Value, State) of
        error ->
            {ok, [Punct], State};
        {ok, Substate} ->
            case Mod:handle_punct(true, From, Substate, Params) of
                {ok, Msgs, _, none} ->
                    {ok, Msgs, [Punct], dict:erase(Value, State)};
                {ok, _, Substate} ->
                    {ok, [Punct], State}
            end
    end.
