-module(gen_operator).

-export([start_link/4, init/4, loop/6]).


start_link(Inputs, Outputs, Module, Params) ->
    spawn_link(?MODULE, init, [Inputs, Outputs, Module, Params]).


init(Inputs, Outputs, Mod, Params) ->
    case Mod:init(Params) of
        {ok, State} ->
            ?MODULE:loop(
               operators:init(Inputs, Outputs),
               queues:init(Inputs),
               maps:from_list([{Id, 0} || Id <- Outputs]),
               Mod,
               Params,
               State);
        {error, Reason} ->
            exit(Reason)
    end.


loop(Operators, Queues, Seqs, Mod, Params, State) ->
    receive
        {operator_location, Id, Pid} when is_pid(Pid) ->
            ?MODULE:loop(operators:update_location(Id, Pid, Operators),
                         Queues, Seqs, Mod, Params, State);
        {From, Seq, Msg} when is_pid(From) ->
            case operators:lookup_input_id(From, Operators) of
                none ->
                    ?MODULE:loop(Operators, Queues, Seqs, Mod, Params, State);
                Id ->
                    {Msgs, Queues1} = queues:add(Id, Seq, Msg, Queues),
                    {Msgs1, State1} = handle_messages(Msgs, Id, Mod, Params, State),
                    Seqs1 = maps:from_list(send_messages(Msgs1, maps:to_list(Seqs), Operators)),
                    ?MODULE:loop(Operators, Queues1, Seqs1, Mod, Params, State1)
            end
    end.


handle_messages([], _, _, _, State) ->
    {[], State};
handle_messages([{'$tuple', Timestamp, Tuple}|Rest], From, Mod, Params, State) ->
    Msgs =
        case Mod:handle_tuple(Tuple, From, State, Params) of
            {ok, Tuples, State1} ->
                build_messages(Timestamp, Tuples, []);
            {ok, Tuples, Puncts, State1} ->
                build_messages(Timestamp, Tuples, Puncts)
        end,
    {Msgs1, State2} = handle_messages(Rest, From, Mod, Params, State1),
    {Msgs ++ Msgs1, State2};
handle_messages([{'$punct', Timestamp, Punct}|Rest], From, Mod, Params, State) ->
    Msgs =
        case Mod:handle_punct(Punct, From, State, Params) of
            {ok, Puncts, State1} ->
                build_messages(Timestamp, [], Puncts);
            {ok, Tuples, Puncts, State1} ->
                build_messages(Timestamp, Tuples, Puncts)
        end,
    {Msgs1, State2} = handle_messages(Rest, From, Mod, Params, State1),
    {Msgs ++ Msgs1, State2}.


build_messages(Timestamp, Tuples, Puncts) ->
    [{'$tuple', Timestamp, T} || T <- Tuples]
        ++ [{'$punct', Timestamp, P} || P <- Puncts].


send_messages([], Seqs, _) ->
    Seqs;
send_messages([Msg|Rest], Seqs, Operators) ->
    Seqs1 =
        lists:map(
          fun({Id, Seq}) ->
                  case operators:lookup_output_pid(Id, Operators) of
                      none ->
                          {Id, Seq};
                      Pid ->
                          Pid ! {self(), Seq, Msg},
                          {Id, Seq + 1}
                  end
          end,
          Seqs),
    send_messages(Rest, Seqs1, Operators).
