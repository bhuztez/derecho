-module(stream).

-export([start_link/0, init/0, loop/3]).


start_link() ->
    spawn_link(?MODULE, init, []).

init() ->
    ?MODULE:loop(dict:new(), dict:new(), dict:new()).

loop(Buffers, Producers, Consumers) ->
    receive
        {add_producer, Pid} when is_pid(Pid) ->
            case dict:is_key(Pid, Producers) of
                true ->
                    ?MODULE:loop(Buffers, Producers, Consumers);
                false ->
                    ?MODULE:loop(
                       dict:store(Pid, [], Buffers),
                       dict:store(Pid, 0, Producers),
                       Consumers)
            end;
        {delete_producer, Pid} when is_pid(Pid) ->
            ?MODULE:loop(dict:erase(Pid, Buffers), dict:erase(Pid, Producers), Consumers);
        {add_consumer, Pid} when is_pid(Pid) ->
            case dict:is_key(Pid, Consumers) of
                true ->
                    ?MODULE:loop(Buffers, Producers, Consumers);
                false ->
                    ?MODULE:loop(
                       Buffers, Producers,
                       dict:store(Pid, 0, Consumers))
            end;
        {delete_consumer, Pid} when is_pid(Pid) ->
            ?MODULE:loop(Buffers, Producers, dict:erase(Pid, Consumers));
        {From, Seq, Msg} when is_pid(From) ->
            {Seq1, Msgs, Buffer} =
                pop_messages(
                  dict:fetch(From, Producers),
                  ordsets:add_element({Seq, Msg},
                                      dict:fetch(From, Buffers))),

            Consumers1 = dict:from_list(
                           send_messages(Msgs,
                                         dict:to_list(Consumers))),

            ?MODULE:loop(
               dict:store(From, Buffer, Buffers),
               dict:store(From, Seq1, Producers),
               Consumers1)
    end.


pop_messages(Seq, [{Seq, Msg}|Rest]) ->
    {Seq1, Msgs, Buffer} = pop_messages(Seq + 1, Rest),
    {Seq1, [Msg|Msgs], Buffer};
pop_messages(Seq, Buffer) ->
    {Seq, [], Buffer}.


send_messages([], Consumers) ->
    Consumers;
send_messages([Msg|Rest], Consumers) ->
    Consumers1 =
        lists:map(
          fun({Pid, Seq}) ->
                  Pid ! {self(), Seq, Msg},
                  {Pid, Seq + 1}
          end,
          Consumers),
    send_messages(Rest, Consumers1).
