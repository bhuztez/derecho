-module(stream_logger).

-export(
   [start_link/0,
    loop/0]).


start_link() ->
    spawn_link(?MODULE, loop, []).

loop() ->
    receive
        Event ->
            io:format("~p~n", [Event]),
            ?MODULE:loop()
    end.
