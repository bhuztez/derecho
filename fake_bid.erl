-module(fake_bid).

-export([start_link/2, init/2, loop/4]).


start_link(Parent, Item) ->
    spawn_link(?MODULE, init, [Parent, Item]).


init(Parent, Item) ->
    Output =
        receive
            {add_consumer, Pid} ->
                Pid
        end,
    {A,B,C} = os:timestamp(),
    random:seed(A,B,C),

    timer:send_after(5000+random:uniform(5000), finish),
    ?MODULE:loop(Parent, Item, Output, 0).


loop(Parent, Item, Output, Seq) ->
    Timeout = 50 + random:uniform(500),
    receive
        finish ->
            finish(Parent, Item, Output, Seq)
    after Timeout ->
            Bid = #{item=>Item, price=>random:uniform(100)},
            %% io:format("bid: ~p~n", [Bid]),
            Output ! {self(), Seq, {'$tuple', timestamp(), Bid}},
            ?MODULE:loop(Parent, Item, Output, Seq+1)
    end.


finish(Parent, Item, Output, Seq) ->
    Output ! {self(), Seq, {'$punct', timestamp(), {'=:=', item, Item}}},
    io:format("bid ~p: FINISHED~n", [Item]),
    Parent ! finished.


timestamp() ->
    {Mega, S, Micro} = os:timestamp(),
    (Mega * 1000000 + S) * 1000000 + Micro.
