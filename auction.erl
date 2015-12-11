-module(auction).

-compile({parse_transform, streams}).

-export([start/0, init/0, stop/0, loop/3]).


start() ->
    Pid = spawn(?MODULE, init, []),
    register(?MODULE, Pid).


stop() ->
    ?MODULE ! stop,
    unregister(?MODULE).


init() ->
    {ok, Pid} = controller:start_link(),
    controller:create_stream(Pid, bid),
    controller:create_stream(Pid, bid_price),

    Query =
        streams:query(
          fun (Bid) ->
                  Price =
                      streams:groupby(
                        fun(P) ->
                                streams:aggregate(aggregate:max(price), P)
                        end,
                        item,
                        Bid),
                  {Price}
          end,
          [bid],
          [bid_price]),

    controller:create_query(Pid, Query),
    {ok, PricePid} = controller:lookup_stream(Pid, bid_price),
    LoggerPid = stream_logger:start_link(),
    PricePid ! {add_consumer, LoggerPid},

    {ok, BidPid} = controller:lookup_stream(Pid, bid),
    ?MODULE:loop(BidPid, 0, 0).


loop(BidPid, NextItem, Count)
  when Count < 5 ->
    Source = fake_bid:start_link(self(), NextItem),
    BidPid ! {add_producer, Source},
    Source ! {add_consumer, BidPid},
    ?MODULE:loop(BidPid, NextItem+1, Count+1);
loop(BidPid, NextItem, Count) ->
    receive
        finished ->
            ?MODULE:loop(BidPid, NextItem, Count-1);
        stop ->
            exit(abort)
    end.
