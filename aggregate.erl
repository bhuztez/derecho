-module(aggregate).

-export([max/1]).

max(Key) ->
    #{init =>
          fun (Tuple) ->
                  maps:find(Key, Tuple)
          end,
      update =>
          fun (Tuple, Max) ->
              case maps:find(Key, Tuple) of
                  {ok, Price} when Price > Max ->
                      Price;
                  _ ->
                      Max
              end
          end,
      final =>
          fun (Price) ->
                  Price
          end
     }.
