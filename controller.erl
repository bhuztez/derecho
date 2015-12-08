-module(controller).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([create_stream/2, lookup_stream/2, create_query/2]).

create_stream(Pid, Name) ->
    gen_server:call(Pid, {create_stream, Name}).

lookup_stream(Pid, Name) ->
    gen_server:call(Pid, {lookup_stream, Name}).

create_query(Pid, Query) ->
    gen_server:call(Pid, {create_query, Query}).


start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, dict:new()}.

handle_call({create_stream, Name}, _From, State) ->
    case dict:is_key(Name, State) of
        true ->
            {reply, already_exist, State};
        false ->
            Pid = stream:start_link(),
            {reply, ok, dict:store(Name, Pid, State)}
    end;
handle_call({lookup_stream, Name}, _From, State) ->
    {reply, dict:find(Name, State), State};
handle_call({create_query, Query}, _From, State) ->
    Outputs = compile_query(Query),
    start_query(Query, Outputs, State),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


compile_query([], Outputs) ->
    Outputs;
compile_query([{_, input, _}|Rest], Outputs) ->
    compile_query(Rest, Outputs);
compile_query([{Id, operator, _, Sources, _}|Rest], Outputs) ->
    compile_query(
      Rest,
      lists:foldl(
        fun (Source, Acc) ->
                dict:append(Source, Id, Acc)
        end,
        Outputs, Sources));
compile_query([{Id, output, _, Sources}|Rest], Outputs) ->
    compile_query(
      Rest,
      lists:foldl(
        fun (Source, Acc) ->
                dict:append(Source, Id, Acc)
        end,
        Outputs, Sources)).

compile_query(Query) ->
    compile_query(Query, dict:new()).


start_query(Query, Outputs, Streams) ->
    Pids =
        lists:foldl(
          fun ({Id, input, Stream}, Acc) ->
                  dict:store(Id, dict:fetch(Stream, Streams), Acc);
              ({Id, operator, Mod, Inputs, Params}, Acc) ->
                  dict:store(Id, Mod:start_link(Inputs, dict:fetch(Id, Outputs), Params), Acc);
              ({Id, output, Stream, _}, Acc) ->
                  dict:store(Id, dict:fetch(Stream, Streams), Acc)
          end,
          dict:new(),
          Query),

    lists:foreach(
      fun ({Id, operator, _, Inputs, _}) ->
              [dict:fetch(Id, Pids) ! {operator_location, Input, dict:fetch(Input, Pids)}
               || Input <- Inputs],
              [dict:fetch(Id, Pids) ! {operator_location, Output, dict:fetch(Output, Pids)}
               || Output <- dict:fetch(Id, Outputs)];
          (_) ->
              ok
      end,
      Query),

    lists:foreach(
      fun ({Id, output, _, Inputs}) ->
              [dict:fetch(Id, Pids) ! {add_producer, dict:fetch(Input, Pids)}
               || Input <- Inputs];
          (_) ->
              ok
      end,
      Query),

    lists:foreach(
      fun ({Id, input, _}) ->
              [dict:fetch(Id, Pids) ! {add_consumer, dict:fetch(Output, Pids)}
               || Output <- dict:fetch(Id, Outputs)];
          (_) ->
              ok
      end,
      Query),

    ok.
