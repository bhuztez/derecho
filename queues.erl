-module(queues).

-export(
   [ init/1,
     add/4]).


init(Inputs) ->
    #{seqs => maps:from_list([{Id, 0} || Id <- Inputs]),
      buffers => maps:from_list([{Id, []}|| Id <- Inputs])}.


add(Id, Seq, Msg, State = #{seqs := Seqs, buffers := Buffers}) ->
    Buffer = ordsets:add_element({Seq, Msg}, maps:get(Id, Buffers)),
    {Seq1, Msgs, Buffer1} = pop(maps:get(Id, Seqs), Buffer),
    {Msgs,
     State#{
       seqs := maps:put(Id, Seq1, Seqs),
       buffers := maps:put(Id, Buffer1, Buffers)}}.


pop(Seq, [{Seq, Msg}|Rest]) ->
    {Seq1, Msgs, Buffer} = pop(Seq + 1, Rest),
    {Seq1, [Msg|Msgs], Buffer};
pop(Seq, Buffer) ->
    {Seq, [], Buffer}.
