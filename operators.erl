-module(operators).

-export(
   [ init/2,
     update_location/3,
     lookup_input_id/2,
     lookup_output_pid/2]).


init(Inputs, Outputs) ->
    #{directions => maps:from_list([{Id, input} || Id <- Inputs] ++ [{Id, output} || Id <- Outputs]),
      input2pid => #{},
      pid2input => #{},
      output2pid => #{}}.


update_location(
  Id, Pid,
  State = #{
    directions := Directions,
    input2pid  := Input2Pid,
    pid2input  := Pid2Input,
    output2pid := Output2Pid}) ->
    case maps:get(Id, Directions, none) of
        none ->
            State;
        input ->
            Pid2Input1 =
                case maps:get(Id, Input2Pid, none) of
                    none ->
                        Pid2Input;
                    Pid1 ->
                        maps:remove(Pid1, Pid2Input)
                end,
            State#{
              pid2input := maps:put(Pid, Id, Pid2Input1),
              input2pid := maps:put(Id, Pid, Input2Pid)
             };
        output ->
            State#{output2pid := maps:put(Id, Pid, Output2Pid)}
    end.


lookup_input_id(Pid, #{pid2input := Pid2Input}) ->
    maps:get(Pid, Pid2Input, none).


lookup_output_pid(Id, #{output2pid := Output2Pid}) ->
    maps:get(Id, Output2Pid, none).
