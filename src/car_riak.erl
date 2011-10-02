-module(car_riak).

-compile(export_all).

-define(CB, <<"car">>). % I CAN HAZ RIAK BUKKET?

%%%----------------------------------------------------------------------
%%% Data Doers
%%%----------------------------------------------------------------------
for_each_pointer_index(KeyProcessingFun, OldHash) when
  is_function(KeyProcessingFun) andalso is_binary(OldHash) ->
  case get_index(<<"points-to_bin">>, OldHash) of
         [] -> no_index;
    Results -> % we can paralleize the crap out of this:
               [KeyProcessingFun(FoundNode) || FoundNode <- Results]
  end.

%%%----------------------------------------------------------------------
%%% Object Wrappers
%%%----------------------------------------------------------------------
obj(Ref, Value) ->
  riakc_obj:new(?CB, Ref, Value).

obj(Ref, Value, Idxs) ->
  riakc_obj:update_metadata(obj(Ref, Value), Idxs).

md(RiakObj) ->
  dict:to_list(riakc_obj:get_metadata(RiakObj)).

%%%----------------------------------------------------------------------
%%% Raw Riak Ops Against Pools
%%%----------------------------------------------------------------------
-compile({inline, [get/1, get/2, get_value/1, get_value/2,
                   put/1, get_index/2, get_index/3]}).
put(RiakObj) ->
  riak_pool:put(car, RiakObj).

get(Bucket, Ref) ->
  riak_pool:get(car, Bucket, Ref).

get_index(Bucket, Idx, Value) ->
  case riak_pool:get_index(car, Bucket, Idx, Value) of
           {ok, []} -> [];  % shorthand so we don't run an empty comprehension
    {ok, FoundOnes} -> [FoundKey || [_Bucket, FoundKey] <- FoundOnes];
                  _ -> []  % technically, we should never get here.
                           % non-existing idxs return {ok, []}, not notfound.
  end.


%%%----------------------------------------------------------------------
%%% Wrapped Raw Riak Ops Against Pools
%%%----------------------------------------------------------------------
get(Ref) ->
  get(?CB, Ref).

get_value(Ref) ->
  get_value(?CB, Ref).

get_value(Bucket, Ref) ->
  case get(Bucket, Ref) of
            {ok, Got} -> riakc_obj:get_value(Got);
    {error, notfound} -> throw({notfound, Bucket, Ref})
  end.

get_index(Idx, Value) ->
  get_index(?CB, Idx, Value).
