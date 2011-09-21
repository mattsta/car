-module(car_tests).

-include_lib("eunit/include/eunit.hrl").

%%%----------------------------------------------------------------------
%%% Prelude
%%%----------------------------------------------------------------------
car_test_() ->
  {setup,
    fun setup/0,
    fun teardown/1,
    [
     {"Write blob",
       fun write_blob/0},
     {"Read blob",
       fun read_blob/0},
     {"Write object",
       fun write_object/0},
     {"Read object",
       fun read_object/0},
     {"Attach object to permanode",
       fun write_permanode/0},
     {"Read object from permanode",
       fun read_permanode_target/0},
     {"Read permanode hash by an index",
       fun read_permanode_by_index/0},
     {"Read permanode hash by points-to pointer",
       fun read_permanode_by_points_to/0},
     {"Update object",
       fun update_object/0},
     {"Verify permanode points to newest object",
       fun read_permanode_target_after_update/0}
    ]
  }.

%%%----------------------------------------------------------------------
%%% Tests
%%%----------------------------------------------------------------------
-define(TEST_DATA, <<"testing one data at a time for one day at a squid.">>).
-define(TEST_DATA_SHA1, <<"sha1-513afa744287621151b6331647af05586888ed16">>).

% mochihex:to_hex(crypto:sha(term_to_binary(orddict:from_list([{<<"animal">>,
% <<"cat">>}, {<<"type">>, <<"snuggly">>}])))).
-define(OBJ_AS_PROPLIST,
  [{<<"animal">>, <<"cat">>}, {<<"type">>, <<"snuggly">>}]).
-define(OBJ_SHA1, <<"sha1-230a69d67e5272a1d08f324fa9fbd7a9399d356f">>).

% D = orddict:from_list([{<<"animal">>, <<"cat">>}, {<<"type">>, <<"rabid">>},
%                        {<<"jones">>, [<<"smith">>]}]).
% mochihex:to_hex(crypto:sha(term_to_binary(D))).
-define(OBJ_AFTER_UPDATE_PROPLIST,
  [{<<"animal">>, <<"cat">>}, {<<"type">>, <<"rabid">>},
   {<<"jones">>, [<<"smith">>]}]).
-define(OBJ_UPDATED_SHA1, <<"sha1-b5c73791c36bc12b9e549ee6c0c617e060a00912">>).

write_blob() ->
  ?assertEqual(?TEST_DATA_SHA1, car:store_blob(?TEST_DATA)).

read_blob() ->
  ?assertEqual(?TEST_DATA, car:get_blob_value(?TEST_DATA_SHA1)).

write_object() ->
  Statebox = car:create_object(?OBJ_AS_PROPLIST),
  ?assertEqual(?OBJ_SHA1, car:store_object(Statebox)).

read_object() -> ok.


write_permanode() ->
  % VERIFY the Testing: Yes survives?  Or should it re-write each time?
  PermaHash = car:store_permanode(?OBJ_SHA1,
                                  car:index_bind([{"testing", "yes"}])),
  put(permanode_A_hash, PermaHash).

read_permanode_target() ->
  ?assertEqual(?OBJ_SHA1, car:get_permanode_points_to(get(permanode_A_hash))).

read_permanode_by_index() ->
  ?assertEqual([get(permanode_A_hash)],
    car:get_permanode_by_index(<<"testing">>, <<"yes">>)).

read_permanode_by_points_to() ->
  ?assertEqual([get(permanode_A_hash)],
    car:get_permanode_by_index(<<"points-to">>, ?OBJ_SHA1)).

update_object() ->
  N = car:update_object(?OBJ_SHA1, [{<<"jones">>, {add_to_set, <<"smith">>}},
                                    {<<"type">>, {write_value, <<"rabid">>}}]),
  ?assertEqual(?OBJ_UPDATED_SHA1, N).

read_permanode_target_after_update() ->
  % Verify the permanode jumped to the updated hash from update_object/0.
  ?assertEqual(?OBJ_UPDATED_SHA1,
    car:get_permanode_points_to(get(permanode_A_hash))).

%NEXT TESTS:
%  Object updating for each mutation type.
 % Update permanode and cause conflicts, check resolutions.
%  pick a better testing pool(?)
%%%----------------------------------------------------------------------
%%% Set it up, tear it down
%%%----------------------------------------------------------------------
setup() ->
  application:start(riak_pool),
  % I hope you don't want anything in your 'car' bucket...
  {ok, Keys} = riak_pool:list_keys(car, <<"car">>),
  [riak_pool:delete(car, <<"car">>, K) || K <- Keys],
  application:start(car).

teardown(_) ->
  application:stop(car),
  application:stop(riak_pool).
