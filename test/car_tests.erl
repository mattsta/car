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

% D = orddict:from_list([{<<"animal">>, <<"cat">>}, {<<"type">>, <<"rabid">>},
%                        {<<"jones">>, [<<"smith">>]}]).
% mochihex:to_hex(crypto:sha(term_to_binary(D))).
-define(OBJ_AFTER_UPDATE_PROPLIST,
  [{<<"animal">>, <<"cat">>}, {<<"type">>, <<"rabid">>},
   {<<"jones">>, [<<"smith">>]}]).

write_blob() ->
  ?assertEqual(?TEST_DATA_SHA1, car:store_blob(?TEST_DATA)).

read_blob() ->
  ?assertEqual(?TEST_DATA, car:get_blob_value(?TEST_DATA_SHA1)).

obj_A_SHA() -> get(obj_A_SHA).

write_object() ->
  Statebox = car:create_object(?OBJ_AS_PROPLIST),
  ObjSha = car:statebox_to_hash(Statebox),
  put(obj_A_SHA, ObjSha),
  ?assertEqual(ObjSha, car:store_object(Statebox)).

read_object() -> ok.


permanode_A_SHA() -> get(permanode_A_SHA).

write_permanode() ->
  % VERIFY the Testing: Yes survives?  Or should it re-write each time?
  PermaHash = car:store_permanode(obj_A_SHA(),
                                  car:index_bind([{"testing", "yes"}])),
  put(permanode_A_SHA, PermaHash).

read_permanode_target() ->
  ?assertEqual(obj_A_SHA(), car:get_permanode_points_to(permanode_A_SHA())).

read_permanode_by_index() ->
  ?assertEqual([permanode_A_SHA()],
    car:get_permanode_by_index(<<"testing">>, <<"yes">>)).

read_permanode_by_points_to() ->
  ?assertEqual([permanode_A_SHA()],
    car:get_permanode_by_index(<<"points-to">>, obj_A_SHA())).

update_object() ->
  car:update_object(obj_A_SHA(),
    [{<<"jones">>, {add_to_set, <<"smith">>}},
     {<<"type">>, {write_value, <<"rabid">>}}]).
  % We can't check the return value here anymore since the hash is now over
  % the *entire* statebox including last modified timestamps.

read_permanode_target_after_update() ->
  % Verify the permanode jumped to the updated hash from update_object/0.
  ?assertNot(obj_A_SHA() =:=
    car:get_permanode_points_to(permanode_A_SHA())).

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
