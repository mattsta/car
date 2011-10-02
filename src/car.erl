-module(car).

-export([store_blob/1]).
-export([get_blob_value/1]).
-export([store_permanode/2]).
-export([get_permanode_points_to/1]).
-export([get_permanode_by_index/2]).
-export([objects_by_index/2]).
-export([index_bind/1]).
-export([store_object/1, store_object/2]).
-export([write/2]).
-export([create_object/0, create_object/1]).
-export([update_object/2, update_object/3]).
-export([statebox_to_hash/1]).

-define(POINTER_IDX,  "points-to").
-define(REPLACES_IDX, "replaces").

%%%----------------------------------------------------------------------
%%% Index Management
%%%----------------------------------------------------------------------
index_pair(Name, Val) when is_list(Name) andalso is_binary(Val) ->
  index_pair(Name, binary_to_list(Val));
index_pair(Name, Val) when is_binary(Name) andalso is_binary(Val) ->
  index_pair(binary_to_list(Name), binary_to_list(Val));
index_pair(Name, Val) when is_list(Name) andalso is_list(Val) ->
  {Name ++ "_bin", Val}.

index_wrap(Pair) when is_tuple(Pair) ->
  [{<<"index">>, [Pair]}];
index_wrap(Pairs) when is_list(Pairs) ->
  [{<<"index">>, Pairs}].

index_bind(FieldValueProplist) when is_list(FieldValueProplist) ->
  index_wrap([index_pair(K, V) || {K, V} <- FieldValueProplist]).

%%%----------------------------------------------------------------------
%%% Creating Things
%%%----------------------------------------------------------------------
% Create Permanode (input: Searchable Attributes, Target Hash):
%  1. Create permanode (hash or natural key?)
%  2. Add indexes for searchable attributes
%  3. Add target-hash and POINTS-TO idx.
%  4. Write permanode (with idxs for points-to and proper attributes)
% Update Object (input: old object hash, mutations)
%  1. Get existing object
%  2. Apply mutations
%     a. add-to-set
%     b. prepend-to-list
%     c. append-to-list
%     d. replace-ref (oldref, newref)
%     e. remove-from-set
%     f. remove-from-list
%  3. Add idx for REPLACES old object hash.
%  4. Re-hash object
%  4. Store with new hash and mutations applied
%  5. Update all POINTS-TO (old hash) with new hash%
%     ! - Important - ! -- Only permanodes have POINTS-TO and get updated
%                          automatically.  If a object A points to another
%                          object B and B is updated, A will never change.
%                          If you *always* want the latest version of B, then
%                          A must point to a permanode of B.
% Delete (input: permanode)
%  1. Get existing permanode
%  2. Clear all hashes in the permanode
%  3. Remove all points-to idx.
%  4. Write back.

%%%----------------------------------------------------------------------
%%% Common Low-level Manipulators
%%%----------------------------------------------------------------------
write(Obj) when is_tuple(Obj) ->
  car_riak:put(Obj).

write(Proplist, Idxs) when is_list(Proplist) andalso is_list(Idxs) ->
  Statebox = car:create_object(Proplist),
  StateboxHash = store_object(Statebox),
  % Assumes we pre-ran index_bind on incoming Idxs:
  car:store_permanode(StateboxHash, Idxs).

write(Ref, Bytes, Idxs) when is_binary(Bytes) andalso is_list(Idxs) ->
  write(Ref, Bytes, dict:from_list(Idxs));
write(Ref, Bytes, Idxs) when is_binary(Bytes) andalso is_tuple(Idxs) ->
  Obj = car_riak:obj(Ref, Bytes, Idxs),
  write(Obj).

fetch(Ref) when is_binary(Ref) ->
  car_riak:get(Ref).

%%%----------------------------------------------------------------------
%%% Blob Manipulations
%%%----------------------------------------------------------------------
%XPORT
store_blob(Bytes) when is_binary(Bytes) ->
  BlobRef = stronghash(Bytes),
  Obj = car_riak:obj(BlobRef, Bytes),
  car_riak:put(Obj),
  BlobRef.

get_blob_value(Hash) when is_binary(Hash) ->
  car_riak:get_value(Hash).

%%%----------------------------------------------------------------------
%%% Permanode Creation, Reading, Merging
%%%----------------------------------------------------------------------
statebox_to_hash(Statebox) ->
  % Hash the entire statebox to get its storage key.
  % NB: Stateboxes have last modified times, so *each* statebox will be a
  % unique hash.  Not entirely useful in a content-addressed system.
  % But -- Objects/stateboxes *point* to stable blob data.  Ideally.

  ResolvedBin = term_to_binary(Statebox),
  stronghash(ResolvedBin).

permanode() ->
  UniqueData = {node(), now()},
  % this is a unique dumbness -- we're storing non-content-addressed content
  % in our content-addressed storage layer.  Meep.
  % If we had content-addressed permanodes, then the permanodes could never
  % change and we would need a clever indexing scheme to track which objects
  % currently resolve to which permanode.
  stronghash(term_to_binary(UniqueData)).

store_permanode(PointsTo, Metadatas) when
    is_binary(PointsTo) andalso is_list(Metadatas) ->
  PermaHash = permanode(),
  Combined = case Metadatas of
               [] -> dict:new();
                _ -> MD = dict:from_list(Metadatas),
                     dict:append(<<"index">>, idx_points_to(PointsTo), MD)
             end,
  write(PermaHash, PointsTo, Combined),
  PermaHash.

objects_by_index(IndexField, IndexValue) ->
  FoundNodes = get_permanode_by_index(IndexField, IndexValue),
  % points_to auto-resolves siblings
  % we can parallelize the crap outta this:
  StateboxHashes = [get_permanode_points_to(N) || N <- FoundNodes],
  % Now return a list of {ObjectHash, Orddict}.
  % ObjectHash *MUST* be given to update with the list of mutations.
  [{H, object_value(binary_to_term(get_blob_value(H)))} ||
    H <- StateboxHashes].


get_permanode_by_index(IndexField, IndexValue) ->
  IndexName = iolist_to_binary([IndexField, <<"_bin">>]),
  car_riak:get_index(IndexName, IndexValue).

get_permanode_points_to(PermaHash) when is_binary(PermaHash) ->
  car_riak:get_value(PermaHash);
get_permanode_points_to(RiakObj) when is_tuple(RiakObj) ->
  case riakc_obj:value_count(RiakObj) of
    1 -> riakc_obj:get_value(RiakObj);
    0 -> <<>>;
    _ -> statebox_resolve_permanode(RiakObj)
  end.

% Three buckets: Object bucket: don't allow duplicates. -- last write wins
%  (but use statebox for mutating ops)
%              Blob bucket: no duplicates.  last write wins.
%              Permanode bucket: allow multi.
% This is basically a merge commit.
statebox_resolve_permanode(RiakObj) ->
  % Get all hashes from the conflicting permanode entires (siblings)
  AllHashes = riakc_obj:get_values(RiakObj),

  % Get all objects those hashes point to
  AllObjects = [get(H) || H <- AllHashes],  % FIXME: this should be parallel

  % Get all values on all the objects.  The values are #statebox{} serialized.
  AllValues = [binary_to_term(riakc_obj:get_value(O)) || O <- AllObjects],

  % Merge all retrieved values according to their statebox mutations
  ResolvedObject = statebox:merge(AllValues),

  % Hash the contents of the statebox for the new merge commit name
  ResolvedHash = statebox_to_hash(ResolvedObject),

  % Create index entries noting this new object replaces all constituent nodes
  Replaces = idx_replaces(AllHashes),

  % Write the resolved object to storage.
  write(ResolvedHash, term_to_binary(ResolvedObject, [compressed]), Replaces),

  % Now update the permanode to point to the newly created resolved object.
  UpdatedPermanodeVal = riakc_obj:update_value(ResolvedHash),
  NewMds = index_wrap(idx_points_to(ResolvedHash)),
  NewMdDict = dict:from_list(NewMds),
  UpdatedPermanodeMeta =
    riakc_obj:update_metadata(UpdatedPermanodeVal, NewMdDict),
  write(UpdatedPermanodeMeta),
  ResolvedHash.


%%%----------------------------------------------------------------------
%%% Permanode Updating
%%%----------------------------------------------------------------------
update_permanodes_for_object(OldHash, NewHash) ->
  MoveOldHashToNewHash =
    fun(PointedToBy) ->
      replace_permanode_pointer(PointedToBy, OldHash, NewHash)
    end,
  car_riak:for_each_pointer_index(MoveOldHashToNewHash, OldHash).

replace_permanode_pointer(ExistingPermaHash, _OldHash, NewHash) ->
  NewMetadata = dict:from_list(index_wrap(idx_points_to(NewHash))),

  {ok, RiakObj} = fetch(ExistingPermaHash),
  UpdatedMd = riakc_obj:update_metadata(RiakObj, NewMetadata),
  NewEverythingAdded = riakc_obj:update_value(UpdatedMd, NewHash),

  write(NewEverythingAdded).



%%%----------------------------------------------------------------------
%%% Index Metadata
%%%----------------------------------------------------------------------
idx_points_to(Hash) ->
  index_pair(?POINTER_IDX, Hash).

idx_replaces(OldHash) when is_binary(OldHash) ->
  index_pair(?REPLACES_IDX, OldHash);
idx_replaces(OldHashes) when is_list(OldHashes) ->
  [index_pair(?REPLACES_IDX, OldHash) || OldHash <- OldHashes].


%%%----------------------------------------------------------------------
%%% Object Creation
%%%----------------------------------------------------------------------
% ALL OBJECTS ARE SERIALIZED #statebox{} RECORDS
% For an object, you can only:
%   - Create ordict with values of ordset
%   - Operations per-field:
%     - Union - add value to set at a field
%     - Subtract - subtract value from set at a field
%     - Update (?) - A way of injecting arbitrary operations onto a key.
%     - Merge - Merge another entire orddict into this orddict
%     - Merge - merge(Proplist) keys/values into the orddict
%     - Store  - store value in field
%     - delete - delete entire field,value from the orddict
%     - erase - deletes all values for field
% Store an object while maintaining all permanode anchors

store_object(Statebox) ->
  store_object(Statebox, []).

% Note: This makes a raw state box with an index that IS NOT a permanode.
% It will break permanode lookups by index.  Probably should never use it.
store_object(Statebox, Idxs) when is_tuple(Statebox) andalso is_list(Idxs) ->
  ResolvedHash = statebox_to_hash(Statebox),
  write(ResolvedHash, term_to_binary(Statebox, [compressed]), Idxs),
  ResolvedHash.

create_object() ->
  statebox_orddict:from_values([]).
create_object(Proplist) when is_list(Proplist) ->
  statebox_orddict:from_values([Proplist]).

%%%----------------------------------------------------------------------
%%% Object Updating
%%%----------------------------------------------------------------------
statebox_prune(Statebox) ->
  LastNOperations = 1024,
  ExpireMS = 3600 * 24 * 1000,  % 3600 sec/hr * 24 hr/day * 1000 ms/s = 1 Day
  statebox:truncate(LastNOperations, statebox:expire(ExpireMS, Statebox)).

update_object(OldHash, Mutations) ->
  update_object(OldHash, Mutations, use_existing_metadata).

update_object(OldHash, Mutations, MetaInfo) ->
  case fetch(OldHash) of
        {ok, RiakObj} -> update_object(RiakObj, OldHash, Mutations, MetaInfo);
    {error, notfound} -> notfound
  end.


update_object(RiakObj, OldHash, Mutations, use_existing_metadata) ->
  ExistingMd = car_riak:md(RiakObj),
  update_object(RiakObj, OldHash, Mutations, {resolved_metadata, ExistingMd});

update_object(RiakObj, OldHash, Mutations, {append_metadata, NewMd}) when
    is_list(NewMd) ->
  UpdatedMd = NewMd ++ car_riak:md(RiakObj),
  update_object(RiakObj, OldHash, Mutations, {resolved_metadata, UpdatedMd});

update_object(RiakObj, OldHash, Mutations, {replace_metadata, NewMd}) when
    is_list(NewMd) ->
  update_object(RiakObj, OldHash, Mutations, {resolved_metadata, NewMd});

update_object(RiakObj, OldHash, Mutations, {resolved_metadata, UseMd}) ->
  Val = riakc_obj:get_value(RiakObj),
  EVal = binary_to_term(Val),
  % Mutations are ~ {Field, {LocalOpName, Value}}
  TotalMutations = [object_pre_update(M) || M <- Mutations],
  Resolved = statebox:modify(TotalMutations, EVal),
  Pruned = statebox_prune(Resolved),
  NewHash = store_object(Pruned,
              UseMd ++ index_wrap(idx_replaces(OldHash))),
  update_permanodes_for_object(OldHash, NewHash),
  NewHash.


object_pre_update({Field, {add_to_set, Value}}) when is_list(Value) ->
  statebox_orddict:f_union(Field, Value);
object_pre_update({Field, {add_to_set, Value}}) when is_binary(Value) ->
  object_pre_update({Field, {add_to_set, [Value]}});

object_pre_update({Field, {remove_from_set, Value}}) when is_list(Value) ->
  statebox_orddict:f_subtract(Field, Value);
object_pre_update({Field, {remove_from_set,Value}}) when is_binary(Value) ->
  object_pre_update({Field, {remove_from_set, [Value]}});

object_pre_update({Field, {write_value, Value}}) ->
  statebox_orddict:f_store(Field, Value);

object_pre_update({Field, {delete_with_value, Value}}) ->
  statebox_orddict:f_delete({Field, Value});

object_pre_update({Field, clear_value}) ->
  [statebox_orddict:f_erase(Field)].

%%%----------------------------------------------------------------------
%%% Object Reading
%%%----------------------------------------------------------------------
object_value(Statebox) ->
  statebox:get_value(Statebox).


%%%----------------------------------------------------------------------
%%% Deleting Things
%%%----------------------------------------------------------------------
% noop.  Implement deletion/GC from permanode claims/roots later.


%%%----------------------------------------------------------------------
%%% Helpers
%%%----------------------------------------------------------------------
-compile({inline, [shahash/1, stronghash/1]}).
stronghash(Bytes) ->
  shahash(Bytes).

shahash(Bytes) ->
  Hash = crypto:sha(Bytes),
  Type = "sha1-",
  Hex = mochihex:to_hex(Hash),
  iolist_to_binary([Type, Hex]).
