-module(car_sup).

-behaviour(supervisor).

%% External exports
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  Processes = [],
  Strategy = {one_for_one, 10, 10},
  {ok,
   {Strategy, lists:flatten(Processes)}}.
