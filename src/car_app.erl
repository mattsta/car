-module(car_app).

-behaviour(application).
-export([start/2,stop/1]).

-spec start(any(), any()) -> any().
start(_Type, _StartArgs) ->
  % zog_deps:ensure(),
  application:start(crypto),
  car_sup:start_link().

-spec stop(any()) -> any().
stop(_State) ->
  ok.
