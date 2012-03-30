car: content-addressed riak
===========================

Status
------
car works as far as the test suite is concerned.  It hasn't been deployed in
any major production environments yet.

What is it?
-----------

It's something that needs more documentation.

See the `test/` directory for under-documented usage examples.

Building
--------
Download LFE:
        ./rebar get-deps

Build:
        ./rebar compile

Testing
-------
        rebar eunit skip_deps=true
