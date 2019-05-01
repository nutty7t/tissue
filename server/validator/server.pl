:- use_module(library(http/thread_httpd)).
:- use_module(library(http/http_dispatch)).


% HTTP Routes.
:- http_handler(root(.), tissue, [prefix]).


% Starts the server.
server(Port) :-
	http_server(http_dispatch, [port(Port)]).


% HTTP Handler.
tissue(_Request) :-
	format('Content-Type: text/plain~n~n'),
	format('tissue validator!~n').

