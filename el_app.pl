:- module(el_app, []).

/** <module> Energy labels application

A simple application to showcase what can be done with
the LOD version of the dataset of energy labels.

@author Wouter Beek
@version 2013/10
*/

:- use_module(generics(meta_ext)).
:- use_module(html(html_table)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/thread_httpd)).
:- use_module(library(pairs)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdfs)).
:- use_module(library(settings)).
:- use_module(rdf(rdf_lit)).
:- use_module(rdf(rdf_read)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').

:- setting(
  port,
  integer,
  5000,
  'The default port at which to start the server.'
).

:- http_handler(root(.), index, [prefix]).

:- initialization(start_el_app).



gas_order(PostcodePrefix, GasOrder):-
  atom_length(PostcodePrefix, PostcodePrefixLength),
  setoff(
    Gas-[Postcode,HouseNumber,Gas],
    (
      rdfs_individual_of(Building, el:'Building'),
      rdf_literal(Building, el:postcode, Postcode, el),
      sub_atom(Postcode, 0, PostcodePrefixLength, _After, PostcodePrefix),
      rdf(Building, el:certificaat, Certificate, el),
      rdf_datatype(Certificate, el:energie_verbruik_gas, decimal, Gas, el),
      rdf_datatype(Building, el:huisnummer, integer, HouseNumber, el)
    ),
    GasOrder
  ).

index(_Request):-
  reply_html_page(el, [], []).

user:body(el, Body) -->
  {
    gas_order('1094', GasOrder),
    pairs_values(GasOrder, List),
    html_table(
      [caption('Energy comparison'),header(true),indexed(true)],
      [['Postcode','Huisnummer','Gasverbruik']|List],
      HTML_Table
    )
  },
  html(body([HTML_Table|Body])).

user:head(el, Head) -->
  html(head([title('Energy Labels Demo App')|Head])).

load_el_data:-
  rdf_graph(el), !.
load_el_data:-
  absolute_file_name(output(el_1), File, [access(read),file_type(turtle)]),
  rdf_load2(File, [format(turtle),graph(el)]).

start_el_app:-
  load_el_data,
  start_el_server.

start_el_server:-
  setting(port, Port),
  http_server_property(Port, start_time(_Time)), !.
start_el_server:-
  setting(port, Port),
  % Make sure Wallace is shut down whenever Prolog shuts down.
  assert(user:at_halt(http_stop_server(Port, []))),
  http_server(http_dispatch, [port(Port)]).

