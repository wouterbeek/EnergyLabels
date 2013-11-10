:- module(el_app, []).

/** <module> Energy labels application

A simple application to showcase what can be done with
the LOD version of the dataset of energy labels.

@author Wouter Beek
@version 2013/10-2013/11
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
:- use_module(rdf(rdf_datatype)).
:- use_module(rdf(rdf_lit_read)).
:- use_module(rdf(rdf_read)).
:- use_module(rdf(rdf_serial)).
:- use_module(server(app_ui)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').

:- http_handler(root(el), el, []).

:- setting(
  port,
  integer,
  5000,
  'The default port at which to start the server.'
).

:- initialization(load_el_data).



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

el(_Request):-
  reply_html_page(app_style, \el_head, \el_body).

el_body -->
  {
    gas_order('1094', GasOrder),
    pairs_values(GasOrder, List),
    html_table(
      [caption('Energy comparison'),header(true),indexed(true)],
      [['Postcode','Huisnummer','Gasverbruik']|List],
      HTML_Table
    )
  },
  html(body(HTML_Table)).

el_head -->
  html(head(title('Energy Labels Demo App'))).

% Already loaded in memory.
load_el_data:-
  rdf_graph(el), !.
% Already available on disk.
load_el_data:-
  absolute_file_name(
    output(el_1),
    File,
    [access(read),file_errors(fail),file_type(turtle)]
  ), !,
  rdf_load2(File, [format(turtle),graph(el)]).
% Has to be created.
load_el_data:-
  use_module(el(el_script)).

