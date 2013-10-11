:- module(
  el_app,
  [
    gas_order/2 % +PostcodePrefix:atom
                % -GarOrder:ordset(pair)
  ]
).

/** <module> Energy labels application

A simple application to showcase what can be done with
the LOD version of the dataset of energy labels.

@author Wouter Beek
@version 2013/10
*/

:- use_module(generics(meta_ext)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(semweb/rdfs)).
:- use_module(rdf(rdf_lit)).
:- use_module(rdf(rdf_read)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').

:- initialization(load_el).



gas_order(PostcodePrefix, GasOrder):-
  atom_length(PostcodePrefix, PostcodePrefixLength),
  setoff(
    Gas-[Postcode,HouseNumber],
    (
      rdfs_individual_of(Building, el:'Building'),
      rdf_literal(Building, el:postcode, Postcode, G),
      sub_atom(Postcode, 0, PostcodePrefixLength, _After, PostcodePrefix),
      rdf(Building, el:certificaat, Certificate, G),
      rdf_datatype(Certificate, el:energie_verbruik_gas, decimal, Gas, G),
      rdf_datatype(Building, el:huisnummer, integer, HouseNumber, G)
    ),
    GasOrder
  ).

load_el:-
  absolute_file_name(output(el_1), File, [access(read),file_type(turtle)]),
  rdf_load2(File, [format(turtle),graph(el)]).

