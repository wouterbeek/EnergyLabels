:- module(
  el_script,
  [
    el_script/0
  ]
).

/** <module> Energylabels script

Script for generating an RDF representation of an XML file on energylabels.

The result was initially intended to be used within the Huiskluis project.

@author Wouter Beek
@see https://data.overheid.nl/data/dataset/energielabels-agentschap-nl
@tbd insert_newlines/2 seems to use too much memory.
@version 2013/04, 2013/06-2013/07, 2013/09-2013/12, 2014/02-2014/03
*/

:- use_module(dcg(dcg_ascii)).
:- use_module(dcg(dcg_generic)).
:- use_module(dcg(dcg_peek)).
:- use_module(library(debug)).
:- use_module(library(pure_input)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(swi_ide)).
:- use_module(library(thread)).
:- use_module(os(archive_ext)).
:- use_module(os(dir_ext)).
:- use_module(os(file_mime)).
:- use_module(os(io_ext)). %DEB
:- use_module(rdf(rdf_build)).
:- use_module(rdf(rdf_container)).
:- use_module(rdf_term(rdf_string)).
:- use_module(xml(xml_namespace)).
:- use_module(xml(xml_word)).

:- xml_register_namespace(el,
    'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').

:- prolog_ide(debug_monitor). %DEB
:- debug(el_script). %DEB



el_script:-
  absolute_file_name(data(el), Dir, [access(read),file_type(directory)]),
  extract_directory([recursive(true)], Dir),
  directory_files([], Dir, Files),
  maplist(parse_file, Files). %DEB
  %%%%concurrent_maplist(parse_file, Files).


parse_file(File):-
  phrase_from_file(xml_parse(el), File),
  %%%%file_to_atom(File, Atom), %DEB
  %%%%dcg_phrase(xml_parse(el), Atom), %DEB
  debug(el_script, 'Done parsing file ~w', [File]). %DEB


%! xml_parse(+RdfGraph:atom)// is det.
% Parses the root tag.

xml_parse(G) -->
  xml_declaration(_),
  xml_start_tag(RootTag), skip_whites,
  {rdf_create_next_resource(G, RootTag, S, G)},
  xml_parses(S, G),
  xml_end_tag(RootTag), dcg_done.


% Non-tag content.
xml_parse(S, G) -->
  xml_start_tag(PTag), skip_whites,
  xml_content(PTag, Codes), !,
  {
    atom_codes(O, Codes),
    rdf_global_id(el:PTag, P),
    rdf_assert_string(S, P, O, G)
  }.
% Skip short tags.
xml_parse(_, _) -->
  xml_empty_tag(_), skip_whites, !.
% Nested tag.
xml_parse(S, G) -->
  xml_start_tag(OTag), skip_whites,
  {rdf_create_next_resource(G, OTag, O, G)},
  xml_parses(O, G),
  xml_end_tag(OTag), skip_whites,
  {
    rdf_assert_individual(S, rdf:'Bag', G),
    rdf_assert_collection_member(S, O, G)
  }.


skip_whites -->
  ascii_whites, !.


xml_parses(S, G) -->
  xml_parse(S, G), !,
  xml_parses(S, G).
xml_parses(_, _) --> [].


% The tag closes: end of content codes.
xml_content(Tag, []) -->
  xml_end_tag(Tag), skip_whites, !.
% Another XML tag starts, this is not XML content.
xml_content(_, []) -->
  dcg_peek(xml_start_tag(_)), !, {fail}.
% Parse a code of content.
xml_content(Tag, [H|T]) -->
  [H],
  xml_content(Tag, T).

