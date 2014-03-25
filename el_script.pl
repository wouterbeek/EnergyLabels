:- module(
  el_script,
  [
    el_script/0
  ]
).

/** <module> Energylabels script

Script for generating an RDF representation of an XML file on energylabels.
The result is intended to be used within the Huiskluis project.

# Strange DOM

~~~{.pl}
[
  element(
    'Pandcertificaat',
    [],
    [
      element('PandVanMeting_postcode',[],['2581VX']),
      element('PandVanMeting_huisnummer',[],['63']),
      element('PandVanMeting_huisnummer_toev',[],[]),
      element('PandVanMeting_gebouwcode',[],[]),
      element('PandVanMeting_opnamedatum',[],['20080613']),
      element('PandVanMeting_energieprestatieindex',[],['2.15']),
      element('PandVanMeting_energieverbruiktype',[],[absoluut]),
      element('PandVanMeting_energieverbruikmj',[],['91253.07']),
      element('PandVanMeting_energieklasse',[],['E']),
      element('Meting_geldig_tot',[],['20180613']),
      element('Afmeldnummer',[],['782469152']),
      element('Pand_registratiedatum',[],['20080620']),
      element('Pand_postcode',[],['2581VX']),
      element('Pand_huisnummer',[],['63']),
      element('Pand_huisnummer_toev',[],[]),
      element('Pand_gebouwcode',[],[]),
      element('Pand_plaats',[],['\'S-GRAVENHAGE']),
      element('Pand_cert_type',[],['W'])
    ]
  ),
  element('Pandcertificaat',[],[])
]
~~~

@author Wouter Beek
@see https://data.overheid.nl/data/dataset/energielabels-agentschap-nl
@tbd insert_newlines/2 seems to use too much memory.
@version 2013/04, 2013/06-2013/07, 2013/09-2013/12, 2014/02-2014/03
*/

:- use_module(dcg(dcg_ascii)).
:- use_module(dcg(dcg_content)).
:- use_module(dcg(dcg_generic)).
:- use_module(dcg(dcg_peek)).
:- use_module(el(el_parse)).
:- use_module(el(el_void)).
:- use_module(library(archive)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(pure_input)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(swi_ide)).
:- use_module(library(thread)).
:- use_module(os(dir_ext)).
:- use_module(os(file_mime)).
:- use_module(os(io_ext)).
:- use_module(rdf(rdf_build)).
:- use_module(rdf(rdf_container)).
:- use_module(rdf_term(rdf_string)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(el,
    'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').

:- prolog_ide(debug_monitor).
:- debug(el_script).



el_script:-
  absolute_file_name(data(el), Dir, [access(read),file_type(directory)]),
  extract_directory([recursive(true)], Dir),
  directory_files([], Dir, Files),
  maplist(parse_file, Files).


parse_file(File):-
  phrase_from_file(xml_parse(el), File),
  %%%%file_to_atom(File, Atom),
  %%%%dcg_phrase(xml_parse(el), Atom),
  debug(el_script, 'Done parsing file ~w', [File]).


%! xml_parse(+RdfGraph:atom)// is det.
% Parses the root tag.

xml_parse(G) -->
  xml_declaration(_),
  xml_tag_open(RootTag), skip_whites,
  {rdf_create_next_resource(G, RootTag, S, G)},
  xml_parses(S, G),
  xml_tag_close(RootTag), dcg_done.


% Non-tag content.
xml_parse(S, G) -->
  xml_tag_open(PTag), skip_whites,
  xml_content(PTag, Codes), !,
  {
    atom_codes(O, Codes),
    rdf_global_id(el:PTag, P),
    rdf_assert_string(S, P, O, G)
  }.
% Skip short tags.
xml_parse(_, _) -->
  xml_tag_short(_), skip_whites, !.
% Nested tag.
xml_parse(S, G) -->
  xml_tag_open(OTag), skip_whites,
  {rdf_create_next_resource(G, OTag, O, G)},
  xml_parses(O, G),
  xml_tag_close(OTag), skip_whites,
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
  xml_tag_close(Tag), !.
% Another XML tag starts, this is not XML content.
xml_content(_, []) -->
  dcg_peek(xml_tag_open(_)), !, {fail}.
% Parse a code of content.
xml_content(Tag, [H|T]) -->
  [H],
  xml_content(Tag, T).


xml_tag_close(Tag) -->
  `</`,
  el_word(Tag),
  blanks,
  `>`, ascii_whites, !.


xml_tag_open(Tag) -->
  `<`,
  el_word(Tag),
  `>`, !.


xml_tag_short(Tag) -->
  `<`,
  el_word(Tag),
  blanks,
  `/>`, ascii_whites, !.


/*
mutation_message(G) -->
  xml_tag_long('

mutation_messages1(G) -->
  xml_tag_long('MutatieBerichten', mutation_messages2(G)).

mutation_messages2(G) -->
  xml_tag_long('MutatieBericht', mutation_message(G)).


certificates(G) -->
  certificate(G), !,
  certificates(G).
certificates(_) -->
  dcg_done.


certificate(G) -->
  {rdf_create_next_resource(el, 'Certificate', S, G)},
  dcg_until(
    [output_format(atom),end_mode(inclusive)],
    xml_tag_open('Pandcertificaat'),
    Debug
  ),
  {format(user_output, '~w\n', [Debug])}, %DEB
  dcg_until(
    [output_format(codes),end_mode(exclusive)],
    xml_tag_close('Pandcertificaat'),
    Codes
  ),
  {phrase(certificate_properties(S, G), Codes)}.


certificate_properties(S, G) -->
  xml_tag_short, !,
  certificate_properties(S, G).
certificate_properties(S, G) -->
  certificate_property(S, G), !,
  certificate_properties(S, G).
certificate_properties(_, _) --> [].


certificate_property(S, G) -->
  xml_tag_open(P1),
  dcg_until(
    [output_format(atom),end_mode(exclusive)],
    xml_tag_close(P1),
    O
  ),
  xml_tag_close(P1),
  {
    rdf_global_id(el:P1, P2),
    rdf_assert_string(S, P2, O, G)
  }.
*/


el_word(Word) -->
  {nonvar(Word)}, !,
  {atom_codes(Word, Codes)},
  el_word_codes(Codes).
el_word(Word) -->
  el_word_codes(Codes),
  {atom_codes(Word, Codes)}.

el_word_codes([H|T]) -->
  (ascii_letter(H) ; underscore(H)),
  el_word_codes(T).
el_word_codes([]) --> [].


%! extract_directory(+Options:list(nvpair), +Directory:atom) is det.
% Extracts all archives in the given directory.
% Extract files recursively, e.g. first `gunzip`, then `tar`.
%
% The following options are supported:
%   * =|file_types(+FileTypes:list(atom))|=
%     A list of atomic file types that are used as a filter.
%     Default: no file type filter.
%   * =|recursive(+Recursive:boolean)|=
%     Whether subdirectories are searched recursively.
%     Default: `true`.

extract_directory(O1, Dir):-
  directory_files(O1, Dir, Files),
  extract_directory(O1, Dir, Files).

extract_directory(_, _, []):- !.
extract_directory(O1, Dir, PreviouslyExtractedFiles):-
  PreviouslyExtractedFiles \== [], !,
  directory_files(O1, Dir, Files),
  extract_files(Files, ExtractedFiles),
  extract_directory(O1, Dir, ExtractedFiles).


extract_files([], []):- !.
% A file that needs to be further extracted.
extract_files([H|T1], [H|T2]):-
  extract_file_once(H), !,
  extract_files(T1, T2).
% A fully extracted file.
extract_files([_|T], L):-
  extract_files(T, L).


%! extract_file_once(+File:atom) is semidet.
% Succeeds if the given file could be extracted.

extract_file_once(File):-
  file_directory_name(File, Dir),
  catch(
    (
      archive_extract(File, Dir, []),
      delete_file(File)
    ),
    error(archive_error(25, _UnrecognizedArchiveFormat), _),
    fail
  ).


/*
      el_script:ap_stage([], extract_archive),
      el_script:ap_stage([], to_small_files),
      el_script:ap_stage([], insert_newlines),
      el_script:ap_stage([to(big,xml)], merge_into_one_file),
      el_script:ap_stage([stat_lag(100)], el_parse),
      el_script:ap_stage([], el_clean),
      el_script:ap_stage([to(output,'VoID',turtle)], assert_el_void)
    ],
    _
  ).

% Split into smaller files.
to_small_files(FromFile, ToDir):-
  split_into_smaller_files(FromFile, ToDir, temp_).

% Insert newlines in small files.
insert_newlines(FromDir, ToDir):-
  % Add newlines to the small files.
  directory_file_path(FromDir, 'temp_*', RE),
  expand_file_name(RE, FromFiles),
  run_on_sublists(FromFiles, insert_newlines_worker(ToDir)).

% This predicate can only run in threads.
% See module THREAD_EXT.
insert_newlines_worker(ToDir, FromFiles):-
  thread_self(ThreadAlias),
  forall(
    member(FromFile, FromFiles),
    setup_call_cleanup(
      open(FromFile, read, Stream),
      (
        directory_file_path(_FromDir, FileName, FromFile),
        absolute_file_name(
          FileName,
          ToFile,
          [access(write),file_type(text),relative_to(ToDir)]
        ),
        read_stream_to_codes(Stream, Codes1),
        % Add linefeeds between tags.
        % From: `> <`
        % To:   `> LINE-FEED <`
        phrase(dcg_replace([62,60], [62,10,60]), Codes1, Codes2),
        % It will sometimes happen that the cuttoff lies exactly
        %  between `>` and `<`.
        % We then have to start with a linefeed.
        (
          first(Codes2, 60)
        ->
          Codes3 = [10|Codes2]
        ;
          Codes3 = Codes2
        ),
        codes_to_file(Codes3, ToFile)
      ),
      (
        close(Stream),
        debug(el_script, 'File ~w split into lines.', [FromFile]),
        thread_success(ThreadAlias)
      )
    )
  ).

el_clean(FromDir, ToDir):-
  % Do this for every Turtle file in the from directory.
  % (Due to memory limitations.)
  directory_files(
    [
      file_types([turtle]),
      include_directories(false),
      order(lexicographic),
      recursive(false)
    ],
    FromDir,
    FromFiles
  ),

  % STATS
  length(FromFiles, Potential),
  ap_stage_init(Potential),

  maplist(el_clean_(ToDir), FromFiles).

el_clean_(ToDir, FromFile):-
  file_alternative(FromFile, ToDir, _, _, ToFile),
  rdf_equal(el:huisnummer_toevoeging, P),

  % Load and unload RDF.
  rdf_update_literal(_, P, _, _, _, _, strip_lexical_form('-')),

  % STATS
  ap_stage_tick.
*/

