:- module(el_script, []).

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
@version 2013/04, 2013/06-2013/07, 2013/09-2013/11
*/

:- use_module(ap(ap)).
:- use_module(ap(ap_act)).
:- use_module(el(el_parse)). % Used in ap_stage/2.
:- use_module(el(el_void)).
:- use_module(generics(codes_ext)).
:- use_module(generics(db_ext)).
:- use_module(generics(list_ext)).
:- use_module(generics(thread_ext)).
:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(lists)).
:- use_module(library(readutil)).
:- use_module(library(semweb/rdf_db)).
:- use_module(os(dir_ext)).
:- use_module(os(file_ext)).
:- use_module(os(io_ext)).
:- use_module(rdf(rdf_clean)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').

:- db_add_novel(user:prolog_file_type('tar.gz', archive)).
:- db_add_novel(user:prolog_file_type(dx,  dx  )).
:- db_add_novel(user:prolog_file_type(txt, text)).
:- db_add_novel(user:prolog_file_type(xml, xml )).

:- nodebug(el_script).

:- initialization(el_script).



el_script:-
  % This is needed for stage 4->5.
  %set_prolog_stack(local, limit(2*10**9)),
  ap(
    [process(xml2rdf),project(el),to('VoID',turtle)],
    [
      ap_stage(
        [from(input,'v20130401.dx',archive),to(_,v20130401,dx)],
        ap_extract_archive
      ),
      ap_stage([from(_,v20130401,dx)], to_small_files),
      ap_stage([], insert_newlines),
      ap_stage([to(_,big,xml)], ap_merge_into_one_file),
      ap_stage([from(_,big,xml),potential(2354560)], el_parse),
      ap_stage([], el_clean),
      ap_stage([to(output,'VoID',turtle)], assert_el_void)
    ]
  ).

assert_el_void(_Id, FromDir, ToFile):-
  G = 'VoID',
  assert_el_void(G, FromDir),
  rdf_save2(ToFile, [format(turtle),graph(G)]).

% Split into smaller files.
to_small_files(_Id, FromFile, ToDir):-
  split_into_smaller_files(FromFile, ToDir, temp_).

% Insert newlines in small files.
insert_newlines(_Id, FromDir, ToDir):-
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
        read_stream_to_codes(Stream, Codes),
        codes_replace2(Codes, [62,60], [62,10,60], NewCodes_),
        % It will sometimes happen that the cuttoff lies exactly
        % in between =|>|= and =|<|=.
        (
          first(NewCodes_, 60)
        ->
          NewCodes = [10|NewCodes_]
        ;
          NewCodes = NewCodes_
        ),
        atom_codes(NewAtom, NewCodes),
        atom_to_file(NewAtom, ToFile)
      ),
      (
        close(Stream),
        debug(el_script, 'File ~w split into lines.', [FromFile]),
        thread_success(ThreadAlias)
      )
    )
  ).

el_clean(_Id, FromDir, ToDir):-
  % Do this for every Turtle file in the from directory.
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
  maplist(el_clean(ToDir), FromFiles).

el_clean(ToDir, FromFile):-
  % Load and unload RDF.
  setup_call_cleanup(
    rdf_load2(FromFile, [format(turtle),graph(el)]),
    el_clean_(ToDir, FromFile),
    rdf_unload_graph(el)
  ).

el_clean_(ToDir, FromFile):-
  rdf_strip_literal([], ['-'], _S, el:huisnummer_toevoeging, el),
  
  % Store the results.
  file_name(FromFile, _FromDir, FileName, _FileExt),
  absolute_file_name(
    FileName,
    ToFile,
    [access(write),file_type(turtle),relative_to(ToDir)]
  ),
  rdf_save2(ToFile, [format(turtle),graph(el)]).

