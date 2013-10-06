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
@version 2013/04, 2013/06-2013/07, 2013/09-2013/10
*/

:- use_module(el(el_parse)). % Used in script_stage/2.
:- use_module(el(el_void)).
:- use_module(generics(codes_ext)).
:- use_module(generics(db_ext)).
:- use_module(generics(list_ext)).
:- use_module(generics(script_ext)).
:- use_module(generics(thread_ext)).
:- use_module(library(archive)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(lists)).
:- use_module(library(readutil)).
:- use_module(os(file_ext)).
:- use_module(os(io_ext)).
:- use_module(rdf(rdf_serial)).

:- db_add_novel(user:prolog_file_type(txt, text)).
:- db_add_novel(user:prolog_file_type(xml, xml )).

:- nodebug(el_script).

:- initialization(el_script).



el_script:-
  % This is needed for stage 4->5.
  set_prolog_stack(local, limit(2*10**9)),
  Process = 'EnergyLabels',
  script(
    [to_file('VoID.ttl')],
    Process,
    [
      stage(
        [from_file('v20130401.dx.tar.gz'),to_file('v20130401.dx')],
        copy_input
      ),
      stage([from_file('v20130401.dx'),to_file('temp_0000')], to_small_files),
      stage([to_file('temp_0000.txt')], insert_newlines),
      stage([to_file('big.xml')], to_big_file),
      stage(
        [from_file('big.xml'),potential(2354560),to_file('el_1.ttl')],
        el_parse
      ),
      stage([], assert_el_void)
    ]
  ).

assert_el_void(_Id, FromDir, _ToDir):-
  assert_el_void('VoID', FromDir),
  absolute_file_name(
    'VoID',
    ToFile,
    [access(write),file_type(turtle),relative_to(FromDir)]
  ),
  rdf_save2(ToFile, [format(turtle),graph('VoID')]).

% Stage 0 (Input) -> Stage 1
copy_input(_Id, FromFile, ToFile):-
  directory_file_path(ToDir, _, ToFile),
  archive_extract(FromFile, ToDir, []).

% Stage 1 -> Stage 2 (Split into smaller files).
to_small_files(_Id, FromFile, ToFile):-
  directory_file_path(ToDir, _, ToFile),
  split_into_smaller_files(FromFile, ToDir, temp_).

% Stage 2 -> Stage 3 (Insert newlines in small files).
insert_newlines(_Id, FromDir, ToFile):-
  % Add newlines to the small files.
  directory_file_path(FromDir, 'temp_*', RE),
  expand_file_name(RE, FromFiles),
  directory_file_path(ToDir, _, ToFile),
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

% Stage 3 -> Stage 4 (Put small files together into big one).
to_big_file(Id, FromDir, ToFile):-
  merge_into_one_file(Id, FromDir, ToFile).
