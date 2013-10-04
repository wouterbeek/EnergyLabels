:- module(energylabels, []).

/** <module> Energylabels

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
@version 2013/04, 2013/06-2013/07, 2013/09-2013/10
*/

:- use_module(energylabels(energylabels_parse)).
:- use_module(generics(codes_ext)).
:- use_module(generics(db_ext)).
:- use_module(generics(list_ext)).
:- use_module(generics(script_stage)).
:- use_module(generics(thread_ext)).
:- use_module(library(archive)).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(readutil)).
:- use_module(os(dir_ext)).
:- use_module(os(file_ext)).
:- use_module(os(io_ext)).

:- db_add_novel(user:prolog_file_type(txt, text)).
:- db_add_novel(user:prolog_file_type(xml, xml )).

:- debug(energylabels).

:- initialization(script).



script:-
  script_begin,
  script_stage(0, copy_input_archived),
  script_stage(1, to_small_files),
  script_stage(2, insert_newlines),
  script_stage(3, to_big_file),
  thread_create(script_stage(4, energylabels_parse), _Id, []),
  script_end.

% Stage 0 (Input) -> Stage 1
copy_input_archived(FromDir, ToDir):-
  absolute_file_name(
    'v20130401.dx.tar.gz',
    FromFile,
    [access(read),relative_to(FromDir)]
  ),
  archive_extract(FromFile, ToDir, []).

% Stage 1 -> Stage 2 (Split into smaller files).
to_small_files(FromDir, ToDir):-
  % Open the copied version of the big file with no newlines.
  absolute_file_name(
    v20130401,
    FromFile,
    [access(read),extensions([dx]),relative_to(FromDir)]
  ),
  split_into_smaller_files(FromFile, ToDir, temp_).

% Stage 2 -> Stage 3 (Insert newlines in small files).
insert_newlines(FromDir, ToDir):-
  % Add newlines to the small files.
  directory_file_path(FromDir, 'temp_*', RE),
  expand_file_name(RE, FromFiles),
  run_on_sublists(FromFiles, energylabels:insert_newlines_worker(ToDir)).

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
        debug(energylabels, 'File ~w split into lines.', [FromFile]),
        thread_success(ThreadAlias)
      )
    )
  ).

% Stage 3 -> Stage 4 (Put small files together into big one).
to_big_file(FromDir, ToDir):-
  create_file(ToDir, big, xml, ToFile),
  merge_into_one_file(FromDir, ToFile).

