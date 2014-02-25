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
@version 2013/04, 2013/06-2013/07, 2013/09-2013/12, 2014/02
*/

:- use_module(ap(ap)).
:- use_module(ap(ap_stat)).
:- use_module(el(el_parse)). % Used in ap_stage/2.
:- use_module(el(el_void)).
:- use_module(generics(codes_ext)).
:- use_module(generics(list_ext)).
:- use_module(generics(thread_ext)).
:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(lists)).
:- use_module(library(readutil)).
:- use_module(library(semweb/rdf_db)).
:- use_module(os(archive_ext)).
:- use_module(os(dir_ext)).
:- use_module(os(file_ext)).
:- use_module(os(io_ext)).
:- use_module(rdf(rdf_clean)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').

:- initialization(el_script).



el_script:-
  ap(
    [reset(true),to('VoID',turtle)],
    el,
    [
      ap_stage([from(input,'v20130401.dx',archive)], extract_archive),
      ap_stage([from(_,'v20130401.dx',_)], to_small_files),
      ap_stage([], insert_newlines),
      ap_stage([to(_,big,xml)], merge_into_one_file),
      ap_stage([from(_,big,xml),stat_lag(100)], el_parse),
      ap_stage([], el_clean),
      ap_stage([to(output,'VoID',turtle)], assert_el_void)
    ],
    _
  ).

assert_el_void(FromDir, ToFile):-
  Graph = 'VoID',
  assert_el_void(Graph, FromDir),
  rdf_save([format(turtle)], Graph, ToFile).

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
  rdf_setup_call_cleanup(
    [],
    FromFile,
    rdf_strip_literal([answer('A')], ['-'], _S, P),
    [format(turtle)],
    ToFile
  ),
  
  % STATS
  ap_stage_tick.

