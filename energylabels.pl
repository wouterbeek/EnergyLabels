:- module(
  energylabels,
  [
    energylabels_script/0
  ]
).

/** <module> ENERGYLABELS

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

# Fault in big to small files translation

~~~{.txt}
File =|postcode_25.xml|=, line  15.341.
File =|postcode_26.xml|=, line 764.433.
File =|postcode_28.xml|=, line  30.921.
File =|postcode_66.xml|=, line 432.811.
~~~

~~~{.xml}
</Pandcertificaat><Pandcertificaat>
<Pandcertificaat>
~~~

@author Wouter Beek
@see https://data.overheid.nl/data/dataset/energielabels-agentschap-nl
@tbd process_create does not form for `cat FROM > TO`.
@tbd Big memory profile for stage 2 or 3.
@version 2013/04, 2013/06
*/

:- use_module(dcg(dcg_ascii)).
:- use_module(dcg(dcg_date)).
:- use_module(dcg(dcg_generic)).
:- use_module(generics(atom_ext)).
:- use_module(generics(codes_ext)).
:- use_module(generics(db_ext)).
:- use_module(generics(list_ext)).
:- use_module(generics(meta_ext)).
:- use_module(generics(script_stage)).
:- use_module(library(archive)).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(process)).
:- use_module(library(readutil)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(settings)).
:- use_module(library(xpath)).
:- use_module(os(dir_ext)).
:- use_module(os(file_ext)).
:- use_module(os(io_ext)).
:- use_module(rdf(rdf_build)).
:- use_module(rdf(rdf_graph)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml)).
:- use_module(xml(xml_namespace)).
:- use_module(xml(xml_stream)).
:- use_module(xml(xml_to_rdf)).

:- dynamic(bag/1).

:- xml_register_namespace(hk, 'http://pilod-huiskluis.appspot.com/data/').

:- db_add_novel(user:prolog_file_type(gz, archive)).
:- db_add_novel(user:prolog_file_type(db, settings)).
:- db_add_novel(user:prolog_file_type(txt, text)).
:- db_add_novel(user:prolog_file_type(tmp, temporary)).
:- db_add_novel(user:prolog_file_type(ttl, turtle)).
:- db_add_novel(user:prolog_file_type(xml, xml)).

:- debug(energylabels).

:- setting(energylabels_graph, atom, el, 'The name of the energylabels graph.').
% This file must be readable.
:- setting(input_file_name, atom, v20130401, 'The original input file.').
% This directory must be readable.
:- setting(input_files_directory, atom, 'Input', 'The subdirectory for input files.').
% This directory must be writeable.
:- setting(output_files_directory, atom, 'Output', 'The subdirectory for output files.').
:- setting(temporary_file_prefix, atom, 'temp_', 'The prefix for temporary files.').



% Stage 0 (Input) -> Stage 1
copy_input_unarchived(From, To):-
  setting(input_file_name, FileName),
  absolute_file_name(
    FileName,
    FromFile,
    [access(read), extensions([dx]), relative_to(From)]
  ),
  absolute_file_name(
    FileName,
    ToFile,
    [access(write), extensions([dx]), relative_to(To)]
  ),
  safe_copy_file(FromFile, ToFile).
copy_input_archived(FromDir, ToDir):-
  setting(input_file_name, FileName1),
  file_name_extension(FileName1, dx, FileName2),
  absolute_file_name(
    FileName2,
    FromFile,
    [access(read), file_type(archive), relative_to(FromDir)]
  ),
  archive_extract(FromFile, ToDir, []).



% Stage 1 -> Stage 2 (Split into smaller files).
to_small_files(FromDir, ToDir):-
  % Open the copied version of the big file with no newlines.
  setting(input_file_name, FromName),
  absolute_file_name(
    FromName,
    FromFile,
    [access(read), extensions([dx]), relative_to(FromDir)]
  ),
  setting(temporary_file_prefix, Prefix),
  split_into_smaller_files(FromFile, ToDir, Prefix).



% Stage 2 -> Stage 3 (Insert newlines in small files).
insert_newlines(FromDir, ToDir):-
  % Add newlines to the small files.
  setting(temporary_file_prefix, Prefix),
  atomic_concat(Prefix, '*', RE0),
  directory_file_path(FromDir, RE0, RE),
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
          [access(write), file_type(text), relative_to(ToDir)]
        ),
        read_stream_to_codes(Stream, Codes),
        codes_replace2(Codes, [62,60], [62,10,60], NewCodes_),
        % It will sometimes happen that the cuttoff lies exactly
        % in between =|>|= and =|<|=.
        if_then_else(
          first(NewCodes_, 60),
          NewCodes = [10 | NewCodes_],
          NewCodes = NewCodes_
        ),
        atom_codes(NewAtom, NewCodes),
        atom_to_file(NewAtom, ToFile)
      ),
      (
        close(Stream),
        thread_success(ThreadAlias)
      )
    )
  ).



% Stage 3 -> Stage 4 (Put small files together into big one).
to_big_file(FromDir, ToDir):-
  setting(temporary_file_prefix, Prefix),
  atomic_concat(Prefix, '*', RE1),
  directory_file_path(FromDir, RE1, RE2),

  % Now create the big file which now has newlines.
  create_file(FromDir, big, xml, TempFile),
  merge_into_one_file(RE2, TempFile),
  create_file(ToDir, big, xml, ToFile),
  safe_move_file(TempFile, ToFile).



% Stage 4 -> Stage 5 (Convert the big XML file to an RDF file.
% Use XML streaming for this.)
xml_to_poscode(FromDir, ToDir):-
  % Open the input file on a read stream.
  absolute_file_name(
    big,
    FromFile,
    [access(read), file_type(xml), relative_to(FromDir)]
  ),
  setting(energylabels_graph, Graph),
  xml_stream(
    FromFile,
    'Pandcertificaat',
    process_postcode(Graph),
    store_postcodes(Graph, ToDir)
  ),
  absolute_file_name(
    postcodes,
    Turtle_File,
    [access(write), file_type(turtle), relative_to(ToDir)]
  ),
  rdf_save2(Turtle_File, [format(turtle), graph(Graph)]).

% The graph is also used as the XML namespace.
process_postcode(Graph, DOM1):-
  create_house(Graph, DOM1, House),
  Spec =.. ['Pandcertificaat', content],
  xpath(DOM1, //Spec, DOM2),
  attribute_value_pairs(Graph, Graph, House, DOM2).

store_postcodes(Graph, Dir):-
  flag(number_of_files, X, X + 1),
  format(atom(FileName), 'postcodes_~w', [X]),
  absolute_file_name(
    FileName,
    File,
    [access(write), file_type(turtle), relative_to(Dir)]
  ),
  rdf_save2(File, [format(turtle), graph(Graph)]),
  rdf_unload_graph(Graph).

/*
  assert_energyclass(Graph, DOM, House),
  assert_validity(Graph, DOM, House),
  assert_joules(Graph, DOM, House),
  assert_energy_prestationindex(Graph, DOM, House).
process_postcode(_Graph, DOM):-
  cowsay(DOM).
*/

create_house(Graph, DOM, House):-
  Spec1 =.. ['PandVanMeting_postcode', content],
  xpath_chk(DOM, //Spec1, [PostCode]),

  Spec2 =.. ['PandVanMeting_huisnummer', content],
  xpath_chk(DOM, //Spec2, [HouseNumber]),

  Spec3 =.. ['PandVanMeting_huisnummer_toev', content],
  xpath_chk(DOM, //Spec3, HouseNumberAddition),

  % A housenumber need not have an addition.
  (
    HouseNumberAddition == []
  ->
    FullHouseNumber = [PostCode, HouseNumber]
  ;
    HouseNumberAddition = [HouseNumberAddition0],
    % Additions need to be stripped of spaces and dashes.
    strip_atom([' ','-'], HouseNumberAddition0, HouseNumberAddition1)
  ->
    FullHouseNumber = [PostCode, HouseNumber, HouseNumberAddition1]
  ),

  % Create the IRI.
  atomic_list_concat(FullHouseNumber, '_', HouseName),
  rdf_global_id(Graph:HouseName, House),

  rdf_global_id(Graph:postcode, Predicate1),
  rdf_assert_literal(House, Predicate1, PostCode, Graph),

  rdf_global_id(Graph:house_number, Predicate2),
  atom_number(HouseNumber, HouseNumber_),
  rdf_assert_datatype(House, Predicate2, int, HouseNumber_, Graph),

  if_then(
    nonvar(HouseNumberAddition1),
    (
      rdf_global_id(Graph:house_number_addition, Predicate3),
      rdf_assert_literal(House, Predicate3, HouseNumberAddition1, Graph)
    )
  ).



postcode_cleaning(Graph):-
  forall(
    (
      rdf_literal(House, el:postcode, Postcode, Graph),
      rdf_datatype(House, el:house_number, Number, Graph)
    ),
    (
      rdfs_assert_individual(House, el:'House', Graph),
      rdf_retractall_literal(House, el:'PandVanMeting_postcode', _, Graph),
      rdf_retractall_literal(House, el:'PandVanMeting_huisnummer', _, Graph),
      rdf_retractall_literal(House, el:'PandVanMeting_huisnummer_toev', _, Graph),
      rdf_retractall_literal(House, el:'Pand_postcode', _, Graph),
      rdf_retractall_literal(House, el:'Pand_huisnummer', _, Graph),
      rdf_retractall_literal(House, el:'Pand_huisnummer_toev', _, Graph)
    )
  ),
  forall(
    rdf_retractall_literal(House, el:'Pand_cert_type', CertificationType, Graph),
    rdf_assert(House, el:certification_type, el:CertificationType, Graph)
  ),
  forall(
    rdf_retractall_literal(House, el:'Pand_plaats', Place1, Graph),
    (
      to_lower(Place1, Place2),
      rdf_assert(House, el:place, el:Place2, Graph)
    )
  ),
  forall(
    rdf_retractall_literal(House, el:'Pand_registratiedatum', RegistrationDate1, Graph),
    (
      dcg_phrase(date(_Lang, RegistrationDate2), RegistrationDate1),
      rdf_assert_datatype(House, el:registration_date, date, RegistrationDate2, Graph)
    )
  ),
  
  % Measurement
  forall(
    rdf_retractall(House, el:'Meting_geldig_tot', ValidDate1, Graph),
    (
      rdf_bnode(Measurement),
      rdf_assert(House, el:measurement, Measurement, Graph),
      dcg_phrase(date(_Lang, ValidDate2), ValidDate1),
      rdf_assert_datatype(Measurement, el:valid_until, date, ValidDate2, Graph)
      forall(
        rdf_retractall_literal(House, el:'PandVanMeting_energieklasse', EnergyClass, Graph),
        rdf_assert(Measurement, el:energyclass, el:EnergyClass)
      ),
      forall(
        (
          rdf_retractall_literal(House, el:'PandVanMeting_energieverbruikmj', Amount1, Graph),
          rdf_retractall_literal(House, el:'PandVanMeting_energieverbruiktype', Type, Graph)
        ),
        (
          rdf_bnode(Consumption),
          rdf_assert(Measurement, el:consumption, Consumption, Graph),
          rdf_assert(Consumption, el:type, el:Type, Graph),
          atom_number(Amount1, Amount2),
          rdf_assert_datatype(Consumption, el:energy_consumption, float, Amount2, Graph)
        )
      ),
      forall(
        rdf_retractall_literal(House, el:'PandVanMeting_opnamedatum', MeasurementDate1, Graph),
        (
          dcg_phrase(date(_Lang, MeasurementDate2), MeasurementDate1),
          rdf_assert_datatype(Measurement, el:measurement_date, MeasurementDate2, Graph)
        )
      ),
      forall(
        rdf_retractall_literal(House, el:'PandVanMeting_energieprestatieindex', Index1, Graph),
        (
          atom_number(Index1, Index2),
          rdf_assert_datatype(Measurement, el:prestation_index, float, Index2, Graph)
        )
      ),
      forall(
        retractall(House, el:'Afmeldnummer', Afmeldnummer1, Graph),
        (
          atom_number(Afmeldnummer1, Afmeldnummer2),
          rdf_assert_datatype(Measurement, el:afmeldnummer, Afmeldnummer2, Graph)
        )
      )
    )
  ).

init:-
  setting(energylabels_graph, Graph),
  xml_register_namespace(
    Graph,
    'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl#'
  ),
  create_personal_subdirectory('Data', Data),
  db_add_novel(user:file_search_path(data, Data)),
  create_personal_subdirectory('Data'('Input'), Input),
  db_add_novel(user:file_search_path(input, Input)),
  create_personal_subdirectory('Data'('Output'), Output),
  db_add_novel(user:file_search_path(output, Output)).

energylabels_script:-
  set_prolog_stack(global, limit(2*10**9)),
  init,
  stage0, stage1, stage2, stage3, stage4.
stage0:-
  init,
  script_stage(0, copy_input_unarchived).
stage1:-
  init,
  script_stage(1, to_small_files).
stage2:-
  init,
  script_stage(2, insert_newlines).
stage3:-
  init,
  script_stage(3, to_big_file).
stage4:-
  init,
  script_stage(4, xml_to_poscode).

