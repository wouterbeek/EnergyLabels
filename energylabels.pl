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
@version 2013/04, 2013/06-2013/07
*/

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
:- use_module(library(readutil)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(settings)).
:- use_module(library(xpath)).
:- use_module(os(dir_ext)).
:- use_module(os(file_ext)).
:- use_module(os(io_ext)).
:- use_module(rdf(rdf_build)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml_namespace)).
:- use_module(xml(xml_stream)).
:- use_module(xml(xml_to_rdf)).

:- dynamic(bag/1).

:- xml_register_namespace(
  el,
  'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl#'
).
:- xml_register_namespace(hk, 'http://pilod-huiskluis.appspot.com/data/').

:- db_add_novel(user:prolog_file_type(gz, archive)).
:- db_add_novel(user:prolog_file_type(db, settings)).
:- db_add_novel(user:prolog_file_type(txt, text)).
:- db_add_novel(user:prolog_file_type(tmp, temporary)).
:- db_add_novel(user:prolog_file_type(ttl, turtle)).
:- db_add_novel(user:prolog_file_type(xml, xml)).

:- debug(energylabels).

:- setting(
  energylabels_graph,
  atom,
  el,
  'The name of the energylabels graph.'
).
% This file must be readable.
:- setting(
  input_file_name,
  atom,
  v20130401,
  'The original input file.'
).
% This directory must be readable.
:- setting(
  input_files_directory,
  atom,
  'Input',
  'The subdirectory for input files.'
).
% This directory must be writeable.
:- setting(
  output_files_directory,
  atom,
  'Output',
  'The subdirectory for output files.'
).
:- setting(
  temporary_file_prefix,
  atom,
  'temp_',
  'The prefix for temporary files.'
).



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
  create_file(ToDir, big, xml, ToFile),
  merge_into_one_file(FromDir, ToFile).



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
    RDF_File,
    [access(write), file_type(turtle), relative_to(ToDir)]
  ),
  rdf_save2(RDF_File, [format(turtle), graph(Graph)]).

% The graph is also used as the XML namespace.
process_postcode(Graph, DOM0):-
  Spec =.. ['Pandcertificaat', content],
  xpath_chk(DOM0, //Spec, DOM1),

  % CERTIFICATE
  flag(certificate, CId, CId + 1),
  format(atom(CName), 'c_~w', [CId]),
  rdf_global_id(el:CName, Certificate),
  rdfs_assert_individual(Certificate, el:'EP_Certificate', Graph),

  % Required postal code.
  selectchk(element('PandVanMeting_postcode', _, [Postcode]), DOM1, DOM2),
  rdf_assert_literal(Certificate, el:postcode, Postcode, Graph),

  % Required house number.
  selectchk(element('PandVanMeting_huisnummer', _, [HouseNumber1]), DOM2, DOM3),
  atom_number(HouseNumber1, HouseNumber2),
  rdf_assert_datatype(Certificate, el:housenumber, int, HouseNumber2, Graph),

  % Optional addition to house number.
  selectchk(element('PandVanMeting_huisnummer_toev', _, Addition1), DOM3, DOM5),
  if_then(
    Addition1 = [Addition2],
    rdf_assert_literal(Certificate, el:housenumber_addition, Addition2, Graph)
  ),

  % Certificate's house building code
  selectchk(element('PandVanMeting_gebouwcode', _, BuildingCode1), DOM5, DOM6),
  (
    BuildingCode1 = [BuildingCode2]
  ->
    atom_number(BuildingCode2, BuildingCode3),
    rdf_assert_datatype(Certificate, el:building_code, int, BuildingCode3, Graph)
  ;
    BuildingCode1 = []
  ),

  % Certificate's inclusion date
  selectchk(element('PandVanMeting_opnamedatum', _, [SurveyDate1]), DOM6, DOM7),
  dcg_phrase(date(_, SurveyDate2), SurveyDate1),
  rdf_assert_datatype(Certificate, el:survey_date, date, SurveyDate2, Graph),

  % Certificate's measurement valid until.
  selectchk(element('Meting_geldig_tot', _, [ValidityDate1]), DOM7, DOM8),
  dcg_phrase(date(_, ValidityDate2), ValidityDate1),
  rdf_assert_datatype(Certificate, el:measurement_valid_until, date, ValidityDate2, Graph),

  % Certificate's number?

  % Certificate's calculation type?

  % Certificate's energy prestation index
  selectchk(element('PandVanMeting_energieprestatieindex', _, [Index1]), DOM8, DOM11),
  atom_number(Index1, Index2),
  rdf_assert_datatype(Certificate, el:prestation_index, float, Index2, Graph),

  % Energy class
  selectchk(element('PandVanMeting_energieklasse', _, [EnergyClass1]), DOM11, DOM12),
  rdf_global_id(el:EnergyClass1, EnergyClass2),
  uri_iri(EnergyClass2, EnergyClass3),
  rdf_assert(Certificate, el:energyclass, EnergyClass3, Graph),
  rdfs_assert_individual(EnergyClass3, el:'EnergyClass', Graph),

  % Certificate's energy consumption
  rdf_bnode(Consumption),
  rdf_assert(Certificate, el:energy_consumption, Consumption, Graph),
  rdfs_assert_individual(Consumption, el:'EnergyConsumption', Graph),
  selectchk(element('PandVanMeting_energieverbruikmj', _, [Amount1]), DOM12, DOM13),
  atom_number(Amount1, Amount2),
  rdf_assert_datatype(Consumption, el:amount, float, Amount2, Graph),

  % Measurement amount
  selectchk(element('PandVanMeting_energieverbruiktype', _, [Type1]), DOM13, DOM14),
  rdf_global_id(el:Type1, Type2),
  uri_iri(Type2, Type3),
  rdf_assert(Consumption, el:type, Type3, Graph),
  rdfs_assert_individual(Type3, el:'EnergyUsageType', Graph),

  DOM20 = DOM14,

  % BUILDING
  flag(building, BId, BId + 1),
  format(atom(BName), 'b_~w', [BId]),
  rdf_global_id(el:BName, Building),
  rdfs_assert_individual(Building, el:'Building', Graph),
  rdf_assert(Building, el:has_certificate, Certificate, Graph),

  % Building's postal code
  selectchk(element('Pand_postcode', _, [Postcode10]), DOM20, DOM21),
  rdf_assert_literal(Building, el:postcode, Postcode10, Graph),

  % Building's house number
  selectchk(element('Pand_huisnummer', _, [HouseNumber11]), DOM21, DOM22),
  atom_number(HouseNumber11, HouseNumber12),
  rdf_assert_datatype(Building, el:house_number, int, HouseNumber12, Graph),

  % Building's house number addition
  selectchk(element('Pand_huisnummer_toev', _, HouseNumberAddition10), DOM22, DOM23),
  rdf_assert_literal(Building, el:house_number_addition, HouseNumberAddition10, Graph),

  % Building's place
  selectchk(element('Pand_plaats', _, [Place1]), DOM23, DOM24),
  downcase_atom(Place1, Place2),
  rdf_global_id(el:Place2, Place3),
  uri_iri(Place3, Place4),
  rdf_assert(Building, el:place, Place4, Graph),
  rdfs_assert_individual(Place4, el:'Place', Graph),

  % Building's certification type. Either 'house' or 'utility'.
  % @tbd Use switch for this.
  % @tbd What is this property's domain?
  selectchk(element('Pand_cert_type', _, [CertificationType1]), DOM24, DOM25),
  (
    CertificationType1 == 'W'
  ->
    rdf_global_id(el:house, CertificationType2)
  ;
    CertificationType1 == 'U'
  ->
    rdf_global_id(el:utility, CertificationType2)
  ),
  uri_iri(CertificationType2, CertificationType3),
  rdf_assert(Building, el:certification_type, CertificationType3, Graph),
  rdfs_assert_individual(CertificationType3, el:'CertificationType', Graph),

  % Building's registration date
  selectchk(element('Pand_registratiedatum', _, [RegistrationDate1]), DOM25, DOM26),
  dcg_phrase(date(_, RegistrationDate2), RegistrationDate1),
  rdf_assert_datatype(Building, el:registration_date, date, RegistrationDate2, Graph),

  % Undocumented 'Pand_gebouwcode'
  selectchk(element('Pand_gebouwcode', _, BuildingCode1), DOM26, DOM27),
  (
    BuildingCode1 = [BuildingCode2]
  ->
    rdf_assert_literal(Building, el:building_code, BuildingCode2, Graph)
  ;
    BuildingCode1 = []
  ),

  % Undocumented 'Afmeldnummer'
  selectchk(element('Afmeldnummer', _, [Afmeldnummer1]), DOM27, DOM30),
  atom_number(Afmeldnummer1, Afmeldnummer2),
  rdf_assert_datatype(Building, el:afmeldnummer, int, Afmeldnummer2, Graph),

  DOM30 == [], !.
assert_entry(Graph, House, DOM):-
  cowsay(DOM),
  gtrace, %WB
  assert_entry(Graph, House, DOM).

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

init:-
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

