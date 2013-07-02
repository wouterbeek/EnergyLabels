:- module(
  multi_parse,
  [
    multi_parse/1, % +Number:integer
    multi_parse/2, % +FromDir:atom
                   % +ToDir:atom
    multi_parse/3, % +FromDir:atom
                   % +ToDir:atom
                   % +Number:integer
    multi_parse/4 % +FromDir:atom
                  % +ToDir:atom
                  % +Min:integer
                  % +Max:integer
  ]
).

/** <module> DOUBLE_PARSE

Double parse for energy labels.

@author Wouter Beek
@version 2013/07
*/

:- use_module(dcg(dcg_date)).
:- use_module(dcg(dcg_generic)).
:- use_module(generics(db_ext)).
:- use_module(generics(cowspeak)).
:- use_module(generics(meta_ext)).
:- use_module(library(lists)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(xpath)).
:- use_module(rdf(rdf_build)).
:- use_module(rdf(rdf_serial)).
:- use_module(rdfs(rdfs_build)).
:- use_module(xml(xml_namespace)).
:- use_module(xml(xml_stream)).

:- xml_register_namespace(
  el,
  'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl#'
).
:- setting(
  energylabels_graph,
  atom,
  el,
  'The name of the energylabels graph.'
).

:- db_add_novel(user:prolog_file_type(ttl, turtle)).
:- db_add_novel(user:prolog_file_type(xml, xml)).



multi_parse(Number):-
  absolute_file_name(
    data(stage_4),
    FromDir,
    [access(read), file_type(directory)]
  ),
  absolute_file_name(
    data(stage_5),
    ToDir,
    [access(write), file_type(directory)]
  ),
  thread_create(multi_parse(FromDir, ToDir, Number), _Id, []).

multi_parse(FromDir, ToDir):-
  multi_parse(FromDir, ToDir, 1, 19).

multi_parse(FromDir, ToDir, Number):-
  multi_parse(FromDir, ToDir, Number, Number).

multi_parse(FromDir, ToDir, Min, Max):-
  set_prolog_stack(global, limit(2*10**9)),
  absolute_file_name(
    big,
    FromFile,
    [access(read), file_type(xml), relative_to(FromDir)]
  ),
  setting(energylabels_graph, Graph),
  forall(
    between(Min, Max, Number),
    multi_parse1(FromFile, Graph, Number, ToDir)
  ).

multi_parse1(FromFile, Graph, Number, ToDir):-
  reset_parse,
  xml_stream(
    FromFile,
    'Pandcertificaat',
    multi_parse1(Graph, Number, Name)
  ),
  absolute_file_name(
    Name,
    RDF_File,
    [access(write), file_type(turtle), relative_to(ToDir)]
  ),
  rdf_save2(RDF_File, [format(turtle), graph(Graph)]),
  cowsay('Parse #~w is done!', [Number]),
  rdf_unload_graph(Graph).

multi_parse1(Graph, Number, Name, DOM1):-
  Spec =.. ['Pandcertificaat', content],
  xpath_chk(DOM1, //Spec, DOM2),
  multi_parse2(Graph, Number, Name, DOM2).

multi_parse2(Graph, 1, 'CertificatePostcode', DOM1):-
  get_certificate(Certificate),
  % Required postal code.
  selectchk(element('PandVanMeting_postcode', _, [Postcode]), DOM1, _DOM2),
  rdf_assert_literal(Certificate, el:postcode, Postcode, Graph).
multi_parse2(Graph, 2, 'CertificateHouseNumber', DOM1):-
  get_certificate(Certificate),
  % Required house number.
  selectchk(element('PandVanMeting_huisnummer', _, [HouseNumber1]), DOM1, _DOM2),
  atom_number(HouseNumber1, HouseNumber2),
  rdf_assert_datatype(Certificate, el:housenumber, int, HouseNumber2, Graph).
multi_parse2(Graph, 3, 'CertificateHouseNumberAddition', DOM1):-
  get_certificate(Certificate),
  % Optional addition to house number.
  selectchk(element('PandVanMeting_huisnummer_toev', _, Addition1), DOM1, _DOM2),
  if_then(
    Addition1 = [Addition2],
    rdf_assert_literal(Certificate, el:housenumber_addition, Addition2, Graph)
  ).
multi_parse2(Graph, 4, 'CertificateBuildingCode', DOM1):-
  get_certificate(Certificate),
  % Certificate's house building code
  selectchk(element('PandVanMeting_gebouwcode', _, BuildingCode1), DOM1, _DOM2),
  (
    BuildingCode1 = [BuildingCode2]
  ->
    atom_number(BuildingCode2, BuildingCode3),
    rdf_assert_datatype(Certificate, el:building_code, int, BuildingCode3, Graph)
  ;
    BuildingCode1 = []
  ).
multi_parse2(Graph, 5, 'CertificateSurveyDate', DOM1):-
  get_certificate(Certificate),
  % Certificate's inclusion date
  selectchk(element('PandVanMeting_opnamedatum', _, [SurveyDate1]), DOM1, _DOM2),
  dcg_phrase(date(_, SurveyDate2), SurveyDate1),
  rdf_assert_datatype(Certificate, el:survey_date, date, SurveyDate2, Graph).
multi_parse2(Graph, 6, 'CertificateMeasurementValidUntil', DOM1):-
  get_certificate(Certificate),
  % Certificate's measurement valid until.
  selectchk(element('Meting_geldig_tot', _, [ValidityDate1]), DOM1, _DOM2),
  dcg_phrase(date(_, ValidityDate2), ValidityDate1),
  rdf_assert_datatype(Certificate, el:measurement_valid_until, date, ValidityDate2, Graph).
multi_parse2(Graph, 7, 'CertificatePrestationIndex', DOM1):-
  get_certificate(Certificate),
  % Certificate's energy prestation index
  selectchk(element('PandVanMeting_energieprestatieindex', _, [Index1]), DOM1, _DOM2),
  atom_number(Index1, Index2),
  rdf_assert_datatype(Certificate, el:prestation_index, float, Index2, Graph).

multi_parse2(Graph, 8, 'CertificateEnergyClass', DOM1):-
  get_certificate(Certificate),
  % Energy class
  selectchk(element('PandVanMeting_energieklasse', _, [EnergyClass1]), DOM1, _DOM2),
  rdf_global_id(el:EnergyClass1, EnergyClass2),
  uri_iri(EnergyClass2, EnergyClass3),
  rdf_assert(Certificate, el:energyclass, EnergyClass3, Graph),
  rdfs_assert_individual(EnergyClass3, el:'EnergyClass', Graph).
multi_parse2(Graph, 9, 'CertificateEnergyConsumption', DOM1):-
  get_certificate(Certificate),
  % Certificate's energy consumption
  rdf_bnode(Consumption),
  rdf_assert(Certificate, el:energy_consumption, Consumption, Graph),
  rdfs_assert_individual(Consumption, el:'EnergyConsumption', Graph),
  selectchk(element('PandVanMeting_energieverbruikmj', _, [Amount1]), DOM1, DOM2),
  atom_number(Amount1, Amount2),
  rdf_assert_datatype(Consumption, el:amount, float, Amount2, Graph),

  % Measurement amount of certificate's energy consumption
  selectchk(element('PandVanMeting_energieverbruiktype', _, [Type1]), DOM2, _DOM3),
  rdf_global_id(el:Type1, Type2),
  uri_iri(Type2, Type3),
  rdf_assert(Consumption, el:type, Type3, Graph),
  rdfs_assert_individual(Type3, el:'EnergyUsageType', Graph).
multi_parse2(Graph, 10, 'BuildingType', DOM1):-
  get_building(Building),
  % Building's certification type. Either 'house' or 'utility'.
  % @tbd Use switch for this.
  % @tbd What is this property's domain?
  selectchk(element('Pand_cert_type', _, [CertificationType1]), DOM1, _DOM2),
  (
    CertificationType1 == 'W'
  ->
    rdf_global_id(el:'House', CertificationType2)
  ;
    CertificationType1 == 'U'
  ->
    rdf_global_id(el:'Utility', CertificationType2)
  ),
  uri_iri(CertificationType2, CertificationType3),
  rdfs_assert_individual(Building, CertificationType3, Graph).
multi_parse2(Graph, 11, 'BuildingPostcode', DOM1):-
  get_building(Building),
  % Building's postal code
  selectchk(element('Pand_postcode', _, [Postcode10]), DOM1, _DOM2),
  rdf_assert_literal(Building, el:postcode, Postcode10, Graph).
multi_parse2(Graph, 12, 'BuildingHouseNumber', DOM1):-
  get_building(Building),
  % Building's house number
  selectchk(element('Pand_huisnummer', _, [HouseNumber11]), DOM1, _DOM2),
  atom_number(HouseNumber11, HouseNumber12),
  rdf_assert_datatype(Building, el:house_number, int, HouseNumber12, Graph).
multi_parse2(Graph, 13, 'BuildingHouseNumberAddition', DOM1):-
  get_building(Building),
  % Building's house number addition
  selectchk(element('Pand_huisnummer_toev', _, HouseNumberAddition10), DOM1, _DOM2),
  rdf_assert_literal(Building, el:house_number_addition, HouseNumberAddition10, Graph).
multi_parse2(Graph, 14, 'BuildingPlace', DOM1):-
  get_building(Building),
  % Building's place
  selectchk(element('Pand_plaats', _, [Place1]), DOM1, _DOM2),
  downcase_atom(Place1, Place2),
  rdf_global_id(el:Place2, Place3),
  uri_iri(Place3, Place4),
  rdf_assert(Building, el:place, Place4, Graph),
  rdfs_assert_individual(Place4, el:'Place', Graph).
multi_parse2(Graph, 15, 'BuildingRegistrationDate', DOM1):-
  get_building(Building),
  % Building's registration date
  selectchk(element('Pand_registratiedatum', _, [RegistrationDate1]), DOM1, _DOM2),
  dcg_phrase(date(_, RegistrationDate2), RegistrationDate1),
  rdf_assert_datatype(Building, el:registration_date, date, RegistrationDate2, Graph).
multi_parse2(Graph, 16, 'BuildingCode', DOM1):-
  get_building(Building),
  % Undocumented 'Pand_gebouwcode'
  selectchk(element('Pand_gebouwcode', _, BuildingCode1), DOM1, _DOM2),
  (
    BuildingCode1 = [BuildingCode2]
  ->
    rdf_assert_literal(Building, el:building_code, BuildingCode2, Graph)
  ;
    BuildingCode1 = []
  ).
multi_parse2(Graph, 17, 'BuildingAfmeldnummer', DOM1):-
  get_building(Building),
  % Undocumented 'Afmeldnummer'
  selectchk(element('Afmeldnummer', _, [Afmeldnummer1]), DOM1, _DOM2),
  atom_number(Afmeldnummer1, Afmeldnummer2),
  rdf_assert_datatype(Building, el:afmeldnummer, int, Afmeldnummer2, Graph).
multi_parse2(Graph, 18, 'BuildingCertificate', _DOM1):-
  get_building(Building),
  get_certificate(Certificate),
  rdf_assert(Building, el:has_certificate, Certificate, Graph).
multi_parse2(Graph, 19, 'CertificateType', _DOM1):-
  get_certificate(Certificate),
  rdfs_assert_individual(Certificate, el:'EP_Certificate', Graph).

reset_parse:-
  flag(certificate, _CId, 0),
  flag(building, _BId, 0),
  set_prolog_stack(global, limit(2*10**9)).

get_building(Building):-
  % BUILDING
  flag(building, BId, BId + 1),
  format(atom(BName), 'b_~w', [BId]),
  rdf_global_id(el:BName, Building).

get_certificate(Certificate):-
  % CERTIFICATE
  flag(certificate, CId, CId + 1),
  format(atom(CName), 'c_~w', [CId]),
  rdf_global_id(el:CName, Certificate).

