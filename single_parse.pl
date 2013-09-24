:- module(
  single_parse,
  [
    single_parse/2 % +FromDirectory:atom
                   % +ToDirectory:atom
  ]
).

/** <module> Single parse

Process all energylabels in a single parse.
On my machine this does not work with 8 GB of RAM...

@author Wouter Beek
@versionm 2013/06-2013/07, 2013/09
*/

:- use_module(dcg(dcg_date)).
:- use_module(dcg(dcg_generic)).
:- use_module(generics(meta_ext)).
:- use_module(library(debug)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(settings)).
:- use_module(library(xpath)).
:- use_module(os(datetime_ext)).
:- use_module(os(file_ext)).
:- use_module(rdf(rdf_build)).
:- use_module(rdf(rdf_lit)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml_stream)).

:- setting(energylabels_graph, atom, el, 'The name of the energylabels graph.').

:- debug(single_parse).



%! single_parse(+FromDirectory:atom, +ToDirectory:atom) is det.

single_parse(FromDir, ToDir):-
  % Open the input file on a read stream.
  absolute_file_name(
    big,
    FromFile,
    [access(read),file_type(xml),relative_to(FromDir)]
  ),
  setting(energylabels_graph, G),
  xml_stream(
    FromFile,
    'Pandcertificaat',
    process_postcode(G),
    temp_save(G, ToDir)
  ),
  absolute_file_name(
    postcodes,
    RDF_File,
    [access(write),file_type(turtle),relative_to(ToDir)]
  ),
  rdf_save2(RDF_File, [format(turtle),graph(G)]).

process_postcode(G, DOM0):-
  Spec =.. ['Pandcertificaat', content],
  xpath_chk(DOM0, //Spec, DOM1),

  % Required postal code.
  selectchk(element('PandVanMeting_postcode', _, [Postcode]), DOM1, DOM2),
  process_postcode(G, DOM2, Postcode).

% AMSTERDAM
process_postcode(G, DOM, Postcode):-
  sub_atom(Postcode, 0, 2, _, '10'), !,

(
  flag(postcode, Id, Id + 1),
  debug(single_parse, '[~w] Adding postcode: ~w', [Id,Postcode]),

  % CERTIFICATE
  flag(certificate, CId, CId + 1),
  format(atom(CName), 'c_~w', [CId]),
  rdf_global_id(el:CName, Certificate),
  rdf_assert_individual(Certificate, el:'EP_Certificate', G),

  rdf_assert_literal(Certificate, el:postcode, Postcode, G),

  % Required house number.
  selectchk(element('PandVanMeting_huisnummer', _, [HouseNumber1]), DOM, DOM3),
  atom_number(HouseNumber1, HouseNumber2),
  rdf_assert_datatype(Certificate, el:housenumber, integer, HouseNumber2, G),

  % Optional addition to house number.
  selectchk(element('PandVanMeting_huisnummer_toev', _, Addition1), DOM3, DOM51),
  (
    Addition1 = [Addition2]
  ->
    rdf_assert_literal(Certificate, el:housenumber_addition, Addition2, G)
  ;
    Addition1 = []
  ),

  (
    selectchk(element('PandVanMeting_energieverbruikelektriciteit', _, V11), DOM51, DOM52),
    V11 = [V12]
  ->
    atom_number(V12, V13),
    rdf_assert_datatype(Certificate, el:electricity, decimal, V13, G)
  ;
    DOM52 = DOM51
  ),

  (
    selectchk(element('PandVanMeting_energieverbruikgas', _, V21), DOM52, DOM53),
    V21 = [V22]
  ->
    atom_number(V22, V23),
    rdf_assert_datatype(Certificate, el:gas, decimal, V23, G)
  ;
    DOM53 = DOM52
  ),

  (
    selectchk(element('PandVanMeting_energieverbruikwarmte', _, V31), DOM53, DOM54),
    V31 = [V32]
  ->
    atom_number(V32, V33),
    rdf_assert_datatype(Certificate, el:warmth, decimal, V33, G)
  ;
    DOM54 = DOM53
  ),

  (
    selectchk(element('PandVanMeting_energieverbruikco2', _, V41), DOM54, DOM55),
    V41 = [V42]
  ->
    atom_number(V42, V43),
    rdf_assert_datatype(Certificate, el:co2, decimal, V43, G)
  ;
    DOM55 = DOM54
  ),

  % Certificate's house building code
  selectchk(element('PandVanMeting_gebouwcode', _, BuildingCode11), DOM55, DOM6),
  (
    BuildingCode11 = [BuildingCode12]
  ->
    rdf_assert_literal(Certificate, el:building_code, BuildingCode12, G)
  ;
    BuildingCode11 = []
  ),

  % Certificate's inclusion date
  selectchk(element('PandVanMeting_opnamedatum', _, [SurveyDate1]), DOM6, DOM71),
  dcg_phrase(date(_, SurveyDate2), SurveyDate1),
  rdf_assert_datatype(Certificate, el:survey_date, date, SurveyDate2, G),

  (
    selectchk(element('PandVanMeting_opnameblauwdruk', _, V51), DOM71, DOM72),
    V51 = [V52]
  ->
    atom_number(V52, V53),
    rdf_assert_datatype(Certificate, el:blueprint, integer, V53, G)
  ;
    DOM72 = DOM71
  ),

  (
    selectchk(element('PandVanMeting_opnameobservatie', _, V61), DOM72, DOM73),
    V61 = [V62]
  ->
    atom_number(V62, V63),
    rdf_assert_datatype(Certificate, el:observation, integer, V63, G)
  ;
    DOM73 = DOM72
  ),

  (
    selectchk(element('PandVanMeting_opnameeigenaarinformatie', _, V71), DOM73, DOM74),
    V71 = [V72]
  ->
    atom_number(V72, V73),
    rdf_assert_datatype(Certificate, el:owner_information, integer, V73, G)
  ;
    DOM74 = DOM73
  ),

  % Certificate's measurement valid until.
  selectchk(element('Meting_geldig_tot', _, [ValidityDate1]), DOM74, DOM8),
  dcg_phrase(date(_, ValidityDate2), ValidityDate1),
  rdf_assert_datatype(Certificate, el:measurement_valid_until, date, ValidityDate2, G),

  % Certificate's number?

  % Certificate's calculation type?

  % Certificate's energy prestation index
  selectchk(element('PandVanMeting_energieprestatieindex', _, [Index1]), DOM8, DOM11),
  atom_number(Index1, Index2),
  rdf_assert_datatype(Certificate, el:prestation_index, decimal, Index2, G),

  % Energy class
  selectchk(element('PandVanMeting_energieklasse', _, [EnergyClass1]), DOM11, DOM12),
  rdf_global_id(el:EnergyClass1, EnergyClass2),
  uri_iri(EnergyClass2, EnergyClass3),
  rdf_assert(Certificate, el:energyclass, EnergyClass3, G),
  rdf_assert_individual(EnergyClass3, el:'EnergyClass', G),

  % Certificate's energy consumption
  rdf_bnode(Consumption),
  rdf_assert(Certificate, el:energy_consumption, Consumption, G),
  rdf_assert_individual(Consumption, el:'EnergyConsumption', G),
  selectchk(element('PandVanMeting_energieverbruikmj', _, [Amount1]), DOM12, DOM13),
  atom_number(Amount1, Amount2),
  rdf_assert_datatype(Consumption, el:amount, decimal, Amount2, G),

  % Measurement amount of certificate's energy consumption
  selectchk(element('PandVanMeting_energieverbruiktype', _, [Type1]), DOM13, DOM14),
  rdf_global_id(el:Type1, Type2),
  uri_iri(Type2, Type3),
  rdf_assert_individual(Consumption, Type3, G),
  rdf_assert_individual(Type3, el:'EnergyUsageType', G),

  % BUILDING
  flag(building, BId, BId + 1),
  format(atom(BName), 'b_~w', [BId]),
  rdf_global_id(el:BName, Building),
  rdf_assert(Building, el:has_certificate, Certificate, G),

  % Building's certification type. Either 'house' or 'utility'.
  % @tbd Use switch for this.
  % @tbd What is this property's domain?
  selectchk(element('Pand_cert_type', _, [CertificationType1]), DOM14, DOM15),
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
  rdf_assert_individual(Building, CertificationType3, G),

  % REDUCE SIZE: CERTIFICATION TYPE IMPLICIT IN HIERARCHY.
  rdf_assert(Building, el:certification_type, CertificationType3, G),
  rdf_assert_individual(CertificationType3, el:'CertificationType', G),
  rdf_assert_individual(Building, el:'Building', G),

  % Building's postal code
  selectchk(element('Pand_postcode', _, [Postcode10]), DOM15, DOM21),
  rdf_assert_literal(Building, el:postcode, Postcode10, G),

  % Building's house number
  selectchk(element('Pand_huisnummer', _, [HouseNumber11]), DOM21, DOM22),
  atom_number(HouseNumber11, HouseNumber12),
  rdf_assert_datatype(Building, el:house_number, integer, HouseNumber12, G),

  % Building's house number addition
  selectchk(element('Pand_huisnummer_toev', _, HouseNumberAddition10), DOM22, DOM23),
  (
    HouseNumberAddition10 = [HouseNumberAddition100]
  ->
    rdf_assert_literal(Building, el:house_number_addition, HouseNumberAddition100, G)
  ;
    HouseNumberAddition10 = []
  ),

  % Building's place
  selectchk(element('Pand_plaats', _, Place1), DOM23, DOM24),
  (
     Place1 = [Place2]
  ->
    downcase_atom(Place2, Place3),
    rdf_global_id(el:Place3, Place4),
    uri_iri(Place4, Place5),
    rdf_assert(Building, el:place, Place5, G),
    rdf_assert_individual(Place5, el:'Place', G)
  ;
    Place1 = []
  ),

  % Building's registration date
  selectchk(element('Pand_registratiedatum', _, [RegistrationDate1]), DOM24, DOM26),
  dcg_phrase(date(_, RegistrationDate2), RegistrationDate1),
  rdf_assert_datatype(Building, el:registration_date, date, RegistrationDate2, G),

  % REDUCE SIZE: CONSIDERED A LESS IMPORTANT PROPERTY.
  % Undocumented 'Pand_gebouwcode'
  selectchk(element('Pand_gebouwcode', _, BuildingCode21), DOM26, DOM27),
  (
    BuildingCode21 = [BuildingCode22]
  ->
    rdf_assert_literal(Building, el:building_code, BuildingCode22, G)
  ;
    BuildingCode21 = []
  ),

  % REDUCE SIZE: CONSIDERED A LESS IMPORTANT PROPERTY.
  % Undocumented 'Afmeldnummer'
  selectchk(element('Afmeldnummer', _, [Afmeldnummer1]), DOM27, DOM30),
  atom_number(Afmeldnummer1, Afmeldnummer2),
  rdf_assert_datatype(Building, el:afmeldnummer, integer, Afmeldnummer2, G),

  DOM30 == [], !
;
  gtrace,
  process_postcode(G, DOM, Postcode)
).
process_postcode(_G, _DOM, _Postcode).

temp_save(G, Dir):-
  current_time(Base),
  create_file(Dir, Base, turtle, File),
  thread_create(rdf_save2(File, [graph(G),format(turtle)]), _, []).

