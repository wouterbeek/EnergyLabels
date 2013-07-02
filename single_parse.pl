:- module(
  single_parse,
  [
    single_parse/2 % +FromDir:atom
                   % +ToDir:atom
  ]
).

/** <module> SINGLE_PARSE

Process all energylabels in a single parse.
On my machine this does not work 7.8 GB of RAM.

@author Wouter Beek
@versionm 2013/06-2013/07
*/



%! single_parse(+FromDir:atom, +ToDir:atom) is det.

single_parse(FromDir, ToDir):-
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

  % Measurement amount of certificate's energy consumption
  selectchk(element('PandVanMeting_energieverbruiktype', _, [Type1]), DOM13, DOM14),
  rdf_global_id(el:Type1, Type2),
  uri_iri(Type2, Type3),
  rdf_assert(Consumption, el:type, Type3, Graph),
  rdfs_assert_individual(Type3, el:'EnergyUsageType', Graph),

  % BUILDING
  flag(building, BId, BId + 1),
  format(atom(BName), 'b_~w', [BId]),
  rdf_global_id(el:BName, Building),
  rdf_assert(Building, el:has_certificate, Certificate, Graph),

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
  rdfs_assert_individual(Building, CertificationType3, Graph),
% REDUCE SIZE: CERTIFICATION TYPE IMPLICIT IN HIERARCHY.
%  rdf_assert(Building, el:certification_type, CertificationType3, Graph),
%  rdfs_assert_individual(CertificationType3, el:'CertificationType', Graph),
%  rdfs_assert_individual(Building, el:'Building', Graph),
  
  % Building's postal code
  selectchk(element('Pand_postcode', _, [Postcode10]), DOM15, DOM21),
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

  % Building's registration date
  selectchk(element('Pand_registratiedatum', _, [RegistrationDate1]), DOM24, DOM26),
  dcg_phrase(date(_, RegistrationDate2), RegistrationDate1),
  rdf_assert_datatype(Building, el:registration_date, date, RegistrationDate2, Graph),

% REDUCE SIZE: CONSIDERED A LESS IMPORTANT PROPERTY.
%  % Undocumented 'Pand_gebouwcode'
%  selectchk(element('Pand_gebouwcode', _, BuildingCode1), DOM26, DOM27),
%  (
%    BuildingCode1 = [BuildingCode2]
%  ->
%    rdf_assert_literal(Building, el:building_code, BuildingCode2, Graph)
%  ;
%    BuildingCode1 = []
%  ),

% REDUCE SIZE: CONSIDERED A LESS IMPORTANT PROPERTY.
%  % Undocumented 'Afmeldnummer'
%  selectchk(element('Afmeldnummer', _, [Afmeldnummer1]), DOM27, DOM30),
%  atom_number(Afmeldnummer1, Afmeldnummer2),
%  rdf_assert_datatype(Building, el:afmeldnummer, int, Afmeldnummer2, Graph),

  DOM26 == [], !.

store_postcodes(Graph, Dir):-
  format(atom(FileName), 'postcodes_~w', [X]),
  absolute_file_name(
    FileName,
    File,
    [access(write), file_type(turtle), relative_to(Dir)]
  ),
  rdf_save2(File, [format(turtle), graph(Graph)]),
  rdf_unload_graph(Graph).

