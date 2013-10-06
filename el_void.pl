:- module(
  el_void,
  [
    assert_el_void/2 % +Graph:atom
                     % +BaseDirectory:atom
  ]
).

/** <module> EnergyLabels VoID

Asserts the VoID description of the energy labels dataset.

@author Wouter Beek
@version 2013/10
*/

:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db)).
:- use_module(os(datetime_ext)).
:- use_module(rdf(rdf_build)).
:- use_module(rdfs(rdfs_build)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(dcterms, 'http://purl.org/dc/terms/').
:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').
:- xml_register_namespace(foaf, 'http://xmlns.com/foaf/0.1/').
:- xml_register_namespace(format, 'http://www.w3.org/ns/formats/').
:- xml_register_namespace(void, 'http://rdfs.org/ns/void#').



assert_el_void(G, Dir):-
  rdf_global_id(el:'EnergyLabels', DD),
  rdf_assert_individual(DD, void:'DatasetDescription', G),

  % Contributors.
  rdf_assert_individual(el:'WouterBeek', foaf:'Person', G),
  rdfs_assert_label(el:'WouterBeek', nl, 'Wouter Beek', G),
  rdf_assert(el:'WouterBeek', foaf:mbox, 'mailto:me@wouterbeek.com', G),
  rdf_assert(DD, dcterms:contributor, el:'WouterBeek', G),

  % Creation time.
  get_time(POSIX_TS),
  posix_timestamp_to_xsd_dateTime(POSIX_TS, XSD_DT),
  rdf_assert_datatype(DD, dcterms:created, dateTime, XSD_DT, G),

  % Title
  rdf_assert_literal(DD, dcterms:title, nl,
    'Energielabels - Agentschap NL', G),

  % Description
  rdf_assert_literal(DD, dcterms:description, nl,
    'Ruwe data van afgegeven energielabels van gebouwen wordt in het kader\c
     van Apps voor Nederland ter beschikking gesteld. Per energielabel is de\c
     beschikbare informatie: gebouwinformatie; dit betreft de plaats,\c
     postcode, het huisnummer, eventuele huisnummertoevoeging en een vrij\c
     veld dat gebruikt kan worden voor extra gebouw identificatie met verder\c
     het woningtype of hoofdgebruiksfunctie; het certificaatnummer, de datum\c
     van opname en registratie door de adviseur, de labelwaarde\c
     (energie-index) en de labelklasse, bron van de opname; het berekende\c
     gebouwgebonden energieverbruik in MJ, en indien beschikbaar m3 gas,\c
     kWh elektrisch, MJ warmte en het aantal m2 van het gebouw.', G),

  % Vocabulary namespaces.
  xml_current_namespace(el, EL_NS),
  rdf_assert(DD, void:vocabulary, EL_NS, G),

  % VoID Feature: RDF serialization format.
  rdf_assert(DD, void:feature, format:'Turtle', G),

  % Web pages.
  rdf_assert(DD, foaf:homepage,
    'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl', G),

  % Example resource
  rdf_assert(DD, void:exampleResource, el:'Building', G),

  forall(
    between(1, 9, N),
    assert_el_void_ds(G, DD, N, Dir)
  ).

assert_el_void_ds(G, DD, N, Dir):-
  format(atom(DS_Name), 'el_~w', [N]),
  rdf_global_id(el:DS_Name, DS),
  absolute_file_name(
    DS_Name,
    DS_File,
    [access(read),file_type(turtle),relative_to(Dir)]
  ),
  rdf_assert_individual(DS, void:'Dataset', G),

  % Title.
  rdf_assert_literal(DS, dcterms:title, en, DS_Name, G),

  % Description.
  format(atom(Desc), 'Energylabel data for postcodes starting with ~w.', [N]),
  rdf_assert_literal(DS, dcterms:description, en, Desc, G),

  % Main class.
  rdf_assert(DS, void:class, el:'Building', G),

  % Datadump location.
  relative_file_name(DS_File, Dir, DS_RelFile),
  rdf_assert(DS, void:dataDump, DS_RelFile, G),

  % Relate dataset to datadescription.
  rdf_assert(DD, void:subset, DS, G).
