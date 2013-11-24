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
@version 2013/10-2013/11
*/

:- use_module(library(filesex)).
:- use_module(library(semweb/rdf_db)).
:- use_module(os(datetime_ext)).
:- use_module(rdf(rdf_build)).
:- use_module(rdf(rdf_datatype)).
:- use_module(rdf(rdf_lit_build)).
:- use_module(rdfs(rdfs_label_build)).
:- use_module(void(void_stat)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(dbpedia, 'http://dbpedia.org/resource/').
:- xml_register_namespace(dcterms, 'http://purl.org/dc/terms/').
:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').
:- xml_register_namespace(foaf, 'http://xmlns.com/foaf/0.1/').
:- xml_register_namespace(formats, 'http://www.w3.org/ns/formats/').
:- xml_register_namespace(void, 'http://rdfs.org/ns/void#').
:- xml_register_namespace(xsd, 'http://www.w3.org/2001/XMLSchema#').



assert_el_void(G, Dir):-
  rdf_global_id(el:'EnergyLabels', DD),

  %%%% dcterms:contributor

  % dcterms:created
  get_time(POSIX_TS),
  posix_timestamp_to_xsd_dateTime(POSIX_TS, XSD_DT),
  rdf_assert_datatype(DD, dcterms:created, xsd:date, XSD_DT, G),

  % dcterms:creator
  assert_foaf_wouterbeek(G, WB),
  rdf_assert(DD, dcterms:creator, WB, G),

  % dcterms:date
  % A point or period of time associated with an event in the life-cycle
  % of the resource.
  %%%%rdf_assert_datatype(DD, dcterms:date, xsd:date, XSD_DT, G),

  % dcterms:description
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

  % dcterms:issued
  % Date of formal issuance (e.g., publication) of the dataset.
  %%%%rdf_assert_datatype(DD, dcterms:issued, xsd:date, XSD_DT, G),

  % dcterms:license
  rdf_assert(DD, dcterms:license,
    'http://www.opendatacommons.org/licenses/pddl/', G),

  % dcterms:modified
  % See module VOID_STAT.

  % dcterms:publisher
  rdf_assert(DD, dcterms:publisher, WB, G),

  % dcterms:source
  assert_energylabels_original_dataset(G, ODS),
  rdf_assert(DD, dcterms:source, ODS, G),

  % dcterms:title
  rdf_assert_literal(DD, dcterms:title, nl, 'Energielabels - Agentschap NL', G),

  % foaf:homepage
  rdf_assert(DD, foaf:homepage,
    'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl', G),

  % foaf:page
  rdf_assert(DD, foaf:page, 'https://github.com/wouterbeek/EnergyLabels', G),

  % rdf:type
  rdf_assert_individual(DD, void:'DatasetDescription', G),

  % rdfs:label
  rdfs_assert_label(DD, en, 'Energylabels'),
  rdfs_assert_label(DD, nl, 'Energielabels'),

  % void:datadump

  % void:documents
  rdf_assert_datatype(DD, void:documents, xsd:integer, 10, G),

  % void:exampleResource
  % @tbd A specific building.
  % @tbd A specific certificate.
  %%%%rdf_assert(DD, void:exampleResource, el:, G),

  % void:feature
  rdf_assert(DD, void:feature, formats:'Turtle', G),

  % void:openSearchDescription

  % void:rootResource
  rdf_assert(DD, void:rootResource, el:'Building', G),
  rdf_assert(DD, void:rootResource, el:'Certificate', G),

  % void:sparqlEndpoint

  % void:subject
  rdf_assert(DD, void:subject, dbpedia:'European_Union_energy_label', G),

  % void:subset
  forall(
    between(1, 9, N),
    assert_el_void_dataset(G, DD, N, Dir)
  ),

  % void:uriLookupEndpoint

  % void:uriRegexPattern

  % void:uriSpace
  rdf_assert_literal(DD, void:uriSpace,
    'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/', G),

  % void:vocabulary
  % @tbd Add `elv`
  %%%%xml_current_namespace(elv, ELV_NS),
  %%%%rdf_assert(DD, void:vocabulary, ELV_NS, G),
  xml_current_namespace(rdf, RDF_NS),
  rdf_assert(DD, void:vocabulary, RDF_NS, G),
  xml_current_namespace(rdfs, RDFS_NS),
  rdf_assert(DD, void:vocabulary, RDFS_NS, G),
  xml_current_namespace(xsd, XSD_NS),
  rdf_assert(DD, void:vocabulary, XSD_NS, G),

  % wv:declaration rdfs:Literal
  % wv:norms foaf:Document
  % wv:waiver foaf:Document

  true.

assert_el_void_dataset(G, DD, N, Dir):-
  format(atom(DS_Name), 'el_~w', [N]),
  rdf_global_id(el:DS_Name, DS),

  % dcterms:description.
  format(atom(Desc), 'Energylabel data for postcodes starting with ~w.', [N]),
  rdf_assert_literal(DS, dcterms:description, en, Desc, G),

  % dcterms:title
  rdf_assert_literal(DS, dcterms:title, en, DS_Name, G),

  % rdf:type
  rdf_assert_individual(DS, void:'Dataset', G),

  % void:class
  rdf_assert(DS, void:class, el:'Building', G),

  % void:dataDump
  absolute_file_name(
    DS_Name,
    DS_File,
    [access(read),file_type(turtle),relative_to(Dir)]
  ),
  relative_file_name(DS_File, Dir, DS_RelFile),
  rdf_assert(DS, void:dataDump, DS_RelFile, G),

  % void:subset
  rdf_assert(DD, void:subset, DS, G),
gtrace,
  void_assert_statistics(G, DS, DS_File).

assert_energylabels_original_dataset(G, ODS):-
  rdf_global_id(el:'OriginalDataset', ODS),
  assert_foaf_agentschapnl(G, ANL),
  rdf_assert(ODS, dcterms:creator, ANL, G),
  rdf_assert(ODS, dcterms:publisher, ANL, G).

assert_foaf_agentschapnl(G, ANL):-
  rdf_global_id(el:'AgentSchapNL', ANL),
  % foaf:homepage
  rdf_assert(ANL, foaf:homepage, 'http://www.agentschapnl.nl', G),
  % rdf:type
  rdf_assert_individual(ANL, foaf:'Organization', G),
  % rdfs:label
  rdfs_assert_label(ANL, nl, 'Agentschap NL', G).

assert_foaf_wouterbeek(G, WB):-
  rdf_global_id(el:'WouterBeek', WB),
  % foaf:firstName
  rdf_assert_literal(WB, foaf:firstName, 'Wouter', G),
  % foaf:lastName
  rdf_assert_literal(WB, foaf:lastName, 'Beek', G),
  % foaf:mbox
  rdf_assert(WB, foaf:mbox, 'mailto:me@wouterbeek.com', G),
  % rdf:type
  rdf_assert_individual(WB, foaf:'Person', G),
  % rdfs:label
  rdfs_assert_label(WB, nl, 'Wouter Beek', G).

