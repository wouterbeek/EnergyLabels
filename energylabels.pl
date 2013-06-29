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

# TODO

  1. Put parses into PostCode files (grouped by the first two digits).
  2. Put properties into separate RDF files.

@author Wouter Beek
@version 2013/04, 2013/06
*/

:- use_module(dcg(dcg_ascii)).
:- use_module(dcg(dcg_generic)).
:- use_module(generics(atom_ext)).
:- use_module(generics(db_ext)).
:- use_module(generics(script_stage)).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(process)).
:- use_module(library(readutil)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(settings)).
:- use_module(os(dir_ext)).
:- use_module(os(file_ext)).
:- use_module(os(io_ext)).
:- use_module(rdf(rdf_graph)).
:- use_module(rdf(rdf_serial)).
:- use_module(xml(xml)).
:- use_module(xml(xml_namespace)).

:- dynamic(bag/1).

:- xml_register_namespace(hk, 'http://pilod-huiskluis.appspot.com/data/').

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
copy_input(From, To):-
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
insert_newlines(Dir, _):-
  % Add newlines to the small files.
  setting(temporary_file_prefix, Prefix),
  atomic_concat(Prefix, '*', RE0),
  directory_file_path(Dir, RE0, RE),
  expand_file_name(RE, FileNames),
  forall(
    member(FileName, FileNames),
    setup_call_cleanup(
      open(FileName, read, Stream),
      (
        file_name_type(FileName, text, File),
        read_stream_to_codes(Stream, Codes),
        phrase(dcg_replace("><", (">", line_feed, "<")), Codes, NewCodes),
        atom_codes(NewAtom, NewCodes),
        atom_to_file(NewAtom, File)
      ),
      close(Stream)
    )
  ).

% Stage 3 -> Stage 4 (Put small files together into big one).
to_big_file(FromDir, ToDir):-
  setting(temporary_file_prefix, Prefix),
  atomic_concat(Prefix, '*', RE0),
  directory_file_path(FromDir, RE0, RE),

  % Now create the big file which now has newlines.
  create_file(ToDir, big, xml, ToFile),
  merge_into_one_file(RE, ToFile).

% Stage 4 -> Stage 5 (Load XML files into RDF).
create_rdf(Dir, _):-
  % Add the closing root tag to the postcode XML documents.
  file_name_type('postcode_*', xml, FileName),
  absolute_file_name(FileName, RE, [relative_to(Dir)]),
  expand_file_name(RE, XML_Files),
  setting(energylabels_graph, Graph),
  forall(
    member(XML_File, XML_Files),
    (
      file_to_xml(XML_File, DOM),
      % Extract RDF from XML DOM.
      dom_to_rdf(Graph, DOM),
      % Save the RDF graph to file.
      once(file_type_alternative(XML_File, turtle,Turtle_File)),
      rdf_save2(Turtle_File, [format(turtle), graph(Graph)]),
      rdf_unload_graph(Graph)
    )
  ).

dom_to_rdf(Graph, DOM):-
  memberchk(element('Pandcertificaten', [], Entries), DOM),
  forall(
    member(element('Pandcertificaat', [], Entry), Entries),
    entry_to_rdf(Graph, Entry)
  ).

% The graph is also used as the XML namespace.
entry_to_rdf(Graph, Entry):-
  memberchk(element('PandVanMeting_postcode', [], [PostCode]), Entry),
  memberchk(element('PandVanMeting_huisnummer', [], [HouseNumber1]), Entry),
  memberchk(
    element('PandVanMeting_huisnummer_toev', [], HouseNumberAddition1),
    Entry
  ),
  (
    HouseNumberAddition1 == []
  ->
    HouseNumber2 = HouseNumber1
  ;
    HouseNumberAddition1 = [HouseNumberAddition2],
    strip_atom([' ','-'], HouseNumberAddition2, HouseNumberAddition3),
    format(atom(HouseNumber2), '~w-~w', [HouseNumber1, HouseNumberAddition3])
  ),
  format(
    atom(HouseName),
    '~w/~w',
    [PostCode, HouseNumber2, HouseNumberAddition3]
  ),
  rdf_global_id(Graph:HouseName, House),
  memberchk(
    element('PandVanMeting_energieklasse', [], [EnergyClassName]),
    Entry
  ),
  rdf_global_id(Graph:EnergyClassName, EnergyClass),
  rdf_global_id(Graph:energyclass, Predicate),
  rdf_assert(House, Predicate, EnergyClass, Graph).
entry_to_rdf(_Graph, Entry):-
  gtrace, %DEB
  write(Entry).

% Stage 5 -> Stage 6 ().
collect_rdf(FromDir, ToDir):-
  % Load all turtle files.
  create_file(FromDir, 'postcode_*', turtle, RE),
  expand_file_name(RE, TurtleFiles),
  findall(
    Graph,
    (
      member(TurtleFile, TurtleFiles),
      file_name(TurtleFile, _Dir, Graph, _Ext),
      rdf_load2(TurtleFile, [format(turtle), graph(Graph)])
    ),
    Graphs
  ),

  % Merge all RDF graphs.
  rdf_graph_merge(Graphs, big),

  % Store the merged RDF graph in a single Turtle file.
  ToFileName = postcodes,
  absolute_file_name(
    ToFileName,
    ToFile,
    [access(write), file_type(turtle), relative_to(ToDir)]
  ),
  rdf_save2(ToFile, [format(turtle), graph(ToFileName)]).





create_postcode(MainStream):-
  at_end_of_stream(MainStream), !.
create_postcode(MainStream):-
  read_entry(MainStream, Entry), !,
  absolute_file_name(
    data(copy),
    File,
    [access(write), file_type(temporary)]
  ),
  setup_call_cleanup(
    open(File, write, TemporaryStream1),
    format(TemporaryStream1, '~w', [Entry]),
    close(TemporaryStream1)
  ),
  setup_call_cleanup(
    open(File, read, TemporaryStream2),
    stream_to_xml(TemporaryStream2, DOM),
    close(TemporaryStream2)
  ),
  safe_delete_file(File),
  store_entry(Entry, DOM),
  create_postcode(MainStream).

%% create_small_files is det.
% We assume that the input file has newlines.
% Use add_newlines/0 in case the input file does not yet contain these.

% Stage 2
create_small_files:-
  % Open the input file on a read stream.
  absolute_file_name(
    stage1(big),
    From,
    [access(read), file_type(xml)]
  ),
  setup_call_cleanup(
    open(From, read, FromStream),
    % Create XML files for each postcode.
    create_postcode(FromStream),
    close(FromStream)
  ),

  % Add the closing root tag to the postcode XML documents.
  absolute_file_name(
    out(.),
    OutDir,
    [access(write), file_type(directory)]
  ),
  directory_file_path(OutDir, 'postcode_*.xml', RE),
  expand_file_name(RE, OutFiles),
  forall(
    member(OutFile, OutFiles),
    setup_call_cleanup(
      open(OutFile, append, OutStream, []),
      format(OutStream, '</Pandcertificaten>\n', []),
      (
        flush_output(OutStream),
        close(OutStream)
      )
    )
  ).

load_rdf:-
  absolute_file_name(
    project('EnergyLabels'),
    File,
    [access(read), file_type(turtle)]
  ),
  rdf_load2(File, [format(turtle), graph(hk)]).

read_entry(Stream, Entry):-
  read_entry(Stream, out, Entry).

read_entry(Stream, out, Entry):-
  peek_atom(Stream, '<Pandcertificaat>'),
  !,
  read_line_to_codes(Stream, Codes1),
  read_entry(Stream, in, Codes2),
  append([Codes1, [10], Codes2, [10]], Codes3),
  atom_codes(Entry, Codes3).
read_entry(Stream, out, Entry):-
  read_line_to_codes(Stream, Codes),
  atom_codes(Atom, Codes),
  debug(energylabels, 'Skipping: ~w', [Atom]),
  read_entry(Stream, out, Entry).
read_entry(Stream, in, Codes):-
  peek_atom(Stream, '</Pandcertificaat>'),
  !,
  read_line_to_codes(Stream, Codes).
read_entry(Stream, in, Codes3):-
  read_line_to_codes(Stream, Codes1),
  read_entry(Stream, in, Codes2),
  append([Codes1, [10], Codes2], Codes3).

store_dom([element('Pandcertificaat', [], DOM)]):-
  memberchk(element('PandVanMeting_postcode', [], [PostCode1]), DOM),
%(PostCode1 == '6463BC' -> gtrace ; true), %DEB
%format(user, '~w\n', [PostCode1]), %DEB
  atom_codes(PostCode1, PostCode2),
  append([One, Two], _, PostCode2),
  number_codes(Index, [One, Two]),
  (
    bag(Index)
  ->
    true
  ;
    assert(bag(Index))
  ),
  flag(Index, ID, ID + 1),
  forall(
    member(element(Property, [], _Value), DOM),
    assert(property(Property))
  ).

store_entry(Entry, DOM1):-
  % Sometimes there is more than one root in the entity.
  memberchk(element('Pandcertificaat', [], DOM2), DOM1),

  % We save the entity in the file that is named using the first two
  % characters of the postal code.
  memberchk(element('PandVanMeting_postcode', [], [PostCode1]), DOM2),
  atom_chars(PostCode1, PostCode2),
  append([One, Two], _, PostCode2),
  format(atom(PostcodeFileName), 'postcode_~w~w', [One, Two]),
  absolute_file_name(
    out(PostcodeFileName),
    File,
    [access(write), extensions([xml])]
  ),

  % If the file does not yet exist we add the root element.
  setup_call_cleanup(
    open(File, append, Stream, []),
    (
      if_then(exists_file(File), format(Stream, '<Pandcertificaten>\n', [])),
      % Add the data entity.
      format(Stream, '~w', [Entry])
    ),
    (
      flush_output(Stream),
      close(Stream)
    )
  ).

init:-
  %absolute_file_name(
  %  energylabels(settings),
  %  File,
  %  [access(read), file_type(settings)]
  %),
  %load_settings(File),
  create_personal_subdirectory('Data', Data),
  db_add_novel(user:file_search_path(data, Data)),
  create_personal_subdirectory('Data'('Input'), Input),
  db_add_novel(user:file_search_path(input, Input)),
  create_personal_subdirectory('Data'('Output'), Output),
  db_add_novel(user:file_search_path(output, Output)).

energylabels_script:-
  set_prolog_stack(global, limit(2*10**9)),
gtrace,
  init,
  script_stage(0, copy_input),
  script_stage(1, to_small_files),
  script_stage(2, insert_newlines), % Stay in the same dir.
  script_stage(2, to_big_file),
  script_stage(4, create_rdf), % Stay in the same dir.
  script_stage(4, collect_rdf).

