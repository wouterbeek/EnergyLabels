:- module(
  energylabel,
  [
    add_newlines/0,
    create_rdf/0,
    create_small_files/0,
    script/0
  ]
).

/** <module> Test

Just testing...

---+ Strange DOM

==
[ element('Pandcertificaat',
	  [],
	  [ element('PandVanMeting_postcode',[],['2581VX']),
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
	  ]),
  element('Pandcertificaat',[],[])
]
==

---+ Fault in big to small files translation

File =|postcode_25.xml|=, line  15.341.
File =|postcode_26.xml|=, line 764.433.
File =|postcode_28.xml|=, line  30.921.
File =|postcode_66.xml|=, line 432.811.

==
</Pandcertificaat><Pandcertificaat>
<Pandcertificaat>
==



---+ TODO

    1. Put parses into PostCode files (grouped by the first two digits).
    2. Put properties into separate RDF files.

@author Wouter Beek
@version 2013/04, 2013/06
*/

:- use_module(dcg(dcg_ascii)).
:- use_module(dcg(dcg_generic)).
:- use_module(generics(atom_ext)).
:- use_module(generics(db_ext)).
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
:- db_add_novel(user:prolog_file_type(tmp, temporary)).
:- db_add_novel(user:prolog_file_type(ttl, turtle)).
:- db_add_novel(user:prolog_file_type(xml, xml)).

:- debug(energylabels).



% Stage 1
add_newlines:-
  % Open a copied version of the big file with no newlines.
  input_file_name(FromName),
  absolute_file_name(in(FromName), From, [access(read), extensions([dx])]),
  create_nested_directory(data(stage1)),
  db_add_novel(user:file_search_path(stage1, data(stage1))),
  absolute_file_name(
    stage1(FromName),
    To,
    [access(write), extensions([dx])]
  ),
  safe_copy_file(From, To),
  absolute_file_name(
    stage1(.),
    ToDir,
    [access(read), file_type(directory)]
  ),
  % Split the big file by byte size into small files.
  % (We cannot split on the number of lines since the file is one big line.)
  process_create(
    path(split),
    ['--bytes=1m', '-d', '--suffix-length=4', To, 'temp_'],
    [cwd(ToDir)]
  ),
  
  % Add newlines to the small files.
  directory_file_path(ToDir, 'temp_*', RE),
  expand_file_name(RE, Files),
  forall(
    member(File, Files),
    setup_call_cleanup(
      open(File, read, Stream),
      (
        file_name_extension(File, txt, FileExt),
        read_stream_to_codes(Stream, Codes),
        phrase(dcg_replace("><", (">", line_feed, "<")), Codes, NewCodes),
        atom_codes(NewAtom, NewCodes),
        atom_to_file(NewAtom, FileExt)
      ),
      close(Stream)
    )
  ),
  
  % Now create the big file which now has newlines.
  process_create(path(cat), ['temp_*', '>', 'big.xml'], [cwd(ToDir)]).

collect_rdf:-
  absolute_file_name(
    project(out),
    OutputDirectory,
    [access(write), file_type(directory)]
  ),
  format(atom(RE), '~w/postcode_*.ttl', [OutputDirectory]),
  expand_file_name(RE, OutputFiles),
  set_prolog_stack(global, limit(2*10**9)),
  forall(
    member(OutputFile, OutputFiles),
    (
      rdf_load2(OutputFile, [graph(small)]),
      rdf_graph_copy(small, big)
    )
  ),
  absolute_file_name(
    out('EnergyLabels'),
    BigFile,
    [access(write), file_type(turtle)]
  ),
  rdf_save2(BigFile, [format(turtle), graph(big)]).

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

%% create_rdf is det.
% We assume that the input files are the postcode-separated XML files.
% Use create_small_files/0 to create these XML files.

create_rdf:-
  % Add the closing root tag to the postcode XML documents.
  absolute_file_name(
    project(out),
    Out,
    [access(write), file_type(directory)]
  ),
  format(atom(RE), '~w/postcode_*.xml', [Out]),
  expand_file_name(RE, OutFiles),
  set_prolog_stack(global, limit(2*10**9)),
  forall(
    member(OutFile, OutFiles),
    (
      file_to_xml(OutFile, DOM),
      % Extract RDF from XML DOM.
      dom_to_rdf(DOM),
      % Save the RDF graph to file.
      once(file_type_alternative(OutputFile, turtle, RDF_File)),
      rdf_save2(RDF_File, [format(turtle), graph(hk)]),
      rdf_unload_graph(hk)
    )
  ).

create_rdf_thread:-
  thread_create(create_rdf, _ThreadId, []).

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

dom_to_rdf(DOM):-
  memberchk(element('Pandcertificaten', [], Entries), DOM),
  forall(
    member(element('Pandcertificaat', [], Entry), Entries),
    entry_to_rdf(Entry)
  ).

entry_to_rdf(Entry):-
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
  rdf_global_id(hk:HouseName, House),
  memberchk(
    element('PandVanMeting_energieklasse', [], [EnergyClassName]),
    Entry
  ),
  rdf_global_id(hk:EnergyClassName, EnergyClass),
  rdf_assert(House, hk:energyclass, EnergyClass, hk).
entry_to_rdf(Entry):-
  gtrace, %DEB
  write(Entry).

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
    PostcodeFile,
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
  absolute_file_name(
    energylabels(settings),
    File,
    [access(read), file_type(settings)]
  ),
  load_settings(File).

script:-
  init,
  add_newlines,
  create_rdf,
  collect_rdf.

