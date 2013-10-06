% The load file for the EnergyLabels project.

project_name('EnergyLabels').

:- initialization(load_energylabels).

load_energylabels:-
  source_file(load_energylabels, ThisFile),
  file_directory_name(ThisFile, ThisDirectory),
  % By asserting the STCN directory as mnemonic =project= we can refer
  % to this from within the PGC (which does not 'know' that STCN is
  % using it).
  assert(user:file_search_path(project, ThisDirectory)),
  assert(user:file_search_path(el, ThisDirectory)),
  
  % Load the PGC.
  assert(user:file_search_path(pgc, el('PGC'))),
  (
    predicate_property(debug_project, visible)
  ->
    ensure_loaded(pgc(debug))
  ;
    ensure_loaded(pgc(load))
  ),
  
  ensure_loaded(el(el_script)).

