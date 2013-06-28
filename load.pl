project_name('EnergyLabels').

load_energylabels:-
  source_file(load_energylabels, ThisFile),
  file_directory_name(ThisFile, ThisDirectory),
  % By asserting the STCN directory as mnemonic =project= we can refer
  % to this from within the PGC (which does not 'know' that STCN is
  % using it).
  assert(user:file_search_path(project, ThisDirectory)),
  assert(user:file_search_path(energylabels, ThisDirectory)),
  
  assert(user:file_search_path(data, energylabels('Data'))),
  
  % Load the PGC.
  assert(user:file_search_path(pgc, energylabels('PGC'))),
  (
    predicate_property(debug, visible)
  ->
    ensure_loaded(pgc(debug))
  ;
    ensure_loaded(pgc(load))
  ),
  
  % STCN main module.
  ensure_loaded(energylabels(energylabels)).
:- load_energylabels.

