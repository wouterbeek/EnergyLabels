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
  assert(user:file_search_path(data, el('Data'))),
  assert(user:file_search_path(output, data('Output'))),
  load_pgc(el).

load_pgc(_Project):-
  user:file_search_path(pgc, _Spec), !.
load_pgc(Project):-
  Spec =.. [Project,'PGC'],
  assert(user:file_search_path(pgc, Spec)),
  load_or_debug(pgc).

load_or_debug(Project):-
  predicate_property(debug_project, visible), !
  Spec =.. [Project,debug],
  ensure_loaded(Spec).
load_or_debug(Project):-
  Spec =.. [Project,load],
  ensure_loaded(Spec).

