% The load file for the EnergyLabels project.

:- multifile(user:project_name/1).
user:project_name('EnergyLabels').

:- initialization(load_el).

load_el:-
  % Entry point.
  source_file(load_el, ThisFile),
  file_directory_name(ThisFile, ThisDir),

  % EnergyLabels
  assert(user:file_search_path(el, ThisDir)),
  assert(user:file_search_path(el_data, el('Data'))),
  assert(user:file_search_path(el_output, data('Output'))),
  use_module(el(el_app)).

