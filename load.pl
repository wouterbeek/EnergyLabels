% The load file for the EnergyLabels project.

:- multifile(user:project_name/1).
user:project_name('EnergyLabels').

load_energylabels(Dir):-
  assert(user:file_search_path(el, Dir)),
  assert(user:file_search_path(data, el('Data'))),
  assert(user:file_search_path(output, data('Output'))),
  use_module(el(el_app)).

