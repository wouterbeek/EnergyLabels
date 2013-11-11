:- module(el_app, []).

/** <module> Energy labels application

A simple application to showcase what can be done with
the LOD version of the dataset of energy labels.

@author Wouter Beek
@version 2013/10-2013/11
*/

:- use_module(html(html_form)).
:- use_module(html(html_table)).
:- use_module(library(http/html_write)).
:- use_module(library(http/http_dispatch)).
:- use_module(library(http/http_parameters)).
:- use_module(library(http/http_path)).
:- use_module(library(lists)).
:- use_module(library(semweb/rdf_db)).
:- use_module(library(settings)).
:- use_module(rdf(rdf_name)).
:- use_module(rdf(rdf_serial)).
:- use_module(server(app_ui)).
:- use_module(server(web_modules)).
:- use_module(sparql(sparql_ext)).
:- use_module(xml(xml_namespace)).

:- xml_register_namespace(el, 'https://data.overheid.nl/data/dataset/energielabels-agentschap-nl/').
:- register_sparql_prefix(el).
:- register_sparql_remote(el, 'lod.cedar-project.nl', 8080, '/sparql/pilod').

:- http_handler(root(el), el, []).

:- setting(
  port,
  integer,
  5000,
  'The default port at which to start the server.'
).

:- initialization(web_module_add('EnergyLabels', el_app, el)).



el(Request):-
  http_parameters(
    Request,
    [
      house_number(HouseNumber, [default(no_house_number)]),
      house_number_addition(
        HouseNumberAddition,
        [default(no_house_number_addition)]
      ),
      postcode(Postcode, [default(no_postcode)])
    ]
  ),
  reply_html_page(
    app_style,
    \el_head,
    \el_body(Postcode, HouseNumber, HouseNumberAddition)
  ).

el_body(Postcode, HouseNumber, HouseNumberAddition) -->
  {http_absolute_location(root(el), URL, [])},
  html([
    \submission_form(
      URL,
      [
        input([name=postcode,placeholder='Postcode',type=text]),
        input([name=house_number,placeholder='House number',type=text]),
        input([name=house_number_addition,placeholder='House number addition',type=text]),
        \submit_button
      ]
    ),
    \el_content(Postcode, HouseNumber, HouseNumberAddition)
  ]).

el_content(Postcode, HouseNumber, HouseNumberAddition) -->
  {(
    Postcode == no_postcode
  ;
    HouseNumber == no_house_number
  ;
    HouseNumberAddition == no_house_number_addition
  )}, !.
el_content(Postcode, HouseNumber, HouseNumberAddition) -->
  {
    el_index(Postcode, HouseNumber, HouseNumberAddition, L),
    el_indexes(Postcode, Ls),
gtrace,
    nth0(I, Ls, L)
  },
  html(
    \html_table(
      [
        caption('SPARQL results'),
        header(true),
        highlighted_rows([I]),
        indexed(true)
      ],
      [['Building','Prestation index']|Ls]
    )
  ).

%  {
%    gas_order('1094', GasOrder),
%    html_table(
%      [caption('Energy comparison'),header(true),indexed(true)],
%      [['Postcode','Huisnummer','Gasverbruik']|List],
%      HTML_Table
%    )
%  },
%  html(body(HTML_Table)).

el_head -->
  html(title('Energy Labels Demo App')).

el_index(Postcode, HouseNumber, HouseNumberAddition, L):-
  Where1 = '?building a el:Building .',
  Where2 = '?building el:postcode ?postcode .',
  format(atom(Where3), 'filter regex(?postcode, "^~w") .', [Postcode]),
  format(atom(Where4), '?building el:huisnummer ~w .', [HouseNumber]),
  format(
    atom(Where5),
    '?building el:huisnummer_toevoeging "-~w" .',
    [HouseNumberAddition]
  ),
  Where6 = '?building el:certificaat ?certificaat .',
  Where7 = '?certificaat el:energie_prestatieindex ?index .',
  formulate_sparql(
    default_graph('http://example.com/el'),
    [el],
    select([distinct(true)],[building,index]),
    [Where1,Where2,Where3,Where4,Where5,Where6,Where7],
    _Extra,
    Query
  ),
  enqueue_sparql(el, Query, _VarNames, [Row]),
  Row =.. [row|L].

el_indexes(PostcodePrefix, Ls):-
  Where1 = '?building a el:Building .',
  Where2 = '?building el:postcode ?postcode .',
  format(atom(Where3), 'filter regex(?postcode, "^~w") .', [PostcodePrefix]),
  Where4 = '?building el:certificaat ?certificaat .',
  Where5 = '?certificaat el:energie_prestatieindex ?index .',
  formulate_sparql(
    default_graph('http://example.com/el'),
    [el],
    select([distinct(true)],[building,index]),
    [Where1,Where2,Where3,Where4,Where5],
    order_by([order(asc)],[index]),
    Query
  ),
  enqueue_sparql(el, Query, _VarNames, Rows),
  sparql_rows_to_lists(Rows, Ls).

% Already loaded in memory.
load_el_data:-
  rdf_graph(el), !.
% Already available on disk.
load_el_data:-
  absolute_file_name(
    output(el_1),
    File,
    [access(read),file_errors(fail),file_type(turtle)]
  ), !,
  rdf_load2(File, [format(turtle),graph(el)]).
% Has to be created.
load_el_data:-
  use_module(el(el_script)).

