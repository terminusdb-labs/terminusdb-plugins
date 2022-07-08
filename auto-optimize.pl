:- module('plugins/auto_optimize', []).
:- use_module(core(api)).

optimize_chance(system_descriptor{}, C) =>
    C = 0.1.
optimize_chance(_D, C) =>
    C = 0.1.

should_optimize(Descriptor) :-
    optimize_chance(Descriptor, Chance),
    random(X),
    X < Chance.

optimize(Descriptor) :-
    api_optimize:descriptor_optimize(Descriptor),
    resolve_absolute_string_descriptor(Path, Descriptor),
    json_log_debug_formatted("Optimized ~s", [Path]).

all_descriptor(Descriptor, Descriptor).
all_descriptor(Descriptor, Parent_Descriptor) :-
    get_dict(repository_descriptor, Descriptor, Intermediate_Descriptor),
    all_descriptor(Intermediate_Descriptor, Parent_Descriptor).
all_descriptor(Descriptor, Parent_Descriptor) :-
    get_dict(database_descriptor, Descriptor, Intermediate_Descriptor),
    all_descriptor(Intermediate_Descriptor, Parent_Descriptor).

plugins:post_commit_hook(Validation_Objects, _Meta_Data) :-
    forall((member(V, Validation_Objects),
            all_descriptor(V.descriptor, Descriptor),
            should_optimize(Descriptor)),
           optimize(Descriptor)).
