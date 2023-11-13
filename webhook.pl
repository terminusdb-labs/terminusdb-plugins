:- module('plugins/webhook', []).
:- use_module(library(http/http_client)).
:- use_module(core(transaction)).

plugins:post_commit_hook(Validation_Objects, Meta_Data) :-
    % assume only the first validation object matters
    [Validation_Object|_] = Validation_Objects,
    % We only care about commits to branches, nothing else
    is_dict(Validation_Object.descriptor, branch_descriptor),
    Branch_Name = (Validation_Object.descriptor.branch_name),
    Commit_Graph = (Validation_Object.parent),
    branch_head_commit(Commit_Graph, Branch_Name, Commit_Uri),
    commit_uri_to_metadata(Commit_Graph, Commit_Uri, Author, Message, Timestamp),
    commit_id_uri(Commit_Graph, Commit_Id, Commit_Uri),
    Deletes = (Meta_Data.deletes),
    Inserts = (Meta_Data.inserts),

    resolve_absolute_string_descriptor(Resource, (Validation_Object.descriptor)),

    Payload = json{
                  resource: Resource,
                  commit_id: Commit_Id,
                  author: Author,
                  message: Message,
                  timestamp: Timestamp,
                  inserts: Inserts,
                  deletes: Deletes
              },

    % The URL used here is a test endpoint that will reply the
    % original object back, along with a (fake) id. Replace with your
    % own thing.
    http_post('https://jsonplaceholder.typicode.com/posts',
              json(Payload),
              Response,
              [json_object(dict)]),

    % We log the returned id. Replace with your own logging based on
    % your response object.
    Id = (Response.id),
    json_log_info_formatted("called webhook and got a response id ~q", [Id]).
