:- module('plugins/change_tracker', []).
:- use_module(core(transaction)).
:- use_module(core(util)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).
:- use_module(library(terminus_store)).

plugins:post_commit_hook(Validation_Objects, Meta_Data) :-
    % Log that hook was triggered
    json_log_info_formatted("=== CHANGE TRACKER HOOK TRIGGERED ===", []),
    json_log_info_formatted("Metadata - inserts: ~q, deletes: ~q", [Meta_Data.inserts, Meta_Data.deletes]),

    % Filter for branch commits only
    [Validation_Object|_] = Validation_Objects,
    json_log_info_formatted("Validation object descriptor type: ~q", [Validation_Object.descriptor]),

    (   is_dict(Validation_Object.descriptor, branch_descriptor)
    ->  json_log_info_formatted("Processing branch commit...", [])
    ;   json_log_info_formatted("Not a branch commit, skipping. Descriptor type: ~q", [Validation_Object.descriptor]),
        fail
    ),

    % Get commit ID
    Branch_Name = (Validation_Object.descriptor.branch_name),
    json_log_info_formatted("Branch name: ~q", [Branch_Name]),
    Commit_Graph = (Validation_Object.parent),
    branch_head_commit(Commit_Graph, Branch_Name, Commit_Uri),
    commit_id_uri(Commit_Graph, Commit_Id, Commit_Uri),
    json_log_info_formatted("Commit ID: ~q", [Commit_Id]),

    % Get other commit metadata
    commit_uri_to_metadata(Commit_Graph, Commit_Uri, Author, Message, Timestamp),
    json_log_info_formatted("Commit metadata - Author: ~q, Message: ~q", [Author, Message]),

    % Build the collection path with commit ID
    atom_string(Commit_Id, Commit_Id_String),
    atom_concat('commit/', Commit_Id_String, Collection_Atom),
    atom_string(Collection_Atom, _Collection_Path),

    /* REFERENCE: Original WOQL query structure - keeping for reference
    % https://medium.com/dfrnt-com/finding-changed-json-ld-objects-using-graph-branching-commit-history-and-a-datalog-63674f44f244
    % This query would detect document changes but causes transaction conflicts in hooks
    % The structure matches the frontend /api/woql endpoint query
    _Query = json{
        query: json{
            '@type': 'Limit',
            limit: 50,
            query: json{
                '@type': 'Using',
                collection: Collection_Path,
                query: json{
                    '@type': 'Or',
                    or: [
                        json{
                            '@type': 'And',
                            and: [
                                json{
                                    '@type': 'AddedQuad',
                                    subject: json{'@type': 'NodeValue', variable: 'added'},
                                    predicate: json{'@type': 'NodeValue', node: 'rdf:type'},
                                    object: json{'@type': 'Value', variable: 'type1'},
                                    graph: "instance"
                                },
                                json{
                                    '@type': 'Not',
                                    query: json{
                                        '@type': 'Triple',
                                        subject: json{'@type': 'NodeValue', variable: 'type1'},
                                        predicate: json{'@type': 'NodeValue', node: 'sys:subdocument'},
                                        object: json{'@type': 'Value', node: 'rdf:nil'},
                                        graph: "schema"
                                    }
                                },
                                json{
                                    '@type': 'Equals',
                                    left: json{'@type': 'Value', variable: 'id'},
                                    right: json{'@type': 'Value', variable: 'added'}
                                },
                                json{
                                    '@type': 'Equals',
                                    left: json{'@type': 'Value', variable: 'action'},
                                    right: json{'@type': 'Value', node: 'added'}
                                }
                            ]
                        },
                        json{
                            '@type': 'Distinct',
                            variables: ['id'],
                            query: json{
                                '@type': 'And',
                                and: [
                                    json{
                                        '@type': 'Or',
                                        or: [
                                            json{
                                                '@type': 'AddedQuad',
                                                subject: json{'@type': 'NodeValue', variable: 'updated_triple'},
                                                predicate: json{'@type': 'NodeValue', variable: 'upd_pred'},
                                                object: json{'@type': 'Value', variable: 'upd_o'},
                                                graph: "instance"
                                            },
                                            json{
                                                '@type': 'RemovedQuad',
                                                subject: json{'@type': 'NodeValue', variable: 'updated_triple'},
                                                predicate: json{'@type': 'NodeValue', variable: 'upd_pred'},
                                                object: json{'@type': 'Value', variable: 'upd_o'},
                                                graph: "instance"
                                            }
                                        ]
                                    },
                                    json{
                                        '@type': 'Path',
                                        subject: json{'@type': 'NodeValue', variable: 'updated'},
                                        pattern: json{
                                            '@type': 'PathStar',
                                            star: json{'@type': 'InversePathPredicate', predicate: ''}
                                        },
                                        object: json{'@type': 'Value', variable: 'updated_triple'}
                                    },
                                    json{
                                        '@type': 'Triple',
                                        subject: json{'@type': 'NodeValue', variable: 'updated'},
                                        predicate: json{'@type': 'NodeValue', node: 'rdf:type'},
                                        object: json{'@type': 'Value', variable: 'type3'}
                                    },
                                    json{
                                        '@type': 'Not',
                                        query: json{
                                            '@type': 'Triple',
                                            subject: json{'@type': 'NodeValue', variable: 'type3'},
                                            predicate: json{'@type': 'NodeValue', node: 'sys:subdocument'},
                                            object: json{'@type': 'Value', node: 'rdf:nil'},
                                            graph: "schema"
                                        }
                                    },
                                    json{
                                        '@type': 'Equals',
                                        left: json{'@type': 'Value', variable: 'id'},
                                        right: json{'@type': 'Value', variable: 'updated'}
                                    },
                                    json{
                                        '@type': 'Equals',
                                        left: json{'@type': 'Value', variable: 'action'},
                                        right: json{'@type': 'Value', node: 'updated'}
                                    }
                                ]
                            }
                        },
                        json{
                            '@type': 'And',
                            and: [
                                json{
                                    '@type': 'RemovedQuad',
                                    subject: json{'@type': 'NodeValue', variable: 'deleted'},
                                    predicate: json{'@type': 'NodeValue', node: 'rdf:type'},
                                    object: json{'@type': 'Value', variable: 'type2'},
                                    graph: "instance"
                                },
                                json{
                                    '@type': 'Not',
                                    query: json{
                                        '@type': 'Triple',
                                        subject: json{'@type': 'NodeValue', variable: 'type2'},
                                        predicate: json{'@type': 'NodeValue', node: 'sys:subdocument'},
                                        object: json{'@type': 'Value', node: 'rdf:nil'},
                                        graph: "schema"
                                    }
                                },
                                json{
                                    '@type': 'Equals',
                                    left: json{'@type': 'Value', variable: 'id'},
                                    right: json{'@type': 'Value', variable: 'deleted'}
                                },
                                json{
                                    '@type': 'Equals',
                                    left: json{'@type': 'Value', variable: 'action'},
                                    right: json{'@type': 'Value', node: 'deleted'}
                                }
                            ]
                        }
                    ]
                }
            }
        }
    },
    */

    % TODO: Use internal methods to query commit data without creating new transaction
    % Extract layers from validation object to query changes directly
    json_log_info_formatted("Extracting layers from validation object...", []),

    % Get the instance layer from the validation object
    (   Validation_Object.instance_objects = [Instance_Object|_],
        Layer = Instance_Object.read
    ->  json_log_info_formatted("Found instance layer: ~q", [Layer]),

        % Query for document changes using proper TerminusDB patterns
        catch(
            (json_log_info_formatted("Starting document change detection...", []),

             % Get rdf:type predicate ID
             global_prefix_expand(rdf:type, Rdf_Type),
             terminus_store:predicate_id(Layer, Rdf_Type, Rdf_Type_Id),

             % Find added documents (new rdf:type triples)
             findall(json{id: Subject_String, action: "added"},
                     (terminus_store:id_triple_addition(Layer, S_Id, Rdf_Type_Id, Type_Id),
                      terminus_store:subject_id(Layer, Subject_String, S_Id),
                      \+ terminus_store:id_triple_removal(Layer, S_Id, _, _)),
                     Added_Docs),

             % Find deleted documents (removed rdf:type triples)
             findall(json{id: Subject_String, action: "deleted"},
                     (terminus_store:id_triple_removal(Layer, S_Id, Rdf_Type_Id, Type_Id),
                      terminus_store:subject_id(Layer, Subject_String, S_Id),
                      \+ terminus_store:id_triple_addition(Layer, S_Id, _, _)),
                     Deleted_Docs),

             % Find updated documents (both additions and removals for same subject)
             findall(json{id: Subject_String, action: "updated"},
                     distinct(Subject_String,
                             (terminus_store:id_triple_addition(Layer, S_Id, _, _),
                              terminus_store:id_triple_removal(Layer, S_Id, _, _),
                              terminus_store:subject_id(Layer, Subject_String, S_Id))),
                     Updated_Docs),

             json_log_info_formatted("Found ~w added, ~w deleted, ~w updated documents",
                                   [length(Added_Docs), length(Deleted_Docs), length(Updated_Docs)])),
            Layer_Error,
            (json_log_error_formatted("Error detecting document changes: ~q", [Layer_Error]),
             Added_Docs = [],
             Deleted_Docs = [],
             Updated_Docs = [])
        ),

        % Combine all results
        append([Added_Docs, Deleted_Docs, Updated_Docs], All_Changes),
        % Remove duplicates and limit to 50
        list_to_set(All_Changes, Unique_Changes),
        length(Unique_Changes, Changes_Count),
        (Changes_Count > 50 ->
            append(First_50, _, Unique_Changes),
            length(First_50, 50),
            Limited_Changes = First_50
        ;
            Limited_Changes = Unique_Changes
        ),

        json_log_info_formatted("Found ~q document changes (limited to 50)", [Changes_Count]),
        Query_Response = json{
            bindings: Limited_Changes
        }
    ;   json_log_info_formatted("No instance layer found in validation object", []),
        Query_Response = json{
            bindings: [],
            error: "No instance layer found"
        }
    ),

    % Get resource path
    json_log_info_formatted("Getting resource path...", []),
    Branch_Descriptor = Validation_Object.descriptor,
    resolve_absolute_string_descriptor(Resource, Branch_Descriptor),
    json_log_info_formatted("Resource path: ~q", [Resource]),

    % Build complete webhook payload
    Payload = json{
        resource: Resource,
        branch: Branch_Name,
        commit: json{
            id: Commit_Id,
            author: Author,
            message: Message,
            timestamp: Timestamp
        },
        metadata: json{
            inserts_count: Meta_Data.inserts,
            deletes_count: Meta_Data.deletes
        },
        query_results: Query_Response
    },

    % Get webhook URL from environment variable or skip webhook
    (   getenv('TERMINUSDB_CHANGESET_WEBHOOK_URL', Webhook_URL),
        Webhook_URL \= ''
    ->  % Webhook URL is configured, proceed with sending
        json_log_info_formatted("Changeset webhook URL configured: ~q", [Webhook_URL]),
        json_log_info_formatted("Payload size: ~q bytes", [Meta_Data.inserts + Meta_Data.deletes]),
        catch(
            (
                json_log_info_formatted("Sending webhook POST request...", []),
                http_post(Webhook_URL,
                         json(Payload),
                         _Response,
                         []),

                % Log success
                json_log_info_formatted("Successfully sent change data for commit ~q to webhook", [Commit_Id])
            ),
            Error,
            json_log_error_formatted("Failed to send webhook for commit ~q: ~q", [Commit_Id, Error])
        )
    ;   % No webhook URL configured, log warning and skip
        json_log_info_formatted("WARNING: TERMINUSDB_CHANGESET_WEBHOOK_URL environment variable not set - skipping webhook notification", [])
    ),
    json_log_info_formatted("=== CHANGE TRACKER HOOK COMPLETED ===", []).root
