# terminusdb-plugins
This project contains example plugins for TerminusDB.

## Installing a plugin
On startup, TerminusDB will look inside the `storage/plugins`
directory, and load any `.pl` file in there. If you like you can
override this location using the `TERMINUSDB_PLUGINS_PATH` environment
variable.

So to install a plugin, simply create the `plugins` directory inside
`storage`, and put the required `.pl` file in there.
