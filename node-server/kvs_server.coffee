express = require 'express'
app = express()

bodyParser = require 'body-parser'
app.use(bodyParser.json())

## state
# key: string
# value: string
db = {}  # {key: [txn_id, value]}
last_txn = 0

## External API
app.get '/head', (req, res) ->
    res.send(String(last_txn))

app.get '/read/:version/:key(*)', (req, res) ->
    {version, key} = req.params
    # FIXME typecheck
    # version: int
    # key: string
    res.sendStatus(400) if not (version? and key?)

    # log it for debugging
    console.log('read', version, key)

    if not db[key]?
        # special case; the key does not exist
        # to simplify the implementation, assume this means it's
        # never been created.  We will default to an empty value,
        # so to `delete` just write an empty value to the key.
        return res.send("")

    [stored_version, stored_value] = db[key]

    if stored_version > version
        # we have a newer version of the resource than was requested
        res.sendStatus(410)

    else
        res.send(String(stored_value))

# version: txn_id
# deps: [key]
# patch: {key: value}
app.post '/write', (req, res) ->
    {version, deps, patch} = req.body
    # FIXME check types as well
    res.sendStatus(400) if not (version? and deps? and patch?)

    # check that none of the deps have changed
    for dep in deps
        if not db[dep]?
            # special case; the key does not exist
            # to simplify the implementation, assume this means it's
            # never been created.  If you want to delete an existing key,
            # write "" to its value.  It will be in the db with a version
            # number matching its deletion time.  If we're here, it means
            # the the key was never in the database, so the last mutation
            # (effectively) at start t=0, initializing the value to ""
            continue

        [stored_version, _stored_value] = db[dep]

        if stored_version > version
            # The request was made depending on an a resource that's changed.
            # It assumes values are x, but they're now x'.
            # Reject the request.
            console.log('txn aborted', req.body)
            return res.sendStatus(410)

    # make this request the next transaction
    txn_id = last_txn + 1
    last_txn = txn_id

    # perform the write
    for own key, value of patch
        db[key] = [txn_id, String(value)]

    # log it for debugging
    console.log('txn committed', txn_id, req.body)

    # report success
    res.sendStatus(200)


## start the server
port = process.env.PORT || 5681
app.listen port, ->
    console.log "running on port #{port}"