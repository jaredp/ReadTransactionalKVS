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
    res.send(last_txn)

app.get '/read/:version/:key(*)', (req, res) ->
    {version, key} = req.params
    res.sendStatus(400) if not (version? and key?)

    if key not in db
        return res.sendStatus(404)

    [stored_version, stored_value] = db[key]

    if stored_version > version
        # we have a newer version of the resource than was requested
        res.sendStatus(410)

    else
        res.send(stored_value)

# version: txn_id
# deps: [key]
# patch: {key: value}
app.post '/write', (req, res) ->
    {version, deps, patch} = req.params
    # FIXME check types as well
    res.sendStatus(400) if not (version? and deps? and patch?)

    # check that none of the deps have changed
    for dep in deps
        [stored_version, _stored_value] = db[dep]
        if stored_version > version
            # The request was made depending on an a resource that's changed.
            # It assumes values are x, but they're now x'.
            # Reject the request.
            return res.sendStatus(410)

    # make this request the next transaction
    txn_id = last_txn + 1
    last_txn = txn_id

    # perform the write
    for own key, value of patch
        db[key] = [txn_id, value]

    # report success
    res.sendStatus(200)


## start the server
port = process.env.PORT || 5681
app.listen port, ->
    console.log "running on port #{port}"