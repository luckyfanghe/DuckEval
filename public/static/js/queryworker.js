var parameters = {}
location.search.slice(1).split("&").forEach( function(key_value) { var kv = key_value.split("="); parameters[kv[0]] = kv[1]; })

console.log(parameters)

onmessage = async function(e) {
    workerResult = {}
    console.log(this, self.duck_db);
    try {
        const conn = await duck_db.connect();
        const elapsed = Date.now();
        // const result = await conn.runQuery(query);
        const result = await conn.sendQuery(query);
    
        let batch = (await result.next()).value;
        const field_names = []
        for (let field of batch.schema.fields) {
            field_names.push(field.name)
        }
    
        var data = []
        while(batch.length > 0 && !cancel_query) {
            for (let row of batch) {
                let r = {}
                for (let f of field_names) {
                    r[f] = row[f];
                }
                data.push(r)
            }
            if (cancel_query) break;
            batch = (await result.next()).value;
            if (!batch) break;
        }
        
        await conn.close();
        
        workerResult.success = true;
        workerResult.field_names = field_names;
        workerResult.data = data;
    } 
    catch (e) {
        workerResult.success = false;
        workerResult.error = e.message;
    }
    postMessage(workerResult);
}