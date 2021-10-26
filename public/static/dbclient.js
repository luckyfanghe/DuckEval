class DuckDBClient {
    constructor(_db) {
        this._db = _db;
        this._counter = 0;
    }

    async db() {
        if (!this._db) {
        this._db = await makeDB();
        }
        return this._db;
    }

    async connection() {
        if (!this._conn) {
        const db = await this.db();
        this._conn = await db.connect();
        }
        return this._conn;
    }

    async reconnect() {
        if (this._conn) {
        this._conn.close();
        }
        delete this._conn;
    }
    
    async query(query, params) {
        const key = `Query ${this._counter++}: ${query}`;
        console.time(key)
        const conn = await this.connection();
        const result = await conn.runQuery(query);
        console.timeEnd(key)
        return result;
    }

    async table(query, params, opts) {
        const result = await this.query(query, params);
        return Inputs.table(result, {layout: 'auto', ...(opts || {})});
    }

    // get the client after the query ran
    async client(query, params) {
        await this.query(query, params);
        return this;
    }

    // query a single row
    async queryRow(query, params) {
        const key = `Query ${this._counter++}: ${query}`;
        console.time(key)
        const conn = await this.connection();
        // use sendQuery as we can stop iterating after we get the first batch
        const result = await conn.sendQuery(query);
        const batch = (await result.next()).value;
        console.timeEnd(key)
        return batch.get(0);
    }
    
    async explain(query, params) {
        const row = await this.queryRow(`EXPLAIN ${query}`, params);
        return element("pre", {className: "observablehq--inspect"}, [
        text(row["explain_value"])
        ]);
    }

    // describe the database (no arg) or a table
    async describe(object) {
        const result = await (object === undefined
        ? this.query(`PRAGMA show_tables`)
        : this.query(`PRAGMA table_info('${object}')`));
        return Inputs.table(result)
    }

    // summzarize a query result
    async summarize(query) {
        const result = await this.query(`SUMMARIZE ${query}`);
        return Inputs.table(result)
    }

    async insertJSON(name, buffer, options) {
        const db = await this.db();
        await db.registerFileBuffer(name, new Uint8Array(buffer))
        const conn = await db.connect();
        await conn.insertJSONFromPath(name, {name, schema: 'main', ...options});
        await conn.close();
    }

    async insertCSV(name, buffer, options) {
        const db = await this.db();
        await db.registerFileBuffer(name, new Uint8Array(buffer))
        const conn = await db.connect();
        await conn.insertCSVFromPath(name, {name, schema: 'main', ...options});
        await conn.close();
    }
    
    // Create a database from FileArrachments
    static async of(files=[]) {
        const db = await makeDB();

        const toName = (file) => file.name.split('.').slice(0, -1).join('.')

        if (files.constructor.name === 'FileAttachment') {
            files = [[toName(files), files]];
        } else if (!Array.isArray(files)) {
            files = Object.entries(files);
        }

        // Add all files to the database. Import JSON and CSV. Create view for Parquet.
        await Promise.all(files.map(async (entry) => {
            let file, name;
            
            if (Array.isArray(entry)) {
                [name, file] = entry;
            } else {
                [name, file] = [toName(entry), entry];
            }
            
            const buffer = await file.arrayBuffer()
            await db.registerFileBuffer(file.name, new Uint8Array(buffer))

            const conn = await db.connect();
            if (file.name.endsWith('.csv')) {
                await conn.insertCSVFromPath(file.name, {name, schema: 'main'});
            } else if (file.name.endsWith('.json')) {
                await conn.insertJSONFromPath(file.name, {name, schema: 'main'});
            } else if (file.name.endsWith('.parquet')) {
                await conn.runQuery(`CREATE VIEW '${name}' AS SELECT * FROM parquet_scan('${file.name}')`);
            } else {
                console.warn(`Don't know how to handle file type of ${file.name}`)
            }
            await conn.close();
        }));

        return new DuckDBClient(db);
    }
}

function element(name, props, children) {
    if (arguments.length === 2) children = props, props = undefined;
    const element = document.createElement(name);
    if (props !== undefined) for (const p in props) element[p] = props[p];
    if (children !== undefined) for (const c of children) element.appendChild(c);
    return element;
}

function text(value) {
    return document.createTextNode(value);
}