function load_data_with_sortabletable(rows) {
    var thead = document.getElementById("thead");
    var tbody = document.getElementById("tbody");

    while (thead.hasChildNodes()) {
        thead.removeChild(thead.lastChild);
    }
    while (tbody.hasChildNodes()) {
        tbody.removeChild(tbody.lastChild);
    }

    var header_row = document.createElement("tr");
    var field_names = []
    for (let field of rows.schema.fields) {
        addCell(header_row, "th", `${field.name}`);
        field_names.push(field.name)
    }
    thead.appendChild(header_row);

    for (let row of rows) {
        var tr = document.createElement("tr");
        for (let f of field_names) {
            addCell(tr, "td", row[f]);
        }
        tbody.appendChild(tr);
    }

    Sortable.initTable(document.querySelector('#sortable_table'))
}

function addCell(_parent, _child, _text="", _id="", _class="", _img=null, onclick=null) {
    var child = document.createElement(_child)
    child.textContent = _text;
    if (_id) child.id = _id;
    if (_class) child.classList.add(_class);
    if (_img) {
        var img = document.createElement('img'); 
        img.src = _img; 
        img.style = "width: 16px; cursor: pointer;";
        if (onclick) img.onclick = onclick;
        if (_text) {
            var span = document.createElement('span'); 
            span.appendChild(img);
            child.appendChild(span);
        } else {
            child.appendChild(img);
        }
    }
    _parent.appendChild(child);
}

async function init_dexie() {
    // Dexie.delete('DuckEval');
    var dexie_db = new Dexie('DuckEval');

    // Declare tables, IDs and indexes
    dexie_db.version(2).stores({
        duckdb: '++id, file, table, parquet'
    });

    await dexie_db.duckdb.each(item => {
        internal_tables.push(item);
        if (item.parquet && item.parquet.blob.length > 0) {
            for(let i=0; i<duck_dbs.length; i++) {
                duck_dbs[i].instance.registerFileBuffer(item.parquet.name, item.parquet.blob);
            }
            run_only_query(duck_dbs[0].instance, `CREATE TABLE IF NOT EXISTS '${item.table.name}' AS (SELECT * FROM '${item.parquet.name}')`);
        }
    })
    refresh_internal_table();
}

async function init_idb(store_name) {
    // await idb.deleteDB('DuckEvalParquets');
    // localStorage.removeItem("duckdb_tables"); 
    var i_db = await idb.openDB('DuckEvalParquets', 2, {
        upgrade(i_db) {
            // Create a store of objects
            i_db.createObjectStore(store_name);
        },
    });

    const duckdb_tables = JSON.parse(localStorage.getItem('duckdb_tables'));

    if (duckdb_tables) {
        const tx = i_db.transaction(store_name, 'readwrite');
        const store = tx.objectStore(store_name);
        for (let t of duckdb_tables) {
            let parquetbuff = (await store.get(t.hash)) || 0;
            if (parquetbuff)
                await duck_dbs[0].instance.registerFileBuffer(`${t.tablename}.parquet`, parquetbuff);
        }
        await tx.done;

        internal_tables = duckdb_tables;
        refresh_internal_table();
    }

    return i_db;
}

async function save_parquet(i_db, store_name, key, buff) {
    const tx = i_db.transaction(store_name, 'readwrite');
    const store = tx.objectStore(store_name);

    await store.put(buff, key);
    await tx.done;
}


function init_localforage() {
    localforage.setDriver(localforage.INDEXEDDB);
    const dbName = 'duckeval';

    lf_parquets = localforage.createInstance({
        name        : dbName,
        storeName   : 'parquets'
    });

    lf_files = localforage.createInstance({
        name        : dbName,
        storeName   : 'files'
    });

    lf_files.iterate(function(value, key, iterationNumber) {
        console.log([key, value]);
    }).then(function() {
        console.log('Iteration has completed');
    }).catch(function(err) {
        console.log(err);
    });
}

async function handleDirectoryEntry( dirHandle, out ) {
    for await (const entry of dirHandle.values()) {
        if (entry.kind === "file"){
            const file = await entry.getFile();
            out[ file.name ] = file;
        }
        if (entry.kind === "directory") {
            const newHandle = await dirHandle.getDirectoryHandle( entry.name, { create: false } );
            const newOut = out[ entry.name ] = {};
            await handleDirectoryEntry( newHandle, newOut );
        }
    }
}

function formatFileSize(size) {
    let format_size = size;
    if (format_size < 1024) return format_size + 'B';
    format_size /= 1024;
    if (format_size < 1024) return Math.round(format_size) + 'KB';
    format_size /= 1024;
    if (format_size < 1024) return Math.round(format_size) + 'MB';
    return (format_size / 1024).toFixed(1) + 'GB';
}