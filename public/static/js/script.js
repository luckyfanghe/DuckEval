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

function addCell(tr, type, value, onclick=null, img_url=null) {
    var td = document.createElement(type)
    td.textContent = value;
    if (onclick) {
        td.onclick = onclick;
    }
    if (img_url) {
        var img = document.createElement('img'); 
        img.src = '/static/images/preview.svg'; 
        img.style = "width: 16px; cursor: pointer;";
        td.appendChild(img);
    }
    tr.appendChild(td);
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