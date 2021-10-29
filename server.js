const express = require("express");
const path = require("path");
const dotenv = require('dotenv').config();

const snowflake = require('snowflake-sdk');
const sfcon = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    database: process.env.SNOWFLAKE_DATABASE
    // authenticator: 'SNOWFLAKE',
    // clientSessionKeepAlive: true
});

let connection_ID;
sfcon.connect( 
    function(err, conn) {
        if (err) {
            console.error('Unable to connect: ' + err.message);
            } 
        else {
            console.log('Successfully connected to Snowflake.');
            // Optional: store the connection ID.
            connection_ID = conn.getId();
        }
    }
);

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use("/static", express.static(path.resolve(__dirname, "public", "static")));

app.get("/", (req, res) => {
    res.sendFile(path.resolve(__dirname, "public", "index.html"));
});

app.post("/registerfile", (req, res) => {
    res.json({ message: "registered file." });
});

app.listen(process.env.PORT || 5000, () => console.log("Server running..."));