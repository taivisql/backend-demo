const express = require('express');
const Snowflake = require('snowflake-sdk');
const bodyParser = require('body-parser');

const app = express();
const cors = require('cors');
app.use(cors({
  origin: '*' // This allows requests from any origin (not recommended for production)
}));
app.use(bodyParser.json()); // Parse JSON data
app.use(bodyParser.urlencoded({ extended: true })); // Parse form-urlencoded data (for x-www-form-urlencoded)



function transformToSchemaObjects(data,db) {
    let schemaObj={'schemaName':'','tables':[]}
    let tableObj={'tableName':'','columns':[]}
    let columnObj={'columnName':'','dataType':''}
    let result={'databaseName':db,'schemas':[]}
    // Group data by schema
  const schemaMap = new Map();
  data.forEach((item) => {
    const { TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE } = item;
    if (!schemaMap.has(TABLE_SCHEMA)) {
      schemaMap.set(TABLE_SCHEMA, new Map());
    }
    const schema = schemaMap.get(TABLE_SCHEMA);

    if (!schema.has(TABLE_NAME)) {
      schema.set(TABLE_NAME, []);
    }
    const table = schema.get(TABLE_NAME);

    table.push({ columnName: COLUMN_NAME, dataType: DATA_TYPE });
  });

  // Convert schema map to JSON
  schemaMap.forEach((tables, schemaName) => {
    const schemaObj = {
      schemaName,
      tables: [],
    };

    tables.forEach((columns, tableName) => {
      schemaObj.tables.push({
        tableName,
        columns,
      });
    });

    result['schemas'].push(schemaObj);
  });
  return result
    // console.log(result)
    // console.log(schemaMap)
  }

app.post('/connectAndGetDBDetails', async (req, res) => {
    let account=req.body.account
    let username=req.body.username
    let password=req.body.password
    let warehouse=req.body.warehouse
    let database=req.body.database
  try {
    var connection = Snowflake.createConnection({

        account: account,
        username: username,
        password: password,
        warehouse: warehouse,
        database:database
        });
        
    // Try to connect to Snowflake, and check whether the connection was successful.
    connection.connect( 
        function(err, conn) {
            if (err) {
                console.error('Unable to connect: ' + err.message);
                } 
            else {
                console.log('Successfully connected to Snowflake.');
                // Optional: store the connection ID.
                connection_ID = conn.getId();
                console.log(connection_ID)
                var statement = connection.execute({
                    sqlText: "select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,DATA_TYPE from "+database+".INFORMATION_SCHEMA.COLUMNS where table_schema != 'INFORMATION_SCHEMA'",
                    complete: async function(err, stmt, rows) {
                      if (err) {
                        console.error('Failed to execute statement due to the following error: ' + err.message);
                      } else {
                        console.log('Successfully executed statement: ' + stmt.getSqlText());
                        
                        res.send(transformToSchemaObjects(rows,database))

                        // res.send(rows)
                        
                    }
                }
            });
                
                }
        }
    );
  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error'); // Avoid leaking sensitive error details
  }
});

app.post('/executeSQL', async (req, res) => {
  const {  sql } = req.body;
  let account=req.body.account
    let username=req.body.username
    let password=req.body.password
    let warehouse=req.body.warehouse
    let database=req.body.database
  try {
    var connection = Snowflake.createConnection({

        account: account,
        username: username,
        password: password,
        warehouse: warehouse,
        database:database
        });
        
    // Try to connect to Snowflake, and check whether the connection was successful.
    connection.connect( 
        function(err, conn) {
            if (err) {
                console.error('Unable to connect: ' + err.message);
                } 
            else {
                console.log('Successfully connected to Snowflake.');
                // Optional: store the connection ID.
                connection_ID = conn.getId();
                console.log(connection_ID)
                var statement = connection.execute({
                    sqlText: sql,
                    complete: async function(err, stmt, rows) {
                      if (err) {
                        console.error('Failed to execute statement due to the following error: ' + err.message);
                      } else {
                        console.log('Successfully executed statement: ' + stmt.getSqlText());
                        
                        // res.send(transformToSchemaObjects(rows,database))

                        res.send(rows)
                        
                    }
                }
            });
                
                }
        }
    );
  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error'); // Avoid leaking sensitive error details
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Server listening on port ${port}`));
