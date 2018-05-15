var odbc = require('odbc')()
const { formatSchemaQueryResults } = require('../utils')

const id = 'ingres_unixodbc'
const name = 'Ingres unixODBC'

const SCHEMA_SQL = `
    SELECT
        table_owner,
        table_name,
        column_name,
        column_datatype
    FROM iicolumns
    WHERE 
        table_owner != '$ingres'
    ORDER BY
        table_owner,
        table_name,
        column_sequence
`

/**
 * Run query for connection
 * Should return { rows, incomplete }
 * @param {string} query
 * @param {object} connection
 */

function runQuery(query, connection) {
  const config = {
    user: connection.username,
    password: connection.password,
    server: connection.host,
    port: connection.port ? connection.port : 'VW0',
    database: connection.database
  }
  // TODO use connection pool


  var cn = 'Driver={Ingres};Server=@' + config.server + ',' + config.port + ';Database=' + database + ';Uid=' + config.user + ';Pwd=' + config.password;  // Currently very Ingres specific (maybe just add DSN support?)

  return openConnection(cn).then(connectionStatus => {
    return executeQuery(query)
  }).then(queryResult => {
    console.log(queryResult)
    odbc.close();
    return Promise.resolve({rows: queryResult, incomplete: false})
  })
    .catch(function (e) {
      console.error(e, e.stack);
    })
}

function executeQuery(sqlString) {
  return new Promise((resolve, reject) => {
    odbc.query(sqlString, function (err, data) {
      if (err) reject(err);
      resolve(data);
    })
  });
}

function openConnection(connectionString) {
  return new Promise((resolve, reject) => {
    odbc.open(connectionString, function (err) {
      if (err) reject(err)
      resolve("Connection Open");
    })
  });
}

/**
 * Test connectivity of connection
 * @param {*} connection
 */
function testConnection(connection) {
  const query = "SELECT 'success' AS TestQuery;"
  return runQuery(query, connection)
}

// TODO - reviewed no change needed? datatypes need reviewing
/**
 * Get schema for connection
 * @param {*} connection
 */
function getSchema(connection) {
  return runQuery(SCHEMA_SQL, connection).then(queryResult =>
    formatSchemaQueryResults(queryResult)
  )
}

// TODO
// using same names/descriptions as Microsoft SQL Server (TODO check other drivers terms)
const fields = [
  {
    key: 'host',
    formType: 'TEXT',
    label: 'Host/Server/IP Address'
  },
  {
    key: 'port',
    formType: 'TEXT',
    label: 'Port (optional)'
  },
  {
    key: 'database',
    formType: 'TEXT',
    label: 'Database'
  },
  {
    key: 'username',
    formType: 'TEXT',
    label: 'Database Username'
  },
  {
    key: 'password',
    formType: 'PASSWORD',
    label: 'Database Password'
  }  // TODO encryption option?
]
// TODO drivername / DSN

// TODO - reviewed no change needed?
module.exports = {
  id,
  name,
  fields,
  getSchema,
  runQuery,
  testConnection
}
