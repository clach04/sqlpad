var odbc = require('odbc')()
const { formatSchemaQueryResults } = require('../utils')

const id = 'unixodbc'
const name = 'unixODBC'

// TODO Ingres specific
// as of 2018-05-15 https://github.com/wankdanker/node-odbc does not offer a schema interface
// Consider adding a SCHEMA_SQL config option?
const SCHEMA_SQL_INGRES = `
    SELECT
        varchar(table_owner) as table_schema,
        varchar(table_name) as table_name,
        varchar(column_name) as column_name,
        lower(varchar(column_datatype)) as data_type
    FROM iicolumns
    WHERE
        table_owner != '$ingres'
    ORDER BY
        table_owner,
        table_name,
        column_sequence
`
const SCHEMA_SQL_SQLITE = `
    SELECT
        'dba' as table_schema,
        name as table_name,
        'unknown' as column_name,
        'unknown' as data_type
    FROM sqlite_master
    WHERE type = 'table';

`

const SCHEMA_SQL = SCHEMA_SQL_INGRES;

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
    connection_string: connection.connection_string
  }
  // TODO use connection pool
  // TODO handle connection.maxRows


  var cn = config.connection_string;

  if (config.user) cn = cn + ';Uid=' + config.user;
  if (config.password) cn = cn + ';Pwd=' + config.password;

  return openConnection(cn).then(connectionStatus => {
    return executeQuery(query)
  }).then(queryResult => {
    odbc.close();  // TODO consider putting into finally()?
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

const fields = [
  {
    key: 'connection_string',
    formType: 'TEXT',
    label: 'ODBC connection string, e.g. dsn=NAME or "Driver={Ingres};Server=VNODE;Database=iidbdb"'
  },
  {
    key: 'username',
    formType: 'TEXT',
    label: 'Database Username (optional)'
  },
  {
    key: 'password',
    formType: 'PASSWORD',
    label: 'Database Password (optional)'
  }
]

module.exports = {
  id,
  name,
  fields,
  getSchema,
  runQuery,
  testConnection
}
