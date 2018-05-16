const assert = require('assert')
const ingres_unixodbc = require('./index.js')

const connection = {
  connection_string: process.env.ODBC_CONNECTION_STRING  // I.e. ensure os variable is set to connection string
}

const createTable = 'CREATE TABLE test (id integer);' // NOTE test(s) will fail if table already exists, expect empty database
const insert1 = 'INSERT INTO test (id) VALUES (1);'
const insert2 = 'INSERT INTO test (id) VALUES (2);'
const insert3 = 'INSERT INTO test (id) VALUES (3);'

// TODO test more datatypes:
//   * integer (different sizes
//   * float
//   * char
//   * varchar
//   * decimal
//   * date
//   * datetime
//   * interval
describe('drivers/ingres_unixodbc', function() {
  before(function() {
    this.timeout(10000)
    return ingres_unixodbc
      .runQuery(createTable, connection)
      .then(() => ingres_unixodbc.runQuery(insert1, connection))
      .then(() => ingres_unixodbc.runQuery(insert2, connection))
      .then(() => ingres_unixodbc.runQuery(insert3, connection))
  })

  it('tests connection', function() {
    return ingres_unixodbc.testConnection(connection)
  })

  it('getSchema()', function() {
    return ingres_unixodbc.getSchema(connection).then(schemaInfo => {
      assert(schemaInfo.ingres, 'ingres')  // assuming connecting as Ingres
      assert(schemaInfo.ingres.test, 'ingres.test')
      const columns = schemaInfo.ingres.test
      assert.equal(columns.length, 1, 'columns.length')
      assert.equal(columns[0].table_schema, 'ingres', 'table_schema')
      assert.equal(columns[0].table_name, 'test', 'table_name')
      assert.equal(columns[0].column_name, 'id', 'column_name')
      assert.equal(columns[0].data_type, 'integer', 'data_type')
    })
  })

  it('runQuery under limit', function() {
    return ingres_unixodbc
      .runQuery('SELECT * FROM test WHERE id = 1;', connection)
      .then(results => {
        assert(!results.incomplete, 'not incomplete')
        assert.equal(results.rows.length, 1, 'row length')
      })
  })

  it('runQuery over limit', function() {
    return ingres_unixodbc
      .runQuery('SELECT * FROM test;', connection)
      .then(results => {
        assert(results.incomplete, 'incomplete')
        assert.equal(results.rows.length, 2, 'row length')
      })
  })
})
