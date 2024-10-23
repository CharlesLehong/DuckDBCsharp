using System.Data;
using System.Data.Common;
using System.Dynamic;
using DuckDB.NET.Data;

namespace DuckDB
{
    public class DuckDBRepository
    {
        private readonly string _dbName;
        public DuckDBRepository(string dbName)
        {
            _dbName = dbName;
        }

        public async Task LoadDataAsync(string query)
        {
            using var connection = await GetOpenConnectionAsync();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                command.ExecuteNonQuery();
            }
        }

        public async IAsyncEnumerable<Dictionary<string, object?>> ExecuteQuery(string query)
        {
            using var connection = await GetOpenConnectionAsync();
            using var command = connection.CreateCommand();

            command.CommandText = query;

            using var reader = await command.ExecuteReaderAsync();

            var columnSchema = reader.GetColumnSchema();
            var columns = columnSchema.Select(s => s.ColumnName).ToList();

            while (reader.Read())
            {
                var expandoDict = new Dictionary<string, object?>();

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string columnName = reader.GetName(i);
                    object? value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    expandoDict[columnName] = value;
                }

                yield return expandoDict;
            }
        }

        //public async IAsyncEnumerable<dynamic> ExecuteQuery(string query)
        //{
        //    using var connection = await GetOpenConnectionAsync();
        //    using var command = connection.CreateCommand();

        //    command.CommandText = query;

        //    using var reader = await command.ExecuteReaderAsync();

        //    var columnSchema = reader.GetColumnSchema();
        //    var columns = columnSchema.Select(s => s.ColumnName).ToList();

        //    while (reader.Read())
        //    {
        //        dynamic expando = new ExpandoObject();
        //        var expandoDict = (IDictionary<string, object?>)expando;

        //        for (int i = 0; i < reader.FieldCount; i++)
        //        {
        //            string columnName = reader.GetName(i);
        //            object? value = reader.IsDBNull(i) ? null : reader.GetValue(i); 
        //            expandoDict[columnName] = value;
        //        }

        //        yield return expando;
        //    }
        //}

        private async Task<DuckDBConnection> GetOpenConnectionAsync()
        {
            var connection = new DuckDBConnection($"Data Source={_dbName}");
            await connection.OpenAsync();
            return connection;
        }
    }
}
