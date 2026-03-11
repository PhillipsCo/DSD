using DSD.UI.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using System.Windows;

namespace DSD.UI.Repositories
{
    /// <summary>
    /// ApiListRepository
    /// =================
    ///
    /// PURPOSE:
    ///   CRUD + queries for DSD_API_LIST.
    ///
    /// IMPORTANT DESIGN DETAIL:
    ///   This repository must be able to connect to a CUSTOMER-SPECIFIC DATABASE.
    ///   The base connection string is stored in appsettings.json (CustomerConnectionDB),
    ///   but the correct database name ("Initial Catalog") comes from CustomerInfoRow.InitialCatalog.
    ///
    /// HOW WE SOLVE THAT:
    ///   - Store the base connection string once.
    ///   - Use SqlConnectionStringBuilder to swap InitialCatalog per call.
    ///
    /// WHY SqlConnectionStringBuilder:
    ///   - Safe (no fragile string replace)
    ///   - Handles either "Initial Catalog" or "Database" forms
    ///   - Preserves all other settings (server, user, password, encrypt, etc.)
    /// </summary>
    public class ApiListRepository
    {
        // ---------------------------------------------------------------------
        // Base connection string (from appsettings.json) - contains server/creds,
        // but may have an incorrect/placeholder Initial Catalog.
        // ---------------------------------------------------------------------
        private readonly string _baseCs;

        public ApiListRepository(IConfiguration config)
        {
            _baseCs = config.GetConnectionString("CustomerConnectionDB")
                  ?? throw new InvalidOperationException(
                      "ConnectionStrings:CustomerConnectionDB not found in appsettings.json");
        }

        // ---------------------------------------------------------------------
        // Helper: builds the real connection string for the selected customer DB.
        // ---------------------------------------------------------------------
        private string BuildCustomerConnectionString(string initialCatalog)
        {
            var builder = new SqlConnectionStringBuilder(_baseCs)
            {
                InitialCatalog = initialCatalog
            };
            return builder.ConnectionString;
        }

        // ---------------------------------------------------------------------
        // Helper: for backwards-compatible overloads (no catalog parameter).
        // NOTE: This uses whatever Initial Catalog is currently in appsettings.
        // Prefer the overloads that accept initialCatalog to avoid wrong DB bugs.
        // ---------------------------------------------------------------------
        private string BuildDefaultConnectionString()
        {
            // If your appsettings has the wrong catalog, this will be wrong.
            // We keep it only so existing code still compiles.
            return _baseCs;
        }

        // =========================================================
        // READ METHODS
        // =========================================================

        /// <summary>
        /// Backwards-compatible: loads from the base connection string's catalog.
        /// Prefer GetAllAsync(initialCatalog).
        /// </summary>
        public Task<List<ApiListRow>> GetAllAsync()
            => GetAllAsyncInternal(BuildDefaultConnectionString());

        /// <summary>
        /// ✅ Correct: loads from the customer-specific catalog.
        /// </summary>
        public Task<List<ApiListRow>> GetAllAsync(string initialCatalog)
            => GetAllAsyncInternal(BuildCustomerConnectionString(initialCatalog));

        private async Task<List<ApiListRow>> GetAllAsyncInternal(string cs)
        {
            const string sql = @"
SELECT TABLE_NAME, API_NAME, [FILTER], BATCHSIZE, [DIR], RUNGROUP, ENDPOINT
FROM DSD_API_LIST
ORDER BY TABLE_NAME;";

            var list = new List<ApiListRow>();

            await using var conn = new SqlConnection(cs);
            await conn.OpenAsync().ConfigureAwait(false);

            await using var cmd = new SqlCommand(sql, conn);

            try
            {
                await using var rdr = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                int oTable = rdr.GetOrdinal("TABLE_NAME");
                int oApi = rdr.GetOrdinal("API_NAME");
                int oFilter = rdr.GetOrdinal("FILTER");
                int oBatch = rdr.GetOrdinal("BATCHSIZE");
                int oDir = rdr.GetOrdinal("DIR");
                int oRunGroup = rdr.GetOrdinal("RUNGROUP");
                int oEndpoint = rdr.GetOrdinal("ENDPOINT");

                while (await rdr.ReadAsync().ConfigureAwait(false))
                {
                    list.Add(new ApiListRow
                    {
                        TABLE_NAME = rdr.IsDBNull(oTable) ? "" : rdr.GetString(oTable),
                        API_NAME = rdr.IsDBNull(oApi) ? "" : rdr.GetString(oApi),
                        FILTER = rdr.IsDBNull(oFilter) ? "" : rdr.GetString(oFilter),
                        BATCHSIZE = rdr.IsDBNull(oBatch) ? 0 : rdr.GetInt32(oBatch),
                        DIR = rdr.IsDBNull(oDir) ? "" : rdr.GetString(oDir),
                        RUNGROUP = rdr.IsDBNull(oRunGroup) ? "" : rdr.GetString(oRunGroup),
                        ENDPOINT = rdr.IsDBNull(oEndpoint) ? "" : rdr.GetString(oEndpoint),
                    });
                }
            }
            catch (SqlException ex)
            {
                MessageBox.Show(
                    $"SQL error {ex.Number}\nMessage: {ex.Message}\nServer: {conn.DataSource}\nDatabase: {conn.Database}\nSQL:\n{cmd.CommandText}");
                throw;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
                throw;
            }

            return list;
        }

        /// <summary>
        /// Backwards-compatible: filters TABLE_NAME using the base connection string.
        /// Prefer GetByTableNameAsync(initialCatalog, tableName).
        /// </summary>
        public Task<List<ApiListRow>> GetByTableNameAsync(string tableName)
            => GetByTableNameAsyncInternal(BuildDefaultConnectionString(), tableName);

        /// <summary>
        /// ✅ Correct: filters TABLE_NAME using the customer-specific catalog.
        /// </summary>
        public Task<List<ApiListRow>> GetByTableNameAsync(string initialCatalog, string tableName)
            => GetByTableNameAsyncInternal(BuildCustomerConnectionString(initialCatalog), tableName);

        private async Task<List<ApiListRow>> GetByTableNameAsyncInternal(string cs, string tableName)
        {
            const string sql = @"
SELECT TABLE_NAME, API_NAME, [FILTER], BATCHSIZE, [DIR], RUNGROUP, ENDPOINT
FROM DSD_API_LIST
WHERE TABLE_NAME = @tableName
ORDER BY TABLE_NAME, API_NAME;";

            var list = new List<ApiListRow>();

            await using var conn = new SqlConnection(cs);
            await conn.OpenAsync().ConfigureAwait(false);

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.Add("@tableName", SqlDbType.NVarChar, 128).Value = tableName;

            try
            {
                await using var rdr = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                int oTable = rdr.GetOrdinal("TABLE_NAME");
                int oApi = rdr.GetOrdinal("API_NAME");
                int oFilter = rdr.GetOrdinal("FILTER");
                int oBatch = rdr.GetOrdinal("BATCHSIZE");
                int oDir = rdr.GetOrdinal("DIR");
                int oRunGroup = rdr.GetOrdinal("RUNGROUP");
                int oEndpoint = rdr.GetOrdinal("ENDPOINT");

                while (await rdr.ReadAsync().ConfigureAwait(false))
                {
                    list.Add(new ApiListRow
                    {
                        TABLE_NAME = rdr.IsDBNull(oTable) ? "" : rdr.GetString(oTable),
                        API_NAME = rdr.IsDBNull(oApi) ? "" : rdr.GetString(oApi),
                        FILTER = rdr.IsDBNull(oFilter) ? "" : rdr.GetString(oFilter),
                        BATCHSIZE = rdr.IsDBNull(oBatch) ? 0 : rdr.GetInt32(oBatch),
                        DIR = rdr.IsDBNull(oDir) ? "" : rdr.GetString(oDir),
                        RUNGROUP = rdr.IsDBNull(oRunGroup) ? "" : rdr.GetString(oRunGroup),
                        ENDPOINT = rdr.IsDBNull(oEndpoint) ? "" : rdr.GetString(oEndpoint),
                    });
                }
            }
            catch (SqlException ex)
            {
                MessageBox.Show(
                    $"SQL error {ex.Number}\nMessage: {ex.Message}\nServer: {conn.DataSource}\nDatabase: {conn.Database}\nSQL:\n{cmd.CommandText}");
                throw;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
                throw;
            }

            return list;
        }

        // =========================================================
        // CRUD METHODS (Catalog-aware overloads added)
        // =========================================================

        public Task<int> InsertAsync(ApiListRow row)
            => InsertAsyncInternal(BuildDefaultConnectionString(), row);

        public Task<int> InsertAsync(string initialCatalog, ApiListRow row)
            => InsertAsyncInternal(BuildCustomerConnectionString(initialCatalog), row);

        private async Task<int> InsertAsyncInternal(string cs, ApiListRow row)
        {
            const string sql = @"
INSERT INTO DSD_API_LIST (TABLE_NAME, API_NAME, [FILTER], BATCHSIZE, [DIR], RUNGROUP, ENDPOINT)
VALUES (@TABLE_NAME, @API_NAME, @FILTER, @BATCHSIZE, @DIR, @RUNGROUP, @ENDPOINT);";

            await using var conn = new SqlConnection(cs);
            await conn.OpenAsync().ConfigureAwait(false);

            await using var cmd = new SqlCommand(sql, conn);
            AddParams(cmd, row);

            try
            {
                return await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (SqlException ex)
            {
                MessageBox.Show(
                    $"SQL error {ex.Number}\nMessage: {ex.Message}\nServer: {conn.DataSource}\nDatabase: {conn.Database}\nSQL:\n{cmd.CommandText}");
                throw;
            }
        }

        public Task<int> UpdateAsync(ApiListRow row)
            => UpdateAsyncInternal(BuildDefaultConnectionString(), row);

        public Task<int> UpdateAsync(string initialCatalog, ApiListRow row)
            => UpdateAsyncInternal(BuildCustomerConnectionString(initialCatalog), row);

        private async Task<int> UpdateAsyncInternal(string cs, ApiListRow row)
        {
            const string sql = @"
UPDATE DSD_API_LIST
SET
      [FILTER]   = @FILTER
    , BATCHSIZE  = @BATCHSIZE
    , [DIR]      = @DIR
    , RUNGROUP   = @RUNGROUP
    , ENDPOINT   = @ENDPOINT
WHERE TABLE_NAME = @TABLE_NAME
  AND API_NAME   = @API_NAME;";

            await using var conn = new SqlConnection(cs);
            await conn.OpenAsync().ConfigureAwait(false);

            await using var cmd = new SqlCommand(sql, conn);
            AddParams(cmd, row);

            try
            {
                return await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (SqlException ex)
            {
                MessageBox.Show(
                    $"SQL error {ex.Number}\nMessage: {ex.Message}\nServer: {conn.DataSource}\nDatabase: {conn.Database}\nSQL:\n{cmd.CommandText}");
                throw;
            }
        }

        public Task<int> DeleteAsync(string tableName, string apiName)
            => DeleteAsyncInternal(BuildDefaultConnectionString(), tableName, apiName);

        public Task<int> DeleteAsync(string initialCatalog, string tableName, string apiName)
            => DeleteAsyncInternal(BuildCustomerConnectionString(initialCatalog), tableName, apiName);

        private async Task<int> DeleteAsyncInternal(string cs, string tableName, string apiName)
        {
            const string sql = @"
DELETE FROM DSD_API_LIST
WHERE TABLE_NAME = @TABLE_NAME
  AND API_NAME   = @API_NAME;";

            await using var conn = new SqlConnection(cs);
            await conn.OpenAsync().ConfigureAwait(false);

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.Add("@TABLE_NAME", SqlDbType.NVarChar, 128).Value = tableName;
            cmd.Parameters.Add("@API_NAME", SqlDbType.NVarChar, 128).Value = apiName;

            try
            {
                return await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (SqlException ex)
            {
                MessageBox.Show(
                    $"SQL error {ex.Number}\nMessage: {ex.Message}\nServer: {conn.DataSource}\nDatabase: {conn.Database}\nSQL:\n{cmd.CommandText}");
                throw;
            }
        }

        public Task<bool> ExistsAsync(string tableName, string apiName)
            => ExistsAsyncInternal(BuildDefaultConnectionString(), tableName, apiName);

        public Task<bool> ExistsAsync(string initialCatalog, string tableName, string apiName)
            => ExistsAsyncInternal(BuildCustomerConnectionString(initialCatalog), tableName, apiName);

        private async Task<bool> ExistsAsyncInternal(string cs, string tableName, string apiName)
        {
            const string sql = @"
SELECT 1
FROM DSD_API_LIST
WHERE TABLE_NAME = @TABLE_NAME
  AND API_NAME   = @API_NAME;";

            await using var conn = new SqlConnection(cs);
            await conn.OpenAsync().ConfigureAwait(false);

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.Add("@TABLE_NAME", SqlDbType.NVarChar, 128).Value = tableName;
            cmd.Parameters.Add("@API_NAME", SqlDbType.NVarChar, 128).Value = apiName;

            var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            return result != null;
        }

        // =========================================================
        // PARAMETER HELPERS
        // =========================================================

        private static void AddParams(SqlCommand cmd, ApiListRow row)
        {
            cmd.Parameters.Add("@TABLE_NAME", SqlDbType.NVarChar, 128).Value = row.TABLE_NAME ?? "";
            cmd.Parameters.Add("@API_NAME", SqlDbType.NVarChar, 128).Value = row.API_NAME ?? "";
            cmd.Parameters.Add("@FILTER", SqlDbType.NVarChar, -1).Value = (object?)row.FILTER ?? DBNull.Value;
            cmd.Parameters.Add("@BATCHSIZE", SqlDbType.Int).Value = row.BATCHSIZE;
            cmd.Parameters.Add("@DIR", SqlDbType.NVarChar, 50).Value = (object?)row.DIR ?? DBNull.Value;
            cmd.Parameters.Add("@RUNGROUP", SqlDbType.NVarChar, 50).Value = (object?)row.RUNGROUP ?? DBNull.Value;
            cmd.Parameters.Add("@ENDPOINT", SqlDbType.NVarChar, 400).Value = (object?)row.ENDPOINT ?? DBNull.Value;
        }
    }
}