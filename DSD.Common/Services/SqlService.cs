
using DSD.Common.Models;
//using DSD.Outbound.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Polly;
using Serilog;
using System.Data;
using static Org.BouncyCastle.Math.EC.ECCurve;

namespace DSD.Common.Services
{
    public class SqlService : ISqlService
    {
        // ------------------------------------------------------------
        // Private field for configuration
        // ------------------------------------------------------------
        // IConfiguration is injected via DI and provides access to
        // appsettings.json values such as connection strings and flags.
        private readonly IConfiguration _config;

        // ------------------------------------------------------------
        // Constructor: Dependency Injection of IConfiguration
        // ------------------------------------------------------------
        public SqlService(IConfiguration config)
        {
            _config = config;
        }

        // ------------------------------------------------------------
        // GetApiListAsync: Retrieves list of APIs for a given customer DB and group
        // ------------------------------------------------------------
        // Parameters:
        //   db    - Database name (InitialCatalog for the customer)
        //   group - API execution group (e.g., ALL, HFS)
        // Returns:
        //   List<TableApiName> containing API details for execution
        public async Task<List<PurgeJob>> GetPurgeJobsAsync(string catalog)
        {
            const string sql = @"
SELECT
    jobKey,
    jobName,
    jobEnabled,
    jobTimeout,
    jobSQL,
    jobRetentionDays
FROM dbo.DSD_EOD
ORDER BY EntryNo;";

            var jobs = new List<PurgeJob>();

            try
            {
                var connectionString = CustomerConnectionString(catalog);

                await using var conn = new SqlConnection(connectionString);
                await using var cmd = new SqlCommand(sql, conn)
                {
                    CommandType = CommandType.Text,
                    CommandTimeout = 120
                };

                await conn.OpenAsync().ConfigureAwait(false);

                await using var rdr = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess)
                                              .ConfigureAwait(false);

                // Use ordinals (faster + less error-prone)
                int oKey = rdr.GetOrdinal("jobKey");
                int oName = rdr.GetOrdinal("jobName");
                int oEnabled = rdr.GetOrdinal("jobEnabled");
                int oTimeout = rdr.GetOrdinal("jobTimeout");
                int oSql = rdr.GetOrdinal("jobSQL");
                int oRetention = rdr.GetOrdinal("jobRetentionDays");

                while (await rdr.ReadAsync().ConfigureAwait(false))
                {
                    // Start with defaults from your model (jobTimeout=120, jobRetentionDays=30)
                    var job = new PurgeJob
                    {
                        jobKey = rdr.IsDBNull(oKey) ? "" : rdr.GetString(oKey),
                        jobName = rdr.IsDBNull(oName) ? "" : rdr.GetString(oName),
                        jobEnabled = !rdr.IsDBNull(oEnabled) && rdr.GetBoolean(oEnabled),
                        jobTimeout = rdr.IsDBNull(oTimeout) ? 120 : rdr.GetInt32(oTimeout),
                        jobSql = rdr.IsDBNull(oSql) ? "" : rdr.GetString(oSql),
                        
                        jobRetentionDays = rdr.IsDBNull(oRetention) ? 120 : rdr.GetInt32(oRetention)
                    };

                    // Only overwrite defaults if DB has values
                    //if (!rdr.IsDBNull(oTimeout))
                    //    job.jobTimeout = rdr.GetInt32(oTimeout);

                    //if (!rdr.IsDBNull(oRetention))
                    //    job.jobRetentionDays = rdr.GetInt32(oRetention);

                    jobs.Add(job);
                }
            }
            catch (SqlException ex)
            {
                Log.Error(ex, "SQL error loading purge jobs from dbo.DSD_EOD in catalog {Catalog}", catalog);
                throw;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Unexpected error loading purge jobs from dbo.DSD_EOD in catalog {Catalog}", catalog);
                throw;
            }

            return jobs;
        }
        public sealed record SqlExecResult(
            bool Success,
            int RowsAffected,
            string? ErrorMessage = null
        );
        
        public async Task<List<CustomerRow>> GetCustomersAsync()
        {
            const string sql = "select id, customer, InitialCatalog  from DSD_CUSTOMERINFO order by customer";

            var list = new List<CustomerRow>();
            var _connectionString = CustomerConnectionString("CustomerConnection");
            using var conn = new SqlConnection(_connectionString);
            using var cmd = new SqlCommand(sql, conn);

            await conn.OpenAsync().ConfigureAwait(false);
            using var rdr = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

            while (await rdr.ReadAsync().ConfigureAwait(false))
            {
                list.Add(new CustomerRow
                {
                    Id = rdr.GetInt32(0),
                    Customer = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
                    InitialCatalog = rdr.IsDBNull(2) ? "" : rdr.GetString(2)
                });
            }

            return list;
        }
        //public async Task<int> SqlCountAsync(string databaseName, string tableName)
        //{ try
        //    {
        //        var count = 0;
        //        var connectionString = CustomerConnectionString(databaseName);

        //        await using var conn = new SqlConnection(connectionString);
        //        await conn.OpenAsync();

        //        var sql = $@"
        //    IF OBJECT_ID(@fullTableName, 'U') IS NULL
        //        SELECT 0
        //    ELSE
        //        EXEC('SELECT COUNT(*) FROM ' + @fullTableName);
        //";

        //        var fullTableName = $"dbo.{tableName}";

        //        await using (var cmd = new SqlCommand(sql, conn))
        //        {
        //            cmd.Parameters.AddWithValue("@fullTableName", fullTableName); ;
        //            var exists = (int)await cmd.ExecuteScalarAsync();
        //            if (exists == 0)
        //            {
        //                Log.Warning("Table {TableName} does not exist or is not a base table.", tableName);
        //                return exists;
        //            }
        //            else
        //            {
        //                count = exists;
        //            }
        //        }
        //        return count;
        //    }

        //    catch (SqlException ex)
        //    {
        //        Log.Error(ex, "SQL error counting rows for table {TableName} in database {Database}",
        //            tableName, databaseName);
        //        throw; // let caller decide how to handle failure
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.Error(ex, "Unexpected error in SqlCountAsync");
        //        throw;
        //    }

        //}
        public async Task<int> SqlCountAsync(string databaseName, string objectName)
        {
            try
            {
                var connectionString = CustomerConnectionString(databaseName);

                await using var conn = new SqlConnection(connectionString);

                

                await conn.OpenAsync();

                var sql = $@"
            IF OBJECT_ID(N'dbo.[{objectName}]') IS NULL
                SELECT 0
            ELSE
                SELECT COUNT(*) FROM dbo.[{objectName}];
        ";

                await using var cmd = new SqlCommand(sql, conn);
                cmd.CommandTimeout = 120;
                

                return (int)await cmd.ExecuteScalarAsync();
            }
            catch (SqlException ex)
            {
                Log.Error(ex,
                    "SQL error counting rows for table/view {ObjectName} in database {Database}",
                    objectName, databaseName);
                throw;
            }
        }
        public async Task DeleteSingleTableAsync(string databaseName, string tableName)
        {
            var connectionString = CustomerConnectionString(databaseName);

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            // Validate table exists and is a base table
            var checkTableSql = @"
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = @tableName";

            await using (var cmd = new SqlCommand(checkTableSql, conn))
            {
                cmd.Parameters.AddWithValue("@tableName", tableName);
                var exists = (int)await cmd.ExecuteScalarAsync();
                if (exists == 0)
                {
                    Log.Warning("Table {TableName} does not exist or is not a base table.", tableName);
                    return;
                }
            }
            try
            {
                var deleteSql = $"DELETE FROM [dbo].[{tableName}]";
                await using var deleteCmd = new SqlCommand(deleteSql, conn);

                var rowsAffected = await deleteCmd.ExecuteNonQueryAsync();
                Log.Information("Deleted {RowsAffected} rows from table {TableName}", rowsAffected, tableName);
            }
            catch (Exception ex)
            {
                Log.Information(ex.Message);
            
        }
            }

        public async Task DeleteTablesWithPrefixAsync(string databaseName, string prefix, string dir, string group)
        {
            var connectionString = CustomerConnectionString(databaseName);

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            // Get all table names with the prefix
            var getTablesSql = @"SELECT TABLE_NAME
                                FROM DSD_API_LIST
                                WHERE  TABLE_NAME LIKE @prefix + '%' AND DIR = @dir AND RUNGROUP = @group";

            var tables = new List<string>();
            await using (var cmd = new SqlCommand(getTablesSql, conn))
            {
                cmd.Parameters.AddWithValue("@prefix", prefix);
                cmd.Parameters.AddWithValue("@dir", dir);
                cmd.Parameters.AddWithValue("@group", group);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    tables.Add(reader.GetString(0));
                }
            }

            // Delete data from each table
            foreach (var table in tables)
            {
                var deleteSql = $"DELETE FROM [{table}]";
                await using var deleteCmd = new SqlCommand(deleteSql, conn);
                await deleteCmd.ExecuteNonQueryAsync();
                Log.Information("Deleted all records from table {Table}", table);
            }
        }

        public async Task<List<TableApiName>> GetApiListAsync(string db, string group, string dir)
        {
            var tableApiNames = new List<TableApiName>();

            // Build connection string dynamically by replacing InitialCatalog
            string connectionString = CustomerConnectionString(db);

            // Use 'await using' for proper disposal of SqlConnection
            await using var conn = new SqlConnection(connectionString);

            try
            {
                // Log attempt to connect
                Log.Information("Attempting to connect to {Database} for API List", db);
                await conn.OpenAsync();
                Log.Information("Connected to database {Database}", conn.Database);

                // Base SQL query to retrieve API list
                string sql = @"SELECT [TABLE_NAME], [ENDPOINT], [FILTER], [BATCHSIZE]
                               FROM dsd_api_list
                               WHERE Dir = @dir AND RUNGROUP = @group
                               ORDER BY API_NAME";
                //if (group == "INBOUND")
                //{
                //    sql = @"SELECT [TABLE_NAME], [ENDPOINT], [FILTER], [BATCHSIZE]
                //               FROM dsd_api_list
                //               WHERE Dir = 'Inbound' AND RUNGROUP = @group
                //               ORDER BY API_NAME";
                //    group = "ALL";
                //}

                // Special case: If group starts with 'HFS', override query logic
                if (group.StartsWith("HFS"))
                {
                    sql = @"SELECT [TABLE_NAME], [ENDPOINT], [FILTER], [BATCHSIZE]
                            FROM dsd_api_list
                            WHERE Dir = @dir AND RUNGROUP = 'ALL' AND [TABLE_NAME] = @group";
                }

                // Prepare SQL command with parameterized query to prevent SQL injection
                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@group", group);
                cmd.Parameters.AddWithValue("@dir", dir);
                // Execute query and read results asynchronously
                await using var rdr = await cmd.ExecuteReaderAsync();
                while (await rdr.ReadAsync())
                {
                    var apiInfo = new TableApiName
                    {
                        tableName = rdr["TABLE_NAME"].ToString(),
                        APIname = rdr["ENDPOINT"].ToString(),
                        filter = rdr["FILTER"].ToString(),
                        batchSize = (int)rdr["BATCHSIZE"]
                    };
                    tableApiNames.Add(apiInfo);
                }

                return tableApiNames;
            }
            catch (Exception ex)
            {
                // Log error and rethrow for higher-level handling
                Log.Error(ex, "Failed to retrieve API list from database {Database}", db);
                throw;
            }
        }

        // ------------------------------------------------------------
        // GetAccessInfoAsync: Retrieves customer-specific access credentials
        // ------------------------------------------------------------
        // Parameters:
        //   customerCode - Unique identifier for the customer
        // Returns:
        //   AccessInfo object populated with API, FTP, and DB credentials
        public async Task<AccessInfo> GetAccessInfoAsync(string customerCode)
        {
            var accessInfo = new AccessInfo();

            // Base connection string for CustomerConnectionDB
            string connectionString = _config.GetConnectionString("CustomerConnectionDB");

            // PROD flag from appsettings.json (e.g., "Y" or "N")
            string prod = _config["prod"];

            // ------------------------------------------------------------
            // Define Polly retry policy for transient errors
            // ------------------------------------------------------------
            // Retries 3 times with exponential backoff (2s, 4s, 6s)
            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(2 * attempt),
                    (exception, timespan, attempt, context) =>
                    {
                        Log.Warning(exception, "Retry {Attempt} after {Delay}s due to error connecting to CustomerConnectionDB", attempt, timespan.TotalSeconds);
                    });

            // Execute DB logic with retry policy
            await retryPolicy.ExecuteAsync(async () =>
            {
                await using var conn = new SqlConnection(connectionString);
                try
                {
                    Log.Information("Attempting to connect to CustomerConnectionDB for customer {Customer}", customerCode);
                    await conn.OpenAsync();
                    Log.Information("Connected to database {Database}", conn.Database);

                    // SQL query to fetch customer info based on customer code and PROD flag
                    string sql = "SELECT * FROM DSD_CustomerInfo WHERE customer = @customer AND PROD = @prod";

                    await using var cmd = new SqlCommand(sql, conn);
                    cmd.Parameters.AddWithValue("@customer", customerCode);
                    cmd.Parameters.AddWithValue("@prod", prod);

                    // Execute query and read results
                    await using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        // Populate AccessInfo object with values from DB
                        accessInfo.Url = reader["Url"].ToString();
                        accessInfo.Grant_Type = reader["Grant_Type"].ToString();
                        accessInfo.Client_ID = reader["Client_ID"].ToString();
                        accessInfo.Scope = reader["Scope"].ToString();
                        accessInfo.Client_Secret = reader["Client_Secret"].ToString();
                        accessInfo.RootUrl = reader["RootUrl"].ToString();
                        accessInfo.ftpHost = reader["ftpHost"].ToString();
                        accessInfo.ftpUser = reader["ftpUser"].ToString();
                        accessInfo.ftpPass = reader["ftpPass"].ToString();
                        accessInfo.ftpRemoteFilePath = reader["ftpRemoteFilePath"].ToString();
                        accessInfo.ftpLocalFilePath = reader["ftpLocalFilePath"].ToString();
                        accessInfo.DataSource = reader["DataSource"].ToString();
                        accessInfo.InitialCatalog = reader["InitialCatalog"].ToString();
                        accessInfo.UserID = reader["UserID"].ToString();
                        accessInfo.Password = reader["Password"].ToString();
                        accessInfo.DayOffset = reader["DayOffset"].ToString();
                        accessInfo.email_tenantId = reader["email_tenantId"].ToString();
                        accessInfo.email_clientId = reader["email_clientid"].ToString();
                        accessInfo.email_secret = reader["email_secret"].ToString();
                        accessInfo.email_sender = reader["email_sender"].ToString();
                        accessInfo.email_recipient = reader["email_recipient"].ToString();

                        Log.Information("AccessInfo successfully retrieved for customer {Customer}", customerCode);
                    }
                    else
                    {
                        // No record found for customer
                        Log.Error("No customer info found for {Customer}", customerCode);
                        throw new Exception($"No customer info found for {customerCode}");
                    }
                }
                catch (Exception ex)
                {
                    // Log error and let Polly handle retry
                    Log.Error(ex, "Failed to retrieve AccessInfo for customer {Customer}", customerCode);
                    throw;
                }
            });

            return accessInfo;
        }


        public async Task<SqlExecResult> ExecuteDeleteAsync(
        string catalog,
        string sql,
        int retentionDays,
        int? commandTimeoutSeconds = null)
        {
            try
            {
                var connectionString = CustomerConnectionString(catalog);
                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    return new SqlExecResult(
                        Success: false,
                        RowsAffected: 0,
                        ErrorMessage: $"Connection string '{connectionString}' was not found.");
                }

                // If anything came from HTML (like &lt;), normalize it:
                sql = sql.Replace("&lt;", "<").Replace("&gt;", ">");

                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();

                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = commandTimeoutSeconds ?? 30;

                cmd.Parameters.Add(
                    new SqlParameter("@RetentionDays", SqlDbType.Int)
                    {
                        Value = retentionDays
                    });




                var rows = await cmd.ExecuteNonQueryAsync();

                // For DELETE, ExecuteNonQuery returns rows affected (or -1 for some statements)
                return new SqlExecResult(true, rows);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "ExecuteDeleteAsync failed. database Name={catalog}", catalog);
                return new SqlExecResult(false, 0, ex.Message);
            }
        }



        public void InsertCSV(string db, string filePath)
        {
            using (SqlConnection conn = new SqlConnection(CustomerConnectionString(db)))
            {
                conn.Open(); // ✅ Open once and keep it open
                Log.Information($"Connected to SQL Database {conn.Database}");

                string[] fileEntries = Directory.GetFiles(filePath, "*.csv");

                foreach (string fileEntry in fileEntries)
                {
                    string csvFileName = Path.GetFileName(fileEntry);
                    string fileName = Path.GetFileNameWithoutExtension(fileEntry);

                    // Normalize file name prefix (before "_")
                    if (fileName.Contains("_"))
                        fileName = fileName.Substring(0, fileName.IndexOf("_"));

                    string sqlTable = "CISOUT_" + fileName;

                    // ✅ Check if records from this file already exist
                    using (SqlCommand checkCmd = new SqlCommand($"SELECT COUNT(*) FROM {sqlTable} WHERE csvFile = @csvFile", conn))
                    {
                        checkCmd.Parameters.AddWithValue("@csvFile", csvFileName);
                        int existingRecords = (int)checkCmd.ExecuteScalar();

                        if (existingRecords > 0)
                        {
                            Log.Information($"{existingRecords} records exist from {csvFileName} in {sqlTable}. No new records added.");
                            continue; // Skip to next file
                        }
                    }

                    // ✅ Efficient record count
                    int recordCount = File.ReadLines(fileEntry).Count();
                    Console.WriteLine($"Total records: {recordCount}");

                    using (StreamReader sr = new StreamReader(fileEntry))
                    {
                        string sql = createSQL(sqlTable);
                        int batchCount = 0;

                        while (!sr.EndOfStream)
                        {
                            string cleanData = sr.ReadLine().Replace("'", "");
                            if (!sql.EndsWith("values"))
                                sql += ",";

                            sql += $"(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time','{csvFileName}','{cleanData.Replace("|", "','")}')";
                            batchCount++;

                            // ✅ Execute batch every 500 rows
                            if (batchCount >= 500)
                            {
                                executeSQL(sql, conn);
                                sql = createSQL(sqlTable); // Reset for next batch
                                batchCount = 0;
                            }
                        }

                        // ✅ Execute remaining rows if any
                        if (batchCount > 0)
                        {
                            executeSQL(sql, conn);
                        }
                    }

                    // ✅ Verify inserted count
                    using (SqlCommand verifyCmd = new SqlCommand($"SELECT COUNT(*) FROM {sqlTable} WHERE csvFile = @csvFile", conn))
                    {
                        verifyCmd.Parameters.AddWithValue("@csvFile", csvFileName);
                        int insertedCount = (int)verifyCmd.ExecuteScalar();

                        Log.Information($"Successfully added {insertedCount} records to {sqlTable} from {csvFileName} containing {recordCount} entries.");
                    }
                }
            }
        }

        //public async Task MergePerm(string db)
        //{
        //    using (SqlConnection conn = new SqlConnection(CustomerConnectionString(db)))
        //    {
        //        conn.Open(); // ✅ Open once and keep it open
        //        Log.Information($"Connected to SQL Database {conn.Database}");

        //        string mergeSql = @"MERGE dbo.DSD_PERM AS tgt
        //                        USING dbo.CISOUT_INVEDYNA AS src
        //                            ON  tgt.CustomerNumber = src.Customer
        //                            AND tgt.ItemNumber     = src.product
        //                            AND tgt.DayofWeek      = src.Day_deliv

        //                        WHEN MATCHED
        //                             AND tgt.Quantity <> src.Quantity
        //                            THEN UPDATE
        //                                 SET tgt.Quantity = src.Quantity

        //                        WHEN NOT MATCHED BY TARGET
        //                            THEN INSERT (CustomerNumber, ItemNumber, DayofWeek, Quantity)
        //                                 VALUES (src.Customer, src.product, src.Day_deliv, src.Quantity);";

        //        //await using var mergeCmd = new SqlCommand(mergeSql, conn);
        //        //var rowsAffected = await mergeCmd.ExecuteNonQueryAsync();
        //        //Log.Information("Merged {RowsAffected} rows from table DSD_Perm", rowsAffected);
        //        using var tx = conn.BeginTransaction();
        //        try
        //        {
        //            using var cmd = new SqlCommand(mergeSql, conn, tx)
        //            {
        //                CommandType = CommandType.Text,
        //                CommandTimeout = 0 // optional if large volume
        //            };

        //            int rowsAffected = cmd.ExecuteNonQuery();
        //            tx.Commit();
        //            Log.Information("Merged {RowsAffected} rows from table DSD_Perm", rowsAffected);
        //        }
        //        catch(Exception ex)
        //        {
        //            Log.Information(ex.Message);
        //            tx.Rollback();
        //            throw;
        //        }


        //    }
        //}


        public async Task<List<string>> GetOutboundTableNamesAsync(string catalog)
        {
            const string sql = @"
        SELECT DISTINCT TABLE_NAME
        FROM dbo.DSD_API_LIST
        ORDER BY TABLE_NAME;";

            var results = new List<string>();

            // Use the DB where DSD_API_LIST lives
            var connectionString = CustomerConnectionString(catalog);

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            await using var cmd = new SqlCommand(sql, conn);
            await using var rdr = await cmd.ExecuteReaderAsync();

            while (await rdr.ReadAsync())
            {
                results.Add(rdr.GetString(0));
            }

            return results;
        }
        public async Task<List<string>> GetCsvNamesAsync(string catalog)
        {
            const string sql = @"
        SELECT name
        FROM sys.views
        WHERE name like 'CIS_%'
        ORDER BY NAME;";

            var results = new List<string>();

            // Use the DB where DSD_API_LIST lives
            var connectionString = CustomerConnectionString(catalog);

            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            await using var cmd = new SqlCommand(sql, conn);
            await using var rdr = await cmd.ExecuteReaderAsync();

            while (await rdr.ReadAsync())
            {
                results.Add(rdr.GetString(0));
            }

            return results;
        }
        string CustomerConnectionString(string catalog)
            {
                string connectionString = _config.GetConnectionString("CustomerConnectionDB")
                                                 .Replace("CustomerConnection", catalog);
                return connectionString;
            }
            void executeSQL(String SQL, SqlConnection conn)
            {
                //using (conn)
                //{
                    try
                    {
                        SqlCommand cmd = new SqlCommand(SQL, conn);
                        //conn.Open();
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Log.Information(SQL);
                        Log.Information(ex.Message);

                    }

                //}
            }
            String createSQL(String tableName)
            {
                String SQL = "";
                SQL = "INSERT INTO " + tableName + " values";
                return SQL;

            }

        //public async Task ScheduleJobsAsync(  int dayOfWeekInt, string dateValue)
        //{
        //    try
        //    {
        //        Log.Information("Starting job scheduling for   DayOfWeek: {DayOfWeek}, DateValue: {DateValue}",
        //              dayOfWeekInt, dateValue);

        //        using (var connection = new SqlConnection(_config.GetConnectionString("CustomerConnectionDB")))
        //        {
        //            await connection.OpenAsync();
        //            Log.Information("SQL connection established successfully.");

        //        //string query = _config["SQL"];



        //            using (var command = new SqlCommand("dbo.Insert_DSD_Job_Log", connection))
        //            {


        //            command.Parameters.Add("@DateValue", SqlDbType.Date)
        //                          .Value = dateValue;

        //            command.Parameters.Add("@DayOfWeekInt", SqlDbType.Int)
        //                          .Value = dayOfWeekInt;



        //            int rowsAffected = await command.ExecuteNonQueryAsync();

        //                if (rowsAffected > 0)
        //                {
        //                    Log.Information("Job successfully scheduled {rowsAffected} jobs for {dateValue}.", dateValue , rowsAffected);
        //                }
        //                else
        //                {
        //                    Log.Warning("No rows were inserted for {dateValue}.", dateValue);
        //                }
        //            }
        //        }
        //    }
        //    catch (SqlException ex)
        //    {
        //        Log.Error(ex, "SQL error occurred while scheduling jobs.");
        //        throw; // Optionally rethrow or handle gracefully
        //    }
        //    catch (InvalidOperationException ex)
        //    {
        //        Log.Error(ex, "Database connection error occurred.");
        //        throw;
        //    }
        //    catch (Exception ex)
        //    {
        //        Log.Error(ex, "An unexpected error occurred while inserting scheduling data.");
        //        throw;
        //    }
        //}
        public async Task ScheduleJobsAsync(int dayOfWeekInt, string dateValue)
        {
            try
            {
                Log.Information("Starting job scheduling for DayOfWeek: {DayOfWeek}, DateValue: {DateValue}",
                                dayOfWeekInt, dateValue);

                // Parse once, pass as Date (prevents culture/format problems)
                var date = DateTime.Parse(dateValue).Date;

                using var connection = new SqlConnection(_config.GetConnectionString("CustomerConnectionDB"));
                await connection.OpenAsync();
                Log.Information("SQL connection established successfully.");

                using var command = new SqlCommand("dbo.Insert_DSD_Job_Log", connection);
                command.CommandType = CommandType.StoredProcedure;

                command.Parameters.Add("@DateValue", SqlDbType.Date).Value = date;
                command.Parameters.Add("@DayOfWeekInt", SqlDbType.Int).Value = dayOfWeekInt;

                int rowsAffected = await command.ExecuteNonQueryAsync();

                if (rowsAffected > 0)
                {
                    Log.Information("Job successfully scheduled {RowsAffected} jobs for {DateValue}.",
                                    rowsAffected, dateValue);
                }
                else
                {
                    Log.Warning("No rows were inserted for {DateValue}.", dateValue);
                }
            }
            catch (SqlException ex)
            {
                Log.Error(ex, "SQL error occurred while scheduling jobs.");
                throw;
            }
            catch (InvalidOperationException ex)
            {
                Log.Error(ex, "Database connection error occurred.");
                throw;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "An unexpected error occurred while inserting scheduling data.");
                throw;
            }
        }
    }
    }





