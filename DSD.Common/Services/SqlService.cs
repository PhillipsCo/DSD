
using DSD.Common.Models;
//using DSD.Outbound.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Polly;
using Serilog;

namespace DSD.Common.Services
{
    public class SqlService
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

            var deleteSql = $"DELETE FROM [{tableName}]";
            await using var deleteCmd = new SqlCommand(deleteSql, conn);
            var rowsAffected = await deleteCmd.ExecuteNonQueryAsync();
            Log.Information("Deleted {RowsAffected} rows from table {TableName}", rowsAffected, tableName);
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

        public void InsertCSV(string db, string filePath)
        {
            using (SqlConnection conn = new SqlConnection(CustomerConnectionString(db)))
            {

                //TruncateCISOUT();
                conn.Open();
                Log.Information($"Connected to SQL Database {conn.Database}");

                string[] fileEntries = Directory.GetFiles(filePath);
                int firstFile = 1;
                String preFile = "XXX";
                foreach (string fileEntry in fileEntries)
                {
                    String csvFileName = Path.GetFileName(fileEntry);
                    String fileName = Path.GetFileName(fileEntry).Replace(".CSV", "");
                    if (fileName.Contains("_"))
                        fileName = fileName.Substring(0, fileName.IndexOf("_"));
                    if (preFile == fileName)
                        firstFile++;
                    else
                        firstFile = 1;
                    preFile = fileName;
                    string SQLtable = "CISOUT_" + fileName;
                    var linecount = File.ReadAllLines(fileEntry);
                    int recordCount = linecount.Length;
                    Console.WriteLine($"Total records: {recordCount}");
                    StreamReader sr = new StreamReader(fileEntry);
                    string SQL = string.Empty;
                    int lines = 0;
                    Int32 existingRecords = 0;
                    SqlCommand comm1 = new SqlCommand($"SELECT COUNT(*) FROM {SQLtable} WHERE csvFile = '{csvFileName}'", conn);
                    existingRecords = (Int32)comm1.ExecuteScalar();
                    while (!sr.EndOfStream)
                    {

                        if (existingRecords == 0)
                        {
                            if (lines == 0)
                                SQL = createSQL(SQLtable);
                            lines++;
                            if (!SQL.EndsWith("values"))
                                SQL += ",";
                            String cleandata = sr.ReadLine().Replace("'", "");
                            SQL += $"(getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time','{csvFileName}','{cleandata.Replace("|", "','")}')";
                            if (lines > 500)
                            {

                                executeSQL(SQL, conn);
                                lines = 0;

                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (existingRecords == 0)
                    {
                        executeSQL(SQL, conn);
                        SqlCommand comm = new SqlCommand($"SELECT COUNT(*) FROM {SQLtable} WHERE csvFile = '{csvFileName}'", conn);
                        Int32 count = (Int32)comm.ExecuteScalar();

                        Log.Information($"Successfully added {count.ToString()} records to the {SQLtable} from {csvFileName} containing {recordCount} entries");

                    }
                    else
                    {

                        Log.Information($"{existingRecords.ToString()} records exist from {csvFileName} in {SQLtable} already 0 more records added");

                    }
                    }
                }
            }
        
            string CustomerConnectionString(string catalog)
            {
                string connectionString = _config.GetConnectionString("CustomerConnectionDB")
                                                 .Replace("CustomerConnection", catalog);
                return connectionString;
            }
            void executeSQL(String SQL, SqlConnection conn)
            {
                using (conn)
                {
                    try
                    {
                        SqlCommand cmd = new SqlCommand(SQL, conn);
                        conn.Open();
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        Log.Information(SQL);
                        Log.Information(ex.Message);

                    }

                }
            }
            String createSQL(String tableName)
            {
                String SQL = "";
                SQL = "INSERT INTO " + tableName + " values";
                return SQL;

            }

        }
    } 


