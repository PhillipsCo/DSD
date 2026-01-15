
using Microsoft.Data.SqlClient; // Using Microsoft.Data.SqlClient for SQL Server connectivity
using Microsoft.Extensions.Configuration;
using Serilog; // Serilog for structured logging
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace DSD.Common.Services
{
    /// <summary>
    /// CsvExportService is responsible for exporting data from SQL tables into CSV files.
    /// It retrieves all tables matching the CIS_* pattern (excluding CISOUT_*),
    /// reads their data, and writes each table's content into a separate CSV file.
    /// </summary>
    public class CsvExportService
    {
        private readonly IConfiguration _config;

        /// <summary>
        /// Constructor that injects IConfiguration for reading settings from appsettings.json.
        /// </summary>
        /// <param name="config">Application configuration object.</param>
        public CsvExportService(IConfiguration config)
        {
            _config = config;
        }

        /// <summary>
        /// Generates CSV files for all CIS_* tables in the specified database.
        /// </summary>
        /// <param name="databaseName">The database name to connect to.</param>
        /// <param name="outputPath">The base directory where CSV files will be stored.</param>
        public async Task GenerateCsvFilesAsync(string databaseName, string outputPath)
        {
            try
            {
                // Log the start of the process
                Log.Information("Starting CSV export process. Output path: {OutputPath}", outputPath);

                // Build the connection string dynamically by replacing placeholder with actual database name
                var connectionString = _config.GetConnectionString("CustomerConnectionDB")
                                              .Replace("CustomerConnection", databaseName);

                // List to hold table names that match the CIS_* pattern
                var tables = new List<string>();

                // Establish a connection to the SQL Server database
                using (var conn = new SqlConnection(connectionString))
                {
                    await conn.OpenAsync();
                    Log.Information("Connected to database successfully.");

                    // SQL query to retrieve all table names starting with 'CIS_' but not 'CISOUT_'
                    var sql = @"SELECT TABLE_NAME 
                                FROM INFORMATION_SCHEMA.COLUMNS 
                                WHERE TABLE_NAME LIKE 'CIS_%' 
                                  AND TABLE_NAME NOT LIKE 'CISOUT_%' 
                                GROUP BY TABLE_NAME";

                    // Execute the query to get table names
                    using (var cmd = new SqlCommand(sql, conn))
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var tableName = reader.GetString(0);
                            tables.Add(tableName);
                            Log.Debug("Found table: {TableName}", tableName);
                        }
                    }

                    // If no tables are found, log a warning and exit
                    if (tables.Count == 0)
                    {
                        Log.Warning("No CIS tables found for export.");
                        return;
                    }

                    // Create a directory for today's date under the specified output path

                    var dirPath = Path.Combine(outputPath, "Outbound", DateTime.Now.ToString("yyyyMMdd"));
                    Directory.CreateDirectory(dirPath);
                    Log.Information("Created output directory: {Directory}", dirPath);

                    // Iterate through each table and export its data to a CSV file
                    foreach (var table in tables)
                    {
                        Log.Information("Exporting table {TableName} to CSV...", table);

                        // Query to select all rows from the current table
                        var dataSql = $"SELECT * FROM {table}";
                        using (var cmd = new SqlCommand(dataSql, conn))
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            // List to hold all rows as strings
                            var records = new List<string>();

                            // Read each row and convert it to a pipe-delimited string
                            while (await reader.ReadAsync())
                            {
                                var values = new object[reader.FieldCount];
                                reader.GetValues(values);
                                records.Add(string.Join("|", values));
                            }

                            // Build the file name by removing 'CIS_' prefix and adding .csv extension
                            var fileName = table.Replace("CIS_", "") + ".csv";
                            var filePath = Path.Combine(dirPath, fileName);

                            //// Write all records to the CSV file
                            //await File.WriteAllLinesAsync(filePath, records);

                            // Only create the file if there are records
                            if (records.Count > 0)
                            {
                                await File.WriteAllLinesAsync(filePath, records);
                                Log.Information("CSV file created: {FilePath} with {RecordCount} records", filePath, records.Count);
                            }
                            else
                            {
                                Log.Warning("No records found in table {TableName}. Skipping file creation.", table);
                            }

                            // Log file creation details
                            Log.Information("CSV file created: {FilePath} with {RecordCount} records", filePath, records.Count);
                        }
                    }
                }

                // Log completion of the process
                Log.Information("CSV export process completed successfully.");
            }
            catch (Exception ex)
            {
                // Log any exceptions that occur during the process
                Log.Error(ex, "Error occurred during CSV export process.");
                throw; // Rethrow to allow higher-level handling
            }
        }
    }
}

