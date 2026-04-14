
using Microsoft.Data.SqlClient; // Using Microsoft.Data.SqlClient for SQL Server connectivity
using Microsoft.Extensions.Configuration;
using Serilog; // Serilog for structured logging
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
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


        public async Task<string?> ExportViewToCsvAsync(
            string databaseName,
            string viewName,
            string outputPath,
            char delimiter = '|',
            bool includeHeader = false,
            CancellationToken cancellationToken = default)
        {
            try
            {
                Log.Information("Starting single view CSV export. View: {ViewName}. Output path: {OutputPath}", viewName, outputPath);

                var connectionString = _config.GetConnectionString("CustomerConnectionDB")
                                              .Replace("CustomerConnection", databaseName);

                // Create a directory for today's date under the specified output path
                var dirPath = Path.Combine(outputPath, "Outbound", DateTime.Now.ToString("yyyyMMdd"));
                Directory.CreateDirectory(dirPath);
                Log.Information("Ensured output directory: {Directory}", dirPath);

                // Validate + quote the view name safely (supports schema-qualified names like dbo.MyView)
                var quotedViewName = QuoteMultipartIdentifierOrThrow(viewName);

                // Build file name using the last identifier part; remove CIS_ prefix if present (matches your current naming behavior)
                var baseName = viewName.Split('.', StringSplitOptions.RemoveEmptyEntries).Last();
                if (baseName.StartsWith("CIS_", StringComparison.OrdinalIgnoreCase))
                    baseName = baseName.Substring("CIS_".Length);

                var filePath = Path.Combine(dirPath, baseName + ".csv");

                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(cancellationToken);
                Log.Information("Connected to database successfully.");

                var sql = $"SELECT * FROM {quotedViewName};";

                using var cmd = new SqlCommand(sql, conn)
                {
                    CommandType = CommandType.Text,
                    CommandTimeout = 0
                };

                using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

                // If the view returns no rows, optionally skip file creation (similar to your group method)
                if (!reader.HasRows)
                {
                    Log.Warning("No records found in view {ViewName}. Skipping file creation.", viewName);
                    return null;
                }

                // Stream rows to file (avoids storing everything in memory)
                await using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.Read, 64 * 1024, useAsync: true);
                await using var writer = new StreamWriter(fs, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

                if (includeHeader)
                {
                    var header = string.Join(delimiter, Enumerable.Range(0, reader.FieldCount).Select(i => EscapeDelimited(reader.GetName(i), delimiter)));
                    await writer.WriteLineAsync(header);
                }

                long rowCount = 0;
                while (await reader.ReadAsync(cancellationToken))
                {
                    var line = new StringBuilder();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (i > 0) line.Append(delimiter);

                        object? value = reader.IsDBNull(i) ? null : reader.GetValue(i);

                        // Format values consistently (dates/numbers), then escape for delimiter/quotes/newlines
                        string formatted = FormatValue(value);
                        line.Append(EscapeDelimited(formatted, delimiter));
                    }

                    await writer.WriteLineAsync(line.ToString());
                    rowCount++;
                }

                await writer.FlushAsync();
                Log.Information("CSV file created: {FilePath} with {RecordCount} records", filePath, rowCount);

                return filePath;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error occurred during single view CSV export. View: {ViewName}", viewName);
                throw;
            }
        }

        private static string FormatValue(object? value)
        {
            if (value is null) return string.Empty;

            // Keep formatting predictable across locales
            return value switch
            {
                DateTime dt => dt.ToString("o", CultureInfo.InvariantCulture),        // ISO 8601
                DateTimeOffset dto => dto.ToString("o", CultureInfo.InvariantCulture),
                IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
                _ => value.ToString() ?? string.Empty
            };
        }

        /// <summary>
        /// Escapes a value for delimiter-separated output (CSV rules apply even if delimiter is '|').
        /// If value contains delimiter, quote, CR, or LF, wrap in quotes and double any quotes.
        /// </summary>
        private static string EscapeDelimited(string value, char delimiter)
        {
            if (value is null) return string.Empty;

            bool mustQuote = value.IndexOfAny(new[] { delimiter, '"', '\r', '\n' }) >= 0;
            if (!mustQuote) return value;

            // Double the quotes inside the value, then wrap the whole thing in quotes
            var escaped = value.Replace("\"", "\"\"");
            return $"\"{escaped}\"";
        }

        /// <summary>
        /// Validates a multipart identifier (e.g., dbo.MyView) and quotes each part: [dbo].[MyView].
        /// This prevents injection via object name because you only allow normal SQL identifier chars.
        /// </summary>
        private static string QuoteMultipartIdentifierOrThrow(string multipartName)
        {
            if (string.IsNullOrWhiteSpace(multipartName))
                throw new ArgumentException("View name is required.", nameof(multipartName));

            var parts = multipartName.Split('.', StringSplitOptions.RemoveEmptyEntries);

            // Support 1-part (MyView) or 2-part (dbo.MyView)
            if (parts.Length is < 1 or > 2)
                throw new ArgumentException("View name must be 'ViewName' or 'Schema.ViewName'.", nameof(multipartName));

            foreach (var p in parts)
            {
                // Basic identifier whitelist: letters, digits, underscore; must not start with digit.
                // (You can widen this if your naming rules allow more characters.)
                if (!IsValidSqlIdentifier(p))
                    throw new ArgumentException($"Invalid identifier part '{p}' in view name.", nameof(multipartName));
            }

            return string.Join(".", parts.Select(BracketQuote));
        }

        private static bool IsValidSqlIdentifier(string name)
        {
            if (string.IsNullOrEmpty(name)) return false;
            if (!(char.IsLetter(name[0]) || name[0] == '_')) return false;

            for (int i = 1; i < name.Length; i++)
            {
                char c = name[i];
                if (!(char.IsLetterOrDigit(c) || c == '_')) return false;
            }
            return true;
        }

        private static string BracketQuote(string identifier)
        {
            // bracket-escape any closing bracket (rare with our whitelist, but safe)
            return $"[{identifier.Replace("]", "]]")}]";
        }

    }
}

