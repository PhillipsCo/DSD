
using DSD.Common.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Polly;
using Serilog;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace DSD.Common.Services
{
    public class ApiExecutorService
    {
        // ------------------------------------------------------------
        // Private fields for dependencies and token management
        // ------------------------------------------------------------
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IConfiguration _config;
        private string _accessToken;
        private DateTime _tokenExpiry;
        private readonly Random _jitter = new Random();

        // ------------------------------------------------------------
        // Constructor
        // ------------------------------------------------------------
        public ApiExecutorService(IHttpClientFactory httpClientFactory, IConfiguration config)
        {
            _httpClientFactory = httpClientFactory;
            _config = config;
        }

        // ------------------------------------------------------------
        // ExecuteApisAndInsertAsync: Main entry point
        // ------------------------------------------------------------
        public async Task ExecuteApisAndInsertAsync(List<TableApiName> apiList, AccessInfo accessInfo)
        {
            var connectionString = _config.GetConnectionString("CustomerConnectionDB")
                                          .Replace("CustomerConnection", accessInfo.InitialCatalog);

            var client = _httpClientFactory.CreateClient("ApiClient");
            client.Timeout = TimeSpan.FromSeconds(120);

            using var globalCts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
            var globalToken = globalCts.Token;

            await EnsureAccessTokenAsync(client, accessInfo, globalToken);

            foreach (var apiInfo in apiList)
            {
                await ProcessApiSync(client, apiInfo, accessInfo, connectionString, globalToken);
            }

            //var tasks = apiList.Select(apiInfo => ProcessApiAsync(client, apiInfo, accessInfo, connectionString, globalToken));
            //await Task.WhenAll(tasks);
        }

        // ------------------------------------------------------------
        // UpdateCriteria: Replace placeholders dynamically
        // ------------------------------------------------------------
        public string UpdateCriteria(string criteria, AccessInfo accessInfo)
        {
            if (criteria != "N")
            {
                criteria = criteria.Replace("SHIPDATE", DateTime.Now.AddDays(Convert.ToInt32(accessInfo.DayOffset)).ToString("yyyy-MM-dd"))
                                   .Replace("ENDDATE", DateTime.Now.AddDays(Convert.ToInt32(accessInfo.DayOffset) + 7).ToString("yyyy-MM-dd"));

                string dow = DateTime.Today.AddDays(Convert.ToInt32(accessInfo.DayOffset)).DayOfWeek.ToString();
                criteria = criteria.Replace("xxxdowxxx", dow);

                DateTime orderdt = GetDateBasedOn1300();
                criteria = criteria.Replace("xxxorderdatexxx", orderdt.ToString("yyyy-MM-dd"));

                DateTime weekAgo = GetDateAWeekAgo();
                criteria = criteria.Replace("xxxpostdatexxx", weekAgo.ToString("yyyy-MM-dd"));

            }
            else
            {
                criteria = string.Empty;
            }

            Log.Information("Criteria updated to: {UpdatedCriteria}", criteria);
            return criteria;
        }

        private DateTime GetDateBasedOn1300()
        {
            var now = DateTime.Now;
            return now.Hour < 13 ? now.Date : now.Date.AddDays(1);
        }

        // ------------------------------------------------------------
        // EnsureAccessTokenAsync: Fetch OAuth token
        // ------------------------------------------------------------
        private async Task EnsureAccessTokenAsync(HttpClient client, AccessInfo accessInfo, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(_accessToken) || DateTime.UtcNow >= _tokenExpiry.AddMinutes(-3))
            {
                Log.Information("Fetching new access token...");

                var formData = new Dictionary<string, string>
                {
                    { "client_id", accessInfo.Client_ID },
                    { "client_secret", accessInfo.Client_Secret },
                    { "scope", accessInfo.Scope },
                    { "grant_type", accessInfo.Grant_Type }
                };

                var retryPolicy = Policy
                    .Handle<HttpRequestException>()
                    .OrResult<HttpResponseMessage>(r => (int)r.StatusCode >= 500 || r.StatusCode == HttpStatusCode.TooManyRequests)
                    .WaitAndRetryAsync(
                        3,
                        attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(_jitter.Next(0, 500)),
                        (outcome, timespan, attempt, context) =>
                        {
                            Log.Warning(outcome.Exception, "Retry {Attempt} after {Delay}s for token request", attempt, timespan.TotalSeconds);
                        });

                await retryPolicy.ExecuteAsync(async () =>
                {
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    cts.CancelAfter(TimeSpan.FromSeconds(30));

                    var response = await client.PostAsync(accessInfo.Url, new FormUrlEncodedContent(formData), cts.Token);
                    response.EnsureSuccessStatusCode();

                    var json = await response.Content.ReadAsStringAsync(cancellationToken);
                    var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(json);

                    _accessToken = tokenResponse.access_token;
                    _tokenExpiry = DateTime.UtcNow.AddSeconds(tokenResponse.expires_in);

                    Log.Information("Access token acquired, expires at {Expiry}", _tokenExpiry);
                    return response;
                });
            }
        }

        // ------------------------------------------------------------
        // ProcessApiAsync: Execute API and insert data
        // ------------------------------------------------------------

        private async Task ProcessApiSync(HttpClient client, TableApiName apiInfo, AccessInfo accessInfo, string connectionString, CancellationToken cancellationToken)
        {
            int skip = 0;
            int batchSize = (int)apiInfo.batchSize;
            bool hasData = true;
            int iteration = 0;
            int maxIterations = 100; // Safety limit

            var retryPolicy = Policy
                .Handle<HttpRequestException>()
                .OrResult<HttpResponseMessage>(r => (int)r.StatusCode >= 500 || r.StatusCode == HttpStatusCode.TooManyRequests)
                .WaitAndRetryAsync(
                    3,
                    attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(_jitter.Next(0, 500)),
                    (outcome, timespan, attempt, context) =>
                    {
                        Log.Warning(outcome.Exception, "Retry {Attempt} after {Delay}s for API call", attempt, timespan.TotalSeconds);
                    });

            while (hasData && !cancellationToken.IsCancellationRequested && iteration++ < maxIterations)
            {
                if (DateTime.UtcNow >= _tokenExpiry.AddMinutes(-3))
                {
                    Log.Information("Token nearing expiry, refreshing...");
                    await EnsureAccessTokenAsync(client, accessInfo, cancellationToken);
                }

                string updatedCriteria = UpdateCriteria(apiInfo.filter, accessInfo);
                var url = $"{apiInfo.APIname}?$top={batchSize}&$skip={skip}";
                if (!string.IsNullOrEmpty(updatedCriteria)) url += updatedCriteria;

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
                Log.Information("Fetching URL: {Url}", url);

                // Execute synchronously in sequence
                var response = await retryPolicy.ExecuteAsync(async () =>
                {
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    cts.CancelAfter(TimeSpan.FromSeconds(60));

                    var resp = await client.GetAsync(url, cts.Token);

                    if (resp.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        Log.Warning("Token expired during API call, refreshing...");
                        await EnsureAccessTokenAsync(client, accessInfo, cancellationToken);
                        resp = await client.GetAsync(url, cts.Token);
                    }

                    resp.EnsureSuccessStatusCode();
                    return resp;
                });

                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                json = Regex.Replace(json, @"\](?=.*\}\]\})", " ");
                json = json.Replace("P[LAIN CITY", "PLAIN CITY");
                json = json.Split('[', ']')[1];
                json = json.Replace("'", "''").Replace("_x0020_", "");
                json = "[" + json + "]";
                if (response.StatusCode == HttpStatusCode.OK && json != "[]")
                {
                    await InsertJsonIntoSqlAsync(connectionString, apiInfo.tableName, json, cancellationToken);
                    skip += batchSize;
                }
                else
                {
                    hasData = false;
                }



            }

            if (iteration >= maxIterations)
            {
                Log.Warning("Max iterations reached for API {ApiName}. Possible pagination issue.", apiInfo.APIname);
            }
        }



        // ------------------------------------------------------------
        // InsertJsonIntoSqlAsync: Insert JSON into SQL
        // ------------------------------------------------------------
        private async Task InsertJsonIntoSqlAsync(string connectionString, string tableName, string json, CancellationToken cancellationToken)
        {
            try
            {
                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(cancellationToken);

                string dictionarySql = @"
                    SELECT COLUMNNAME, JSONNAME
                    FROM DSD_API_DICTIONARY
                    WHERE TableName = @tableName";

                var mappings = new List<(string ColumnName, string JsonPath)>();

                await using (var dictCmd = new SqlCommand(dictionarySql, conn))
                {
                    dictCmd.Parameters.AddWithValue("@tableName", tableName);
                    await using var reader = await dictCmd.ExecuteReaderAsync(cancellationToken);

                    while (await reader.ReadAsync())
                    {
                        mappings.Add((reader["COLUMNNAME"].ToString(), reader["JSONNAME"].ToString()));
                    }
                }

                if (!mappings.Any())
                {
                    Log.Error("No column mappings found for table {TableName}", tableName);
                    throw new Exception($"API Dictionary has no mappings for {tableName}");
                }

                var withClause = string.Join(",\n", mappings.Select(m =>
                    $"[{m.ColumnName}] VARCHAR(100) '$.{m.JsonPath}'"));

                string sql = $@"
                    INSERT INTO [{tableName}] ({string.Join(", ", mappings.Select(m => $"[{m.ColumnName}]"))})
                    SELECT {string.Join(", ", mappings.Select(m => $"[{m.ColumnName}]"))}
                    FROM OPENJSON(@json)

                    WITH (
                        {withClause}
                    );";

                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@json", json);

                int rowsAffected = await cmd.ExecuteNonQueryAsync(cancellationToken);
                Log.Information("{RowsAffected} rows inserted into {TableName}", rowsAffected, tableName);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to insert JSON into {TableName} {json}", tableName, json);
                throw;
            }
        }

        // ------------------------------------------------------------
        // TokenResponse: Model for OAuth token response
        // ------------------------------------------------------------
        public class TokenResponse
        {
            public string access_token { get; set; }
            public int expires_in { get; set; }
        }
        static DateTime GetDateAWeekAgo()
        {
            DateTime now = DateTime.Now;
            return now.Date.AddDays(-8);
        }
    }
}

