using DSD.Common.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Polly;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;                 // For Stream/StreamReader when streaming HTTP responses
using System.Linq;
using System.Net;               // For HttpStatusCode
using System.Net.Http;          // For HttpClient, HttpRequestMessage, HttpCompletionOption
using System.Net.Http.Headers;  // For AuthenticationHeaderValue
using System.Net.Sockets;       // For SocketException (transient network faults)
using System.Text.Json;         // For JsonSerializer
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace DSD.Common.Services
{
    /// <summary>
    /// Executes paged API calls, transforms the JSON payload, and inserts into SQL tables.
    /// This version is hardened against transient network faults and timeouts:
    /// - Uses IHttpClientFactory + SocketsHttpHandler (configured in DI) for robust pooling
    /// - Avoids HttpClient.Timeout; uses per-request CancellationTokenSource instead
    /// - Uses HttpRequestMessage + SendAsync with ResponseHeadersRead to stream responses
    /// - Forces HTTP/1.1 initially for stability across proxies/LBs (toggle later if H2 is stable)
    /// - Retries on transient errors (5xx/429, IO/Socket, HttpClient timeouts)
    /// - Disposes HttpResponseMessage objects promptly to free sockets
    /// </summary>
    public class ApiExecutorService
    {
        // ------------------------------------------------------------
        // Dependencies and token state
        // ------------------------------------------------------------

        private readonly IHttpClientFactory _httpClientFactory; // Factory-created clients share a tuned handler pool
        private readonly IConfiguration _config;                // For connection strings and app settings

        // OAuth token and expiry; refreshed proactively
        private string _accessToken;
        private DateTime _tokenExpiry;

        // Jitter source for exponential backoff to prevent thundering herds
        private readonly Random _jitter = new Random();

        // ------------------------------------------------------------
        // Constructor
        // ------------------------------------------------------------

        private readonly int _globalTimeoutMinutes;
        private readonly int _maxIterations;

        public ApiExecutorService(IHttpClientFactory httpClientFactory, IConfiguration config)
        {
            _httpClientFactory = httpClientFactory;
            _config = config;

            _globalTimeoutMinutes = _config.GetValue<int>("ApiExecutorConfig:GlobalTimeoutMinutes", 30);
            _maxIterations = _config.GetValue<int>("ApiExecutorConfig:MaxIterations", 100);

        }

        // ------------------------------------------------------------
        // ExecuteApisAndInsertAsync: Main entry point
        // ------------------------------------------------------------
        /// <summary>
        /// Orchestrates token acquisition and sequential execution of multiple API definitions.
        /// Uses a global CTS to bound the total run time (10 minutes here).
        /// </summary>
        public async Task ExecuteApisAndInsertAsync(List<TableApiName> apiList, AccessInfo accessInfo)
        {
            // Build the target database connection string dynamically
            var connectionString = _config.GetConnectionString("CustomerConnectionDB")
                                          .Replace("CustomerConnection", accessInfo.InitialCatalog);

            // Get the named client ("ApiClient") configured in Program.cs / Startup.cs
            // DO NOT set client.Timeout; we control per-request timeouts via CTS below.
            var client = _httpClientFactory.CreateClient("ApiClient");

            // Global boundary for the whole run (prevents runaway jobs)
            using var globalCts = new CancellationTokenSource(TimeSpan.FromMinutes(_globalTimeoutMinutes));
            var globalToken = globalCts.Token;

            // Ensure we start with a valid access token
            await EnsureAccessTokenAsync(client, accessInfo, globalToken);

            // Sequential execution (safe default); can be parallelized if the backend allows
            foreach (var apiInfo in apiList)
            {
                await ProcessApiSync(client, apiInfo, accessInfo, connectionString, globalToken);
            }

            // NOTE: If you switch to parallel execution (Task.WhenAll),
            // keep per-request CTS and retries; ensure the upstream API supports the load.
        }

        // ------------------------------------------------------------
        // Criteria Helpers
        // ------------------------------------------------------------

        /// <summary>
        /// Dynamically replaces placeholders in the filter string with computed values.
        /// </summary>
        public string UpdateCriteria(string criteria, AccessInfo accessInfo)
        {
            if (criteria != "N")
            {
                // Replace date placeholders; these are business-specific rules
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
                criteria = string.Empty; // "N" means no filter
            }

            Log.Information("Criteria updated to: {UpdatedCriteria}", criteria);
            return criteria;
        }

        /// <summary>
        /// If current local time is before 13:00 (1 PM), returns today; otherwise tomorrow.
        /// </summary>
        private DateTime GetDateBasedOn1300()
        {
            var now = DateTime.Now;
            return now.Hour < 13 ? now.Date : now.Date.AddDays(1);
        }

        /// <summary>
        /// Returns the date 8 days in the past (business-specific).
        /// </summary>
        private static DateTime GetDateAWeekAgo()
        {
            DateTime now = DateTime.Now;
            return now.Date.AddDays(-8);
        }
        /// <summary>
        /// Ensures a valid OAuth access token exists. When the current token is
        /// missing or within 3 minutes of expiry, fetch a fresh one.
        /// 
        /// Resiliency:
        /// - Uses a short per-request CTS (30s) so this call cannot hang.
        /// - Non-generic Polly policy (no return value required from the lambda),
        ///   avoiding "not all code paths return a value" compile errors.
        /// - Retries transient faults: HttpRequestException, IOException, SocketException,
        ///   TaskCanceledException (HttpClient-style timeout).
        /// - Treats HTTP 5xx and 429 as retryable by throwing manually.
        /// - Forces HTTP/1.1 and ResponseHeadersRead to reduce flakiness with proxies/LBs.
        /// </summary>
        private async Task EnsureAccessTokenAsync(HttpClient client, AccessInfo accessInfo, CancellationToken cancellationToken)
        {
            // If we already hold a token that is not near expiry, reuse it.
            if (!string.IsNullOrEmpty(_accessToken) && DateTime.UtcNow < _tokenExpiry.AddMinutes(-3))
                return;

            Log.Information("Fetching new access token...");

            var formData = new Dictionary<string, string>
    {
        { "client_id",     accessInfo.Client_ID },
        { "client_secret", accessInfo.Client_Secret },
        { "scope",         accessInfo.Scope },
        { "grant_type",    accessInfo.Grant_Type }
    };

            // Non-generic retry policy (no .OrResult<...>()), so the lambda doesn't need to return anything.
            var retryPolicy = Policy
                .Handle<HttpRequestException>()  // general http/network failures
                .Or<IOException>()               // I/O pipeline failures
                .Or<SocketException>()           // TCP-level transient errors
                .Or<TaskCanceledException>()     // HttpClient-style timeouts surface as canceled tasks
                .WaitAndRetryAsync(
                    retryCount: 3,
                    sleepDurationProvider: attempt =>
                        TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(_jitter.Next(0, 500)),
                    onRetryAsync: async (ex, delay, attempt, _) =>
                    {
                        Log.Warning(ex, "Retry {Attempt} after {Delay}s for token request", attempt, delay.TotalSeconds);
                        await Task.CompletedTask;
                    });

            await retryPolicy.ExecuteAsync(async () =>
            {
                // Short, per-request timeout for token calls
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromSeconds(900));

                // Explicit request so we can force HTTP/1.1 and stream headers early
                using var req = new HttpRequestMessage(HttpMethod.Post, accessInfo.Url)
                {
                    Content = new FormUrlEncodedContent(formData),
                    Version = HttpVersion.Version11,                       // stabilize path; relax later if H2 is proven stable
                    VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
                };

                using var response = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);

                // Manually treat 5xx/429 as transient by throwing so Polly will retry.
                if ((int)response.StatusCode >= 500 || response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var status = response.StatusCode;
                    string body = string.Empty;
                    try
                    {
                        body = await response.Content.ReadAsStringAsync(cts.Token);
                    }
                    catch
                    {
                        // ignore body read failures on error path
                    }

                    // Dispose response before throwing to free the socket
                    throw new HttpRequestException($"Token request failed with {status}. Body: {body}");
                }

                // Throw for non-success (4xx non-429, etc.). Caller won't retry those by default.
                response.EnsureSuccessStatusCode();

                // Read payload with the same CTS to respect the 30s cap
                var json = await response.Content.ReadAsStringAsync(cts.Token);

                var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(json);
                if (tokenResponse is null || string.IsNullOrWhiteSpace(tokenResponse.access_token))
                {
                    throw new InvalidOperationException("Token response did not contain an access_token.");
                }

                // Store token and compute expiry from 'expires_in' seconds (UTC-based)
                _accessToken = tokenResponse.access_token;
                _tokenExpiry = DateTime.UtcNow.AddSeconds(tokenResponse.expires_in);

                Log.Information("Access token acquired, expires at {Expiry}", _tokenExpiry);
            });
        }


        // ------------------------------------------------------------
        // API execution loop (paged) + SQL insert
        // ------------------------------------------------------------
        /// <summary>
        /// Iterates over a paged API endpoint, retrieves batches, transforms the JSON,
        /// and inserts the results into SQL. Requests are serialized (safe default).
        /// </summary>
        private async Task ProcessApiSync(HttpClient client, TableApiName apiInfo, AccessInfo accessInfo, string connectionString, CancellationToken cancellationToken)
        {
            int skip = 0;
            int batchSize = (int)apiInfo.batchSize;
            bool hasData = true;
            int iteration = 0;
            int maxIterations = _maxIterations; // Safety guard to avoid infinite loops if pagination breaks

            // Retry policy for data calls, same rationale as token policy
            var retryPolicy = Policy
                .Handle<HttpRequestException>()
                .Or<IOException>()
                .Or<SocketException>()
                .Or<TaskCanceledException>()
                .OrResult<HttpResponseMessage>(r => (int)r.StatusCode >= 500 || r.StatusCode == HttpStatusCode.TooManyRequests)
                .WaitAndRetryAsync(
                    retryCount: 3,
                    sleepDurationProvider: attempt =>
                        TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(_jitter.Next(0, 500)),
                    onRetryAsync: async (outcome, delay, attempt, _) =>
                    {
                        Log.Warning(outcome.Exception, "Retry {Attempt} after {Delay}s for API call", attempt, delay.TotalSeconds);
                        await Task.CompletedTask;
                    });

            while (hasData && !cancellationToken.IsCancellationRequested && iteration++ < maxIterations)
            {
                // Proactive token refresh window (3 minutes before expiry)
                if (DateTime.UtcNow >= _tokenExpiry.AddMinutes(-3))
                {
                    Log.Information("Token nearing expiry, refreshing...");
                    await EnsureAccessTokenAsync(client, accessInfo, cancellationToken);
                }

                // Build the request URL with paging and dynamic criteria
                string updatedCriteria = UpdateCriteria(apiInfo.filter, accessInfo);
                var url = $"{apiInfo.APIname}?$top={batchSize}&$skip={skip}";
                if (!string.IsNullOrEmpty(updatedCriteria)) url += updatedCriteria;

                Log.Information("Fetching URL: {Url}", url);

                // Local helper sends ONE request using per-request Authorization header.
                // HttpRequestMessage is single-use, so we recreate it for retries and after 401 refresh.
                async Task<HttpResponseMessage> SendOnceAsync(string bearer, CancellationToken ct)
                {
                    var req = new HttpRequestMessage(HttpMethod.Get, url)
                    {
                        // Force HTTP/1.1 initially. If HTTP/2 proves stable in your infra,
                        // remove these lines to allow negotiation up to H2.
                        Version = HttpVersion.Version11,
                        VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
                    };

                    // Set Authorization PER REQUEST (safer than DefaultRequestHeaders)
                    req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);

                    // ResponseHeadersRead instructs HttpClient to return as soon as headers arrive,
                    // letting us stream the body (reduces buffering delays on large payloads).
                    return await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
                }

                HttpResponseMessage response = null; // Will be disposed in finally

                try
                {
                    // Wrap the send in our retry policy and a per-request CTS (300s SLA here)
                    response = await retryPolicy.ExecuteAsync(async () =>
                    {
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        cts.CancelAfter(TimeSpan.FromSeconds(300)); // Per-request timeout—this is your only timeout

                        // First attempt with current token
                        var resp = await SendOnceAsync(_accessToken, cts.Token);

                        // If token expired between EnsureAccessTokenAsync and now, refresh and retry once
                        if (resp.StatusCode == HttpStatusCode.Unauthorized)
                        {
                            Log.Warning("401 Unauthorized during API call, refreshing token and retrying...");
                            resp.Dispose(); // Important: dispose before reissuing request

                            await EnsureAccessTokenAsync(client, accessInfo, cts.Token);
                            resp = await SendOnceAsync(_accessToken, cts.Token);
                        }

                        // Throw on 4xx/5xx (so Polly can handle 5xx/429 per policy)
                        resp.EnsureSuccessStatusCode();
                        return resp;
                    });

                    // STREAM the content to avoid large buffering + timeouts for big payloads
                    string json;
                    await using (var stream = await response.Content.ReadAsStreamAsync(cancellationToken))
                    using (var reader = new StreamReader(stream))
                    {
                        json = await reader.ReadToEndAsync();
                    }

                    // Business-specific normalization (preserved from your code)
                    json = Regex.Replace(json, @"\](?=.*\}\]\})", " ");
                    json = json.Replace("P[LAIN CITY", "PLAIN CITY");
                    json = json.Split('[', ']')[1];
                    json = json.Replace("_x0020_", "");
                    json = "[" + json + "]";

                    // If the API returned a non-empty batch, insert and advance the page
                    if (response.StatusCode == HttpStatusCode.OK && json != "[]")
                    {
                        await InsertJsonIntoSqlAsync(connectionString, apiInfo.tableName, json, cancellationToken);
                        skip += batchSize;
                    }
                    else
                    {
                        hasData = false; // No more pages/data
                    }
                }
                catch (OperationCanceledException oce)
                {
                    // Distinguish caller/global cancellation from HttpClient-style timeout
                    if (cancellationToken.IsCancellationRequested)
                        Log.Warning(oce, "Request canceled by caller/global token.");
                    else
                        Log.Warning(oce, "Request timed out (no explicit cancellation). Consider increasing per-request timeout or investigating upstream latency.");
                    throw;
                }
                finally
                {
                    // Always dispose the response to promptly return the connection to the pool
                    response?.Dispose();
                }
            }

            if (iteration >= maxIterations)
            {
                Log.Warning("Max iterations reached for API {ApiName}. Possible pagination issue.", apiInfo.APIname);
            }
        }

        // ------------------------------------------------------------
        // SQL Insert
        // ------------------------------------------------------------
        /// <summary>
        /// Inserts normalized JSON into the target table using OPENJSON with a dynamic WITH clause.
        /// Keeps the same cancellation token so DB operations can also be canceled.
        /// </summary>
        private async Task InsertJsonIntoSqlAsync(string connectionString, string tableName, string json, CancellationToken cancellationToken)
        {
            try
            {
                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(cancellationToken);

                // Fetch column mapping rules from dictionary table (COLUMNNAME -> JSON path)
                const string dictionarySql = @"
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

                // Build the OPENJSON WITH clause dynamically based on dictionary
                var withClause = string.Join(",\n", mappings.Select(m =>
                    $"[{m.ColumnName}] VARCHAR(100) '$.{m.JsonPath}'"));

                // Build the INSERT...SELECT via OPENJSON projection
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
                // Include JSON in the error log for forensic purposes (watch size in production)
                Log.Error(ex, "Failed to insert JSON into {TableName} {json}", tableName, json);
                throw;
            }
        }

        // ------------------------------------------------------------
        // TokenResponse DTO
        // ------------------------------------------------------------
        /// <summary>
        /// Minimal DTO for OAuth token responses.
        /// </summary>
        public class TokenResponse
        {
            public string access_token { get; set; }
            public int expires_in { get; set; }
        }
    }
}
//using DSD.Common.Models;

//using Microsoft.Data.SqlClient;
//using Microsoft.Extensions.Configuration;
//using Polly;
//using Serilog;
//using System.Collections.Generic;
//using System.Linq;
//using System.Net;
//using System.Net.Http.Headers;
//using System.Text.Json;
//using System.Text.RegularExpressions;
//using System.Threading;
//using System.Threading.Tasks;

//namespace DSD.Common.Services
//{
//    public class ApiExecutorService
//    {
//        // ------------------------------------------------------------
//        // Private fields for dependencies and token management
//        // ------------------------------------------------------------
//        private readonly IHttpClientFactory _httpClientFactory;
//        private readonly IConfiguration _config;
//        private string _accessToken;
//        private DateTime _tokenExpiry;
//        private readonly Random _jitter = new Random();

//        // ------------------------------------------------------------
//        // Constructor
//        // ------------------------------------------------------------
//        public ApiExecutorService(IHttpClientFactory httpClientFactory, IConfiguration config)
//        {
//            _httpClientFactory = httpClientFactory;
//            _config = config;
//        }

//        // ------------------------------------------------------------
//        // ExecuteApisAndInsertAsync: Main entry point
//        // ------------------------------------------------------------
//        public async Task ExecuteApisAndInsertAsync(List<TableApiName> apiList, AccessInfo accessInfo)
//        {
//            var connectionString = _config.GetConnectionString("CustomerConnectionDB")
//                                          .Replace("CustomerConnection", accessInfo.InitialCatalog);

//            var client = _httpClientFactory.CreateClient("ApiClient");
//            //client.Timeout = TimeSpan.FromSeconds(120);

//            using var globalCts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
//            var globalToken = globalCts.Token;

//            await EnsureAccessTokenAsync(client, accessInfo, globalToken);

//            foreach (var apiInfo in apiList)
//            {
//                await ProcessApiSync(client, apiInfo, accessInfo, connectionString, globalToken);
//            }

//            //var tasks = apiList.Select(apiInfo => ProcessApiAsync(client, apiInfo, accessInfo, connectionString, globalToken));
//            //await Task.WhenAll(tasks);
//        }

//        // ------------------------------------------------------------
//        // UpdateCriteria: Replace placeholders dynamically
//        // ------------------------------------------------------------
//        public string UpdateCriteria(string criteria, AccessInfo accessInfo)
//        {
//            if (criteria != "N")
//            {
//                criteria = criteria.Replace("SHIPDATE", DateTime.Now.AddDays(Convert.ToInt32(accessInfo.DayOffset)).ToString("yyyy-MM-dd"))
//                                   .Replace("ENDDATE", DateTime.Now.AddDays(Convert.ToInt32(accessInfo.DayOffset) + 7).ToString("yyyy-MM-dd"));

//                string dow = DateTime.Today.AddDays(Convert.ToInt32(accessInfo.DayOffset)).DayOfWeek.ToString();
//                criteria = criteria.Replace("xxxdowxxx", dow);

//                DateTime orderdt = GetDateBasedOn1300();
//                criteria = criteria.Replace("xxxorderdatexxx", orderdt.ToString("yyyy-MM-dd"));

//                DateTime weekAgo = GetDateAWeekAgo();
//                criteria = criteria.Replace("xxxpostdatexxx", weekAgo.ToString("yyyy-MM-dd"));

//            }
//            else
//            {
//                criteria = string.Empty;
//            }

//            Log.Information("Criteria updated to: {UpdatedCriteria}", criteria);
//            return criteria;
//        }

//        private DateTime GetDateBasedOn1300()
//        {
//            var now = DateTime.Now;
//            return now.Hour < 13 ? now.Date : now.Date.AddDays(1);
//        }

//        // ------------------------------------------------------------
//        // EnsureAccessTokenAsync: Fetch OAuth token
//        // ------------------------------------------------------------
//        private async Task EnsureAccessTokenAsync(HttpClient client, AccessInfo accessInfo, CancellationToken cancellationToken)
//        {
//            if (string.IsNullOrEmpty(_accessToken) || DateTime.UtcNow >= _tokenExpiry.AddMinutes(-3))
//            {
//                Log.Information("Fetching new access token...");

//                var formData = new Dictionary<string, string>
//                {
//                    { "client_id", accessInfo.Client_ID },
//                    { "client_secret", accessInfo.Client_Secret },
//                    { "scope", accessInfo.Scope },
//                    { "grant_type", accessInfo.Grant_Type }
//                };

//                var retryPolicy = Policy
//                    .Handle<HttpRequestException>()
//                    .OrResult<HttpResponseMessage>(r => (int)r.StatusCode >= 500 || r.StatusCode == HttpStatusCode.TooManyRequests)
//                    .WaitAndRetryAsync(
//                        3,
//                        attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(_jitter.Next(0, 500)),
//                        (outcome, timespan, attempt, context) =>
//                        {
//                            Log.Warning(outcome.Exception, "Retry {Attempt} after {Delay}s for token request", attempt, timespan.TotalSeconds);
//                        });

//                await retryPolicy.ExecuteAsync(async () =>
//                {
//                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
//                    cts.CancelAfter(TimeSpan.FromSeconds(30));

//                    //var response = await client.PostAsync(accessInfo.Url, new FormUrlEncodedContent(formData), cts.Token);
//                    //response.EnsureSuccessStatusCode();

//                    //var json = await response.Content.ReadAsStringAsync(cancellationToken);

//                    using var req = new HttpRequestMessage(HttpMethod.Post, accessInfo.Url)
//                    {
//                        Content = new FormUrlEncodedContent(formData),
//                        Version = HttpVersion.Version11,                    // force H/1.1 initially
//                        VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
//                    };

//                    var response = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
//                    response.EnsureSuccessStatusCode();

//                    var json = await response.Content.ReadAsStringAsync(cts.Token); // use the same CTS

//                    var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(json);

//                    _accessToken = tokenResponse.access_token;
//                    _tokenExpiry = DateTime.UtcNow.AddSeconds(tokenResponse.expires_in);

//                    Log.Information("Access token acquired, expires at {Expiry}", _tokenExpiry);
//                    return response;
//                });
//            }
//        }

//        // ------------------------------------------------------------
//        // ProcessApiAsync: Execute API and insert data
//        // ------------------------------------------------------------

//        private async Task ProcessApiSync(HttpClient client, TableApiName apiInfo, AccessInfo accessInfo, string connectionString, CancellationToken cancellationToken)
//        {
//            int skip = 0;
//            int batchSize = (int)apiInfo.batchSize;
//            bool hasData = true;
//            int iteration = 0;
//            int maxIterations = 100; // Safety limit

//            var retryPolicy = Policy
//                .Handle<HttpRequestException>()
//                .OrResult<HttpResponseMessage>(r => (int)r.StatusCode >= 500 || r.StatusCode == HttpStatusCode.TooManyRequests)
//                .WaitAndRetryAsync(
//                    3,
//                    attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(_jitter.Next(0, 500)),
//                    (outcome, timespan, attempt, context) =>
//                    {
//                        Log.Warning(outcome.Exception, "Retry {Attempt} after {Delay}s for API call", attempt, timespan.TotalSeconds);
//                    });

//            while (hasData && !cancellationToken.IsCancellationRequested && iteration++ < maxIterations)
//            {
//                if (DateTime.UtcNow >= _tokenExpiry.AddMinutes(-3))
//                {
//                    Log.Information("Token nearing expiry, refreshing...");
//                    await EnsureAccessTokenAsync(client, accessInfo, cancellationToken);
//                }

//                string updatedCriteria = UpdateCriteria(apiInfo.filter, accessInfo);
//                var url = $"{apiInfo.APIname}?$top={batchSize}&$skip={skip}";
//                if (!string.IsNullOrEmpty(updatedCriteria)) url += updatedCriteria;

//                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
//                Log.Information("Fetching URL: {Url}", url);

//                // Execute synchronously in sequence
//                var response = await retryPolicy.ExecuteAsync(async () =>
//                {
//                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
//                    cts.CancelAfter(TimeSpan.FromSeconds(300));

//                    var resp = await client.GetAsync(url, cts.Token);

//                    if (resp.StatusCode == HttpStatusCode.Unauthorized)
//                    {
//                        Log.Warning("Token expired during API call, refreshing...");
//                        await EnsureAccessTokenAsync(client, accessInfo, cancellationToken);
//                        resp = await client.GetAsync(url, cts.Token);
//                    }

//                    resp.EnsureSuccessStatusCode();
//                    return resp;
//                });

//                var json = await response.Content.ReadAsStringAsync(cancellationToken);
//                json = Regex.Replace(json, @"\](?=.*\}\]\})", " ");
//                json = json.Replace("P[LAIN CITY", "PLAIN CITY");
//                json = json.Split('[', ']')[1];
//                //json = json.Replace("'", "''").Replace("_x0020_", "");
//                json = json.Replace("_x0020_", "");
//                json = "[" + json + "]";
//                if (response.StatusCode == HttpStatusCode.OK && json != "[]")
//                {
//                    await InsertJsonIntoSqlAsync(connectionString, apiInfo.tableName, json, cancellationToken);
//                    skip += batchSize;
//                }
//                else
//                {
//                    hasData = false;
//                }



//            }

//            if (iteration >= maxIterations)
//            {
//                Log.Warning("Max iterations reached for API {ApiName}. Possible pagination issue.", apiInfo.APIname);
//            }
//        }



//        // ------------------------------------------------------------
//        // InsertJsonIntoSqlAsync: Insert JSON into SQL
//        // ------------------------------------------------------------
//        private async Task InsertJsonIntoSqlAsync(string connectionString, string tableName, string json, CancellationToken cancellationToken)
//        {
//            try
//            {
//                await using var conn = new SqlConnection(connectionString);
//                await conn.OpenAsync(cancellationToken);

//                string dictionarySql = @"
//                    SELECT COLUMNNAME, JSONNAME
//                    FROM DSD_API_DICTIONARY
//                    WHERE TableName = @tableName";

//                var mappings = new List<(string ColumnName, string JsonPath)>();

//                await using (var dictCmd = new SqlCommand(dictionarySql, conn))
//                {
//                    dictCmd.Parameters.AddWithValue("@tableName", tableName);
//                    await using var reader = await dictCmd.ExecuteReaderAsync(cancellationToken);

//                    while (await reader.ReadAsync())
//                    {
//                        mappings.Add((reader["COLUMNNAME"].ToString(), reader["JSONNAME"].ToString()));
//                    }
//                }

//                if (!mappings.Any())
//                {
//                    Log.Error("No column mappings found for table {TableName}", tableName);
//                    throw new Exception($"API Dictionary has no mappings for {tableName}");
//                }

//                var withClause = string.Join(",\n", mappings.Select(m =>
//                    $"[{m.ColumnName}] VARCHAR(100) '$.{m.JsonPath}'"));

//                string sql = $@"
//                    INSERT INTO [{tableName}] ({string.Join(", ", mappings.Select(m => $"[{m.ColumnName}]"))})
//                    SELECT {string.Join(", ", mappings.Select(m => $"[{m.ColumnName}]"))}
//                    FROM OPENJSON(@json)

//                    WITH (
//                        {withClause}
//                    );";

//                await using var cmd = new SqlCommand(sql, conn);
//                cmd.Parameters.AddWithValue("@json", json);

//                int rowsAffected = await cmd.ExecuteNonQueryAsync(cancellationToken);
//                Log.Information("{RowsAffected} rows inserted into {TableName}", rowsAffected, tableName);
//            }
//            catch (Exception ex)
//            {
//                Log.Error(ex, "Failed to insert JSON into {TableName} {json}", tableName, json);
//                throw;
//            }
//        }

//        // ------------------------------------------------------------
//        // TokenResponse: Model for OAuth token response
//        // ------------------------------------------------------------
//        public class TokenResponse
//        {
//            public string access_token { get; set; }
//            public int expires_in { get; set; }
//        }
//        static DateTime GetDateAWeekAgo()
//        {
//            DateTime now = DateTime.Now;
//            return now.Date.AddDays(-8);
//        }
//    }
//}

