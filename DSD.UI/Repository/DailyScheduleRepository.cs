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
    public class DailyScheduleRepository
    {
        private readonly string _cs;

        public DailyScheduleRepository(IConfiguration config)
        {
            _cs = config.GetConnectionString("CustomerConnectionDB")
                  ?? throw new InvalidOperationException(
                      "ConnectionStrings:CustomerConnectionDB not found in appsettings.json");
        }

        public async Task<List<DailyScheduleRow>> GetByCustomerAsync(string cust)
        {
            // ✅ Fix: alias jobid AS JobId so GetOrdinal("JobId") is valid.
            const string sql = @"
SELECT
      jobid AS JobId
    , cust  AS Cust
    , [Job]
    , TargetComputer
    , ScheduleTime
    , ExecuteWeekDays
    , IsActive
    , RUNGROUP
    , SendCIS
FROM dbo.DSD_Job_Executablesdev
WHERE cust = @cust
ORDER BY ScheduleTime;";

            var list = new List<DailyScheduleRow>();

            await using var conn = new SqlConnection(_cs);
            await conn.OpenAsync().ConfigureAwait(false);

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.Add("@cust", SqlDbType.NVarChar, 50).Value = cust;

            try
            {
                await using var rdr = await cmd.ExecuteReaderAsync().ConfigureAwait(false);

                // Grab ordinals once (faster and avoids repeated name lookups).
                int oJobId = rdr.GetOrdinal("JobId");
                int oCust = rdr.GetOrdinal("Cust");
                int oJob = rdr.GetOrdinal("Job");
                int oTarget = rdr.GetOrdinal("TargetComputer");
                int oSchedule = rdr.GetOrdinal("ScheduleTime");
                int oDays = rdr.GetOrdinal("ExecuteWeekDays");
                int oActive = rdr.GetOrdinal("IsActive");
                int oRunGroup = rdr.GetOrdinal("RUNGROUP");
                int oSendCis = rdr.GetOrdinal("SendCIS");

                while (await rdr.ReadAsync().ConfigureAwait(false))
                {
                    // ScheduleTime: supports time, datetime, string, null
                    TimeSpan scheduleTime = TimeSpan.Zero;

                    if (!rdr.IsDBNull(oSchedule))
                    {
                        object v = rdr.GetValue(oSchedule);

                        if (v is TimeSpan ts) scheduleTime = ts;
                        else if (v is DateTime dt) scheduleTime = dt.TimeOfDay;
                        else if (v is DateTimeOffset dto) scheduleTime = dto.TimeOfDay;
                        else if (TimeSpan.TryParse(v.ToString(), out var parsed)) scheduleTime = parsed;
                        else
                            throw new InvalidOperationException(
                                $"Unexpected ScheduleTime type: {v.GetType().FullName}, value: {v}");
                    }

                    list.Add(new DailyScheduleRow
                    {
                        // ✅ JobId is now properly aliased from SQL
                        jobId = rdr.IsDBNull(oJobId) ? 0 : rdr.GetInt32(oJobId),

                        Cust = rdr.IsDBNull(oCust) ? "" : rdr.GetString(oCust),
                        Job = rdr.IsDBNull(oJob) ? "" : rdr.GetString(oJob),
                        TargetComputer = rdr.IsDBNull(oTarget) ? "" : rdr.GetString(oTarget),

                        ScheduleTime = scheduleTime,

                        ExecuteWeekDays = rdr.IsDBNull(oDays) ? "" : rdr.GetString(oDays),

                        // NOTE: Your model uses string for IsActive; we preserve that.
                        // If you later change model to bool, change this to rdr.GetBoolean(oActive).
                        IsActive = rdr.IsDBNull(oActive) ? "False" : rdr.GetBoolean(oActive).ToString(),

                        RUNGROUP = rdr.IsDBNull(oRunGroup) ? "" : rdr.GetString(oRunGroup),

                        // Depending on your schema, SendCIS might be "Y/N", "True/False", etc.
                        SendCIS = rdr.IsDBNull(oSendCis) ? "" : rdr.GetString(oSendCis)
                    });
                }
            }
            catch (SqlException ex)
            {
                MessageBox.Show(
                    $"SQL error {ex.Number}\n" +
                    $"Message: {ex.Message}\n" +
                    $"Server: {conn.DataSource}\n" +
                    $"Database: {conn.Database}\n" +
                    $"State: {conn.State}\n" +
                    $"CommandTimeout: {cmd.CommandTimeout}\n" +
                    $"SQL:\n{cmd.CommandText}");
                throw;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
                throw;
            }

            return list;
        }
    }
}