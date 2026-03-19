using DSD.Common.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

public sealed class DsdLogRepository
{
    private readonly string _connectionString;

    public DsdLogRepository(IConfiguration config)
    {
        _connectionString = config.GetConnectionString("CustomerConnectionDB");
    }

    public async Task<IReadOnlyList<DsdLog>> GetByDateAsync(DateOnly date)
    {
        const string sql = @"
SELECT
    Cust,
    Job,
    TargetComputer,
    ScheduleDate,
    ScheduleTime,
    StartDate,
    EndDate,
    Status
FROM dbo.DSD_Job_Log
WHERE ScheduleDate = @TodaysDate
ORDER BY ScheduleTime;";

        var results = new List<DsdLog>();

        using var connection = new SqlConnection(_connectionString);
        using var command = new SqlCommand(sql, connection);

        command.Parameters.Add("@TodaysDate", SqlDbType.Date)
                          .Value = date.ToDateTime(TimeOnly.MinValue);

        await connection.OpenAsync();

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            results.Add(new DsdLog
            {
                Cust = reader["Cust"] as string,
                Job = reader["Job"] as string,
                TargetComputer = reader["TargetComputer"] as string,

                ScheduleDate = reader["ScheduleDate"] is DateTime sd
                    ? DateOnly.FromDateTime(sd)
                    : null,

                ScheduleTime = reader["ScheduleTime"] is TimeSpan st
                    ? TimeOnly.FromTimeSpan(st)
                    : null,

                StartDate = reader["StartDate"] as DateTime?,
                EndDate = reader["EndDate"] as DateTime?,
                Status = reader["Status"] as string
            });
        }

        return results;
    }
}