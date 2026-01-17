using DSD.Common.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Graph.Models;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace DSD.Scheduler.Runners
{
    public class SchedulerAppRunner
    {
        private readonly SqlService _sqlService;              // Handles SQL database operations
        private readonly IConfiguration _configuration;

        public SchedulerAppRunner(SqlService sqlService
                                 , IConfiguration configuration)
        {
            _sqlService = sqlService;
            _configuration = configuration;
        }



        public async Task RunAsync()
        {
            Log.Information("Starting Scheduler");
            DateTime today = DateTime.Today;
            for (int i = 1; i <= 4; i++)
            {

                DateTime futureDate = today.AddDays(i);
                int dayOfWeekInt = ((int)futureDate.DayOfWeek + 1); // Sunday = 1
                string dateValue = futureDate.ToString("yyyy-MM-dd");
                await _sqlService.ScheduleJobsAsync(dayOfWeekInt, dateValue);
            }
        }
    }
}
   
