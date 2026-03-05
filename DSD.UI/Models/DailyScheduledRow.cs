namespace DSD.UI.Models
{
    public class DailyScheduleRow
    {
        public int jobId { get; set; } = 0;
        public string Cust { get; set; } = "";
        public string Job { get; set; } = "";
        public string TargetComputer { get; set; } = "";
        public TimeSpan ScheduleTime { get; set; }
        public string ExecuteWeekDays { get; set; } = "";
        public string IsActive { get; set; } = "";
        public string RUNGROUP { get; set; } = "";
        public string SendCIS { get; set; } = "";
    }
}