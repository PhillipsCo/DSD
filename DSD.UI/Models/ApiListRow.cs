namespace DSD.UI.Models
{
    public class ApiListRow
    {
        public string TABLE_NAME { get; set; } = "";
        public string API_NAME { get; set; } = "";
        public string FILTER { get; set; } = "";
        public int BATCHSIZE { get; set; } = 0;
        public string DIR { get; set; } = "";
        public string RUNGROUP { get; set; } = "";
        public string ENDPOINT { get; set; } = "";
    }
}