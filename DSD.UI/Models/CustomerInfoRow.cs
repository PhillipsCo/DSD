using System.ComponentModel;

public class CustomerInfoRow : INotifyPropertyChanged
{
    private string? _customer;
    public string? Customer { get => _customer; set { _customer = value; OnPropertyChanged(); } }

    private string? _url;
    public string? Url { get => _url; set { _url = value; OnPropertyChanged(); } }

    public string? Grant_Type { get => _grant_Type; set { _grant_Type = value; OnPropertyChanged(); } }
    private string? _grant_Type;

    public string? Client_ID { get => _client_ID; set { _client_ID = value; OnPropertyChanged(); } }
    private string? _client_ID;

    public string? Client_Secret { get => _client_Secret; set { _client_Secret = value; OnPropertyChanged(); } }
    private string? _client_Secret;

    public string? Scope { get => _scope; set { _scope = value; OnPropertyChanged(); } }
    private string? _scope;

    public string? RootUrl { get => _rootUrl; set { _rootUrl = value; OnPropertyChanged(); } }
    private string? _rootUrl;

    public string? ftpHost { get => _ftpHost; set { _ftpHost = value; OnPropertyChanged(); } }
    private string? _ftpHost;

    public string? ftpUser { get => _ftpUser; set { _ftpUser = value; OnPropertyChanged(); } }
    private string? _ftpUser;

    public string? ftpPass { get => _ftpPass; set { _ftpPass = value; OnPropertyChanged(); } }
    private string? _ftpPass;

    public string? ftpRemoteFilePath { get => _ftpRemoteFilePath; set { _ftpRemoteFilePath = value; OnPropertyChanged(); } }
    private string? _ftpRemoteFilePath;

    public string? ftpLocalFilePath { get => _ftpLocalFilePath; set { _ftpLocalFilePath = value; OnPropertyChanged(); } }
    private string? _ftpLocalFilePath;

    public string? DataSource { get => _dataSource; set { _dataSource = value; OnPropertyChanged(); } }
    private string? _dataSource;

    public string? UserID { get => _userID; set { _userID = value; OnPropertyChanged(); } }
    private string? _userID;

    public string? Password { get => _password; set { _password = value; OnPropertyChanged(); } }
    private string? _password;

    public string? InitialCatalog { get => _initialCatalog; set { _initialCatalog = value; OnPropertyChanged(); } }
    private string? _initialCatalog;

    public int? DayOffset { get => _dayOffset; set { _dayOffset = value; OnPropertyChanged(); } }
    private int? _dayOffset;

    public string? email_tenantId { get => _email_tenantId; set { _email_tenantId = value; OnPropertyChanged(); } }
    private string? _email_tenantId;

    public string? email_clientId { get => _email_clientId; set { _email_clientId = value; OnPropertyChanged(); } }
    private string? _email_clientId;

    public string? email_secret { get => _email_secret; set { _email_secret = value; OnPropertyChanged(); } }
    private string? _email_secret;

    public string? email_sender { get => _email_sender; set { _email_sender = value; OnPropertyChanged(); } }
    private string? _email_sender;

    public string? email_recipient { get => _email_recipient; set { _email_recipient = value; OnPropertyChanged(); } }
    private string? _email_recipient;

    public string? PROD { get => _prod; set { _prod = value; OnPropertyChanged(); } }
    private string? _prod;

    public int? HISTORY_DAYS { get => _historyDays; set { _historyDays = value; OnPropertyChanged(); } }
    private int? _historyDays;

    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged([System.Runtime.CompilerServices.CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}