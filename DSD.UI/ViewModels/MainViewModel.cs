using DSD.Common.Models;
using DSD.Common.Services;
using DSD.UI.ViewModels;        // Child tab ViewModels
using Microsoft.Extensions.Configuration;

using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DSD.UI.ViewModels;

/// <summary>
/// MainViewModel
/// =============
///
/// This ViewModel is the APPLICATION SHELL / COORDINATOR.
///
/// DESIGN PHILOSOPHY:
/// ------------------
/// ✅ Owns GLOBAL UI STATE shared across tabs
/// ✅ Creates and wires CHILD TAB VIEWMODELS
/// ✅ Propagates customer/direction/table context to child tabs
/// ✅ Owns the top-level "Run" command
///
/// ❌ Does NOT contain tab-specific SQL
/// ❌ Does NOT perform CRUD directly
/// ❌ Does NOT own DataGrids
///
/// Each tab ViewModel:
/// - Owns its own data
/// - Owns its own repositories
/// - Owns its own CRUD commands
/// </summary>
public class MainViewModel : INotifyPropertyChanged
{
    // =========================================================
    // 1) SHARED SERVICES / DEPENDENCIES
    // =========================================================

    private readonly IConfiguration _config;
    private readonly ISqlService _sqlService;

    // =========================================================
    // 2) CHILD TAB VIEWMODELS (ONE PER TAB)
    // =========================================================
    //
    // These are created ONCE and live for the lifetime of the app.
    // Context (customer, table, direction) is pushed into them.
    //

    /// <summary>
    /// Admin → Daily Schedule tab
    /// </summary>
    public DailyScheduleGridViewModel DailySchedule { get; }

    /// <summary>
    /// Admin → Customer Info tab
    /// </summary>
    public CustomerInfoViewModel CustomerInfo { get; }

    /// <summary>
    /// Admin → API List tab
    /// Backed by DSD_API_LIST
    /// </summary>
    public ApiListGridViewModel ApiList { get; }

    // =========================================================
    // 3) GLOBAL COMMANDS (OWNED BY SHELL)
    // =========================================================

    /// <summary>
    /// Launches inbound/outbound executable
    /// </summary>
    public ICommand RunCommand { get; }

    // =========================================================
    // 4) CONSTRUCTOR (COMPOSITION ROOT)
    // =========================================================

    public MainViewModel(ISqlService sqlService, IConfiguration config)
    {
        _sqlService = sqlService;
        _config = config;

        // -----------------------------------------------------
        // Create child ViewModels
        // -----------------------------------------------------

        DailySchedule = new DailyScheduleGridViewModel(_config);
        CustomerInfo = new CustomerInfoViewModel(_config);
        ApiList = new ApiListGridViewModel(_config);
        CustomerInfo.PropertyChanged += CustomerInfo_PropertyChanged;
        // -----------------------------------------------------
        // Default global UI state
        // -----------------------------------------------------

        _selectedDirection = "Inbound";
        _sendToCis = true;

        TableOptions.Add("ALL");
        _selectedTableOption = "ALL";

        // -----------------------------------------------------
        // Commands
        // -----------------------------------------------------

        RunCommand = new DSD.UI.RelayCommand(Run, CanRun);
    }

    // =========================================================
    // 5) CUSTOMER CONTEXT (DRIVES ALL TABS)
    // =========================================================

    public ObservableCollection<CustomerRow> Customers { get; } = new();
    private void CustomerInfo_PropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        // We only care when the CustomerInfo row is loaded or replaced
        if (e.PropertyName != nameof(CustomerInfoViewModel.Current))
            return;

        var catalog = CustomerInfo.Current?.InitialCatalog;

        System.Diagnostics.Debug.WriteLine(
            $"[MainVM] CustomerInfo.Current changed. InitialCatalog = '{catalog}'");

        // Push the correct DB catalog into the API List tab
        ApiList.SetInitialCatalog(catalog);
    }
    private CustomerRow? _selectedCustomer;

    /// <summary>
    /// Currently selected customer.
    /// Changing this reloads ALL tabs.
    /// </summary>
    public CustomerRow? SelectedCustomer
    {
        get => _selectedCustomer;
        set
        {
            if (_selectedCustomer?.Id == value?.Id)
                return;

            _selectedCustomer = value;
            OnPropertyChanged();
            OnPropertyChanged(nameof(SelectedCustomerId));

            // Push customer context to all child tabs
            PropagateCustomerContext(value);

            // Direction + customer determines available table options
            _ = LoadTableOptionsAsync();
        }
    }

    public int? SelectedCustomerId => SelectedCustomer?.Id;

    /// <summary>
    /// Initial load of customers at app startup
    /// </summary>
    public async Task LoadCustomersAsync()
    {
        var rows = await _sqlService.GetCustomersAsync();

        Customers.Clear();
        foreach (var row in rows)
            Customers.Add(row);

        // Auto-select first customer to initialize UI
        if (Customers.Count > 0)
            SelectedCustomer = Customers[0];
    }

    /// <summary>
    /// Pushes customer context into each child tab.
    /// Centralized to keep setters clean.
    /// </summary>
    private void PropagateCustomerContext(CustomerRow? customer)
    {
        DailySchedule.SetCustomer(customer);
        CustomerInfo.SetCustomer(customer);
        ApiList.SetCustomer(customer);
    }

    // =========================================================
    // 6) DIRECTION (INBOUND / OUTBOUND)
    // =========================================================

    public ObservableCollection<string> Directions { get; }
        = new() { "Inbound", "Outbound" };

    private string _selectedDirection;

    public string SelectedDirection
    {
        get => _selectedDirection;
        set
        {
            if (string.Equals(_selectedDirection, value, StringComparison.Ordinal))
                return;

            _selectedDirection = value;
            OnPropertyChanged();
            OnPropertyChanged(nameof(IsOutbound));

            // Direction impacts outbound table options
            _ = LoadTableOptionsAsync();
        }
    }

    public bool IsOutbound => SelectedDirection == "Outbound";

    // =========================================================
    // 7) TABLE / GROUP OPTIONS (OUTBOUND)
    // =========================================================

    public ObservableCollection<string> TableOptions { get; } = new();

    private string? _selectedTableOption;

    /// <summary>
    /// Selected table or group (ALL or specific value).
    /// This drives API list filtering and run behavior.
    /// </summary>
    public string? SelectedTableOption
    {
        get => _selectedTableOption;
        set
        {
            if (string.Equals(_selectedTableOption, value, StringComparison.Ordinal))
                return;

            _selectedTableOption = value;
            OnPropertyChanged();

            // Keep API List tab in sync
            ApiList.SetTableOption(value);
        }
    }

    /// <summary>
    /// Loads available outbound tables/groups.
    /// Always includes ALL.
    /// </summary>
    private async Task LoadTableOptionsAsync()
    {
        TableOptions.Clear();
        TableOptions.Add("ALL");

        if (IsOutbound
            && SelectedCustomer is not null
            && !string.IsNullOrWhiteSpace(SelectedCustomer.InitialCatalog))
        {
            var tables = await _sqlService.GetOutboundTableNamesAsync(
                SelectedCustomer.InitialCatalog);

            foreach (var t in tables)
            {
                if (string.IsNullOrWhiteSpace(t)) continue;
                if (string.Equals(t, "ALL", StringComparison.OrdinalIgnoreCase)) continue;
                if (!TableOptions.Contains(t)) TableOptions.Add(t);
            }
        }

        // Default selection after reload
        SelectedTableOption = "ALL";
    }

    // =========================================================
    // 8) SEND TO CIS FLAG
    // =========================================================

    private bool _sendToCis;

    public bool SendToCis
    {
        get => _sendToCis;
        set
        {
            if (_sendToCis == value) return;
            _sendToCis = value;
            OnPropertyChanged();
        }
    }

    // =========================================================
    // 9) RUN COMMAND
    // =========================================================

    private bool CanRun()
    {
        return SelectedCustomer != null
            && !string.IsNullOrWhiteSpace(SelectedDirection)
            && !string.IsNullOrWhiteSpace(SelectedTableOption);
    }

    private void Run()
    {
        var message =
$@"You are about to run:

Direction: {SelectedDirection}
Customer: {SelectedCustomer?.Customer}
Group / Table: {SelectedTableOption}
Send to CIS: {(SendToCis ? "Yes" : "No")}

Do you want to continue?";

        if (MessageBox.Show(
                message,
                "Confirm Run",
                MessageBoxButton.OKCancel,
                MessageBoxImage.Warning)
            != MessageBoxResult.OK)
            return;

        ExecuteRun();
    }

    private void ExecuteRun()
    {
        var exePath = SelectedDirection == "Inbound"
            ? _config["InboundPath"]
            : _config["OutboundPath"];

        if (string.IsNullOrWhiteSpace(exePath) || !File.Exists(exePath))
        {
            MessageBox.Show(
                $"Executable not found:\n{exePath}",
                "Execution Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
            return;
        }

        var args = $"{SelectedCustomer?.Customer} {SelectedTableOption} {(SendToCis ? "Y" : "N")}";

        Process.Start(new ProcessStartInfo
        {
            FileName = exePath,
            Arguments = args,
            UseShellExecute = true,
            WorkingDirectory = Path.GetDirectoryName(exePath)!,
            CreateNoWindow = false
        });
    }

    // =========================================================
    // 10) INotifyPropertyChanged
    // =========================================================

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}