using DSD.Common.Models;
using DSD.Common.Services;
using DSD.UI.ViewModels;   // DailyScheduleGridViewModel, CustomerInfoViewModel

using Microsoft.Extensions.Configuration;

using System;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DSD.UI.ViewModels;

/// <summary>
/// MainViewModel
/// ============
///
/// This ViewModel is the "application shell / coordinator".
///
/// RESPONSIBILITIES (by design):
///  ✅ Global application state shared across tabs:
///      - SelectedCustomer
///      - SelectedDirection (Inbound/Outbound)
///      - SelectedTableOption (ALL or a table)
///      - SendToCis flag
///  ✅ Top-level Run command (launch external executable)
///  ✅ Creates and coordinates child ViewModels (one per tab)
///
/// NON-RESPONSIBILITIES:
///  ❌ It does NOT own tab-specific SQL or CRUD logic.
///     Each tab ViewModel owns its own data and persistence.
/// </summary>
public class MainViewModel : INotifyPropertyChanged
{
    // =========================================================
    // 1) Services / Dependencies (shared across the application)
    // =========================================================

    private readonly IConfiguration _config;
    private readonly ISqlService _sqlService;

    // =========================================================
    // 2) Child ViewModels (one per tab)
    // =========================================================
    //
    // MainViewModel creates these VMs and supplies shared context (SelectedCustomer).
    // Each child VM owns its own data, commands, and repository/SQL details.
    //

    /// <summary>
    /// ViewModel backing the "Admin - Daily Schedule" tab.
    /// Owns its own Items, SelectedItem, and CRUD commands.
    /// </summary>
    public DailyScheduleGridViewModel DailySchedule { get; }

    /// <summary>
    /// ViewModel backing the "Admin - Customer Info" tab.
    /// Owns its own Current record and Update command.
    /// </summary>
    public CustomerInfoViewModel CustomerInfo { get; }  // ✅ ADDED

    // =========================================================
    // 3) Commands (global commands owned by the shell)
    // =========================================================

    /// <summary>
    /// Launches the inbound/outbound executable for the selected customer and options.
    /// </summary>
    public ICommand RunCommand { get; }

    // =========================================================
    // 4) Constructor (composition root for ViewModels)
    // =========================================================

    public MainViewModel(ISqlService sqlService, IConfiguration config)
    {
        // Store dependencies
        _sqlService = sqlService;
        _config = config;

        // -----------------------------------------------------
        // Create child tab ViewModels
        // -----------------------------------------------------
        // IMPORTANT:
        // - These child VMs are created once at startup
        // - They are "fed" customer context via SetCustomer(...)
        //
        DailySchedule = new DailyScheduleGridViewModel(_config);
        CustomerInfo = new CustomerInfoViewModel(_config);  // ✅ ADDED

        // -----------------------------------------------------
        // Default UI state (safe initial values)
        // -----------------------------------------------------
        _selectedDirection = "Inbound";
        _sendToCis = true;

        TableOptions.Add("ALL");
        _selectedTableOption = "ALL";

        // -----------------------------------------------------
        // Command wiring
        // -----------------------------------------------------
        RunCommand = new DSD.UI.RelayCommand(Run, CanRun);
    }

    // =========================================================
    // 5) Global Customer Context (drives all tabs)
    // =========================================================

    /// <summary>
    /// Customers list for the top ComboBox.
    /// </summary>
    public ObservableCollection<CustomerRow> Customers { get; } = new();

    private CustomerRow? _selectedCustomer;

    /// <summary>
    /// Selected customer from the ComboBox.
    /// When this changes, we propagate it to each child tab VM.
    /// </summary>
    public CustomerRow? SelectedCustomer
    {
        get => _selectedCustomer;
        set
        {
            // If the customer didn't actually change, avoid duplicate reload work.
            if (_selectedCustomer?.Id == value?.Id)
                return;

            _selectedCustomer = value;
            OnPropertyChanged();
            OnPropertyChanged(nameof(SelectedCustomerId));

            // Propagate customer context to child tabs.
            // Use 'value' (the new selection) to avoid any timing issues.
            PropagateCustomerContext(value);

            // Direction affects outbound table options, so reload them when customer changes too.
            // Fire-and-forget is okay because UI remains responsive.
            _ = LoadTableOptionsAsync();
        }
    }

    /// <summary>
    /// Convenience property sometimes used in UI.
    /// </summary>
    public int? SelectedCustomerId => SelectedCustomer?.Id;

    /// <summary>
    /// Loads customers at application startup.
    /// This is typically called after MainViewModel is constructed.
    /// </summary>
    public async Task LoadCustomersAsync()
    {
        var rows = await _sqlService.GetCustomersAsync();

        Customers.Clear();
        foreach (var row in rows)
            Customers.Add(row);

        // Select first customer by default to initialize the UI and child tabs.
        if (Customers.Count > 0)
            SelectedCustomer = Customers[0];
    }

    /// <summary>
    /// Centralized place to push customer changes into all child tab ViewModels.
    /// This keeps the SelectedCustomer setter clean and makes future tabs easy to add.
    /// </summary>
    private void PropagateCustomerContext(CustomerRow? customer)
    {
        // Daily schedule tab: reload grid rows for this customer
        DailySchedule.SetCustomer(customer);

        // Customer info tab: load the editable customer record for this customer
        CustomerInfo.SetCustomer(customer);

        // Future tabs go here:
        // ApiList.SetCustomer(customer);
        // AnotherTab.SetCustomer(customer);
    }

    // =========================================================
    // 6) Direction (Inbound / Outbound)
    // =========================================================

    public ObservableCollection<string> Directions { get; } =
        new() { "Inbound", "Outbound" };

    private string _selectedDirection;

    /// <summary>
    /// Inbound/Outbound selector.
    /// Changing this impacts available table options.
    /// </summary>
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

            // Direction affects outbound table options.
            _ = LoadTableOptionsAsync();
        }
    }

    public bool IsOutbound => SelectedDirection == "Outbound";

    // =========================================================
    // 7) Outbound Table / Group Options
    // =========================================================

    public ObservableCollection<string> TableOptions { get; } = new();

    private string? _selectedTableOption;

    /// <summary>
    /// Selected table or group (ALL or a specific table).
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
        }
    }

    /// <summary>
    /// Populates TableOptions when direction is Outbound and a customer is selected.
    /// Always includes "ALL".
    /// </summary>
    private async Task LoadTableOptionsAsync()
    {
        TableOptions.Clear();
        TableOptions.Add("ALL");

        // Only fetch tables when outbound + selected customer has a catalog.
        if (IsOutbound
            && SelectedCustomer is not null
            && !string.IsNullOrWhiteSpace(SelectedCustomer.InitialCatalog))
        {
            var tables = await _sqlService.GetOutboundTableNamesAsync(SelectedCustomer.InitialCatalog);

            foreach (var t in tables)
            {
                if (string.IsNullOrWhiteSpace(t)) continue;
                if (string.Equals(t, "ALL", StringComparison.OrdinalIgnoreCase)) continue;
                if (!TableOptions.Contains(t)) TableOptions.Add(t);
            }
        }

        // Default selection after reload.
        SelectedTableOption = "ALL";

        // Not strictly required because ObservableCollection notifies on changes,
        // but harmless if you want to force refresh in some bindings.
        OnPropertyChanged(nameof(TableOptions));
        OnPropertyChanged(nameof(SelectedTableOption));
    }

    // =========================================================
    // 8) Send To CIS flag (global option)
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
    // 9) Run Command Logic (Process Launch)
    // =========================================================

    /// <summary>
    /// Determines whether the Run button should be enabled.
    /// </summary>
    private bool CanRun()
    {
        return SelectedCustomer != null
            && !string.IsNullOrWhiteSpace(SelectedDirection)
            && !string.IsNullOrWhiteSpace(SelectedTableOption);
    }

    /// <summary>
    /// Confirm then run the executable.
    /// </summary>
    private void Run()
    {
        var message =
$@"You are about to run:

Direction: {SelectedDirection}
Customer: {SelectedCustomer?.Customer}
Group / Table: {SelectedTableOption}
Send to CIS: {(SendToCis ? "Yes" : "No")}

Do you want to continue?";

        var result = MessageBox.Show(
            message,
            "Confirm Run",
            MessageBoxButton.OKCancel,
            MessageBoxImage.Warning);

        if (result != MessageBoxResult.OK)
            return;

        ExecuteRun();
    }

    /// <summary>
    /// Performs the actual process launch.
    /// </summary>
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

        var customer = SelectedCustomer?.Customer ?? "";
        var group = SelectedTableOption ?? "ALL";
        var sendToCis = SendToCis ? "Y" : "N";

        var arguments = $"{customer} {group} {sendToCis}";

        Process.Start(new ProcessStartInfo
        {
            FileName = exePath,
            Arguments = arguments,
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