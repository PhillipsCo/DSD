using DSD.Common.Models;
using DSD.Common.Services;
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
/// APPLICATION SHELL / COORDINATOR
///
/// ✅ Owns GLOBAL UI STATE shared across tabs
/// ✅ Creates and wires CHILD TAB VIEWMODELS
/// ✅ Propagates customer/direction/table context
///
/// ❌ Does NOT do CRUD
/// ❌ Does NOT call APIs directly
/// </summary>
public class MainViewModel : INotifyPropertyChanged
{
    // =========================================================
    // 1) SHARED SERVICES
    // =========================================================

    private readonly IConfiguration _config;
    private readonly ISqlService _sqlService;

    // =========================================================
    // 2) CHILD TAB VIEWMODELS
    // =========================================================

    public DailyScheduleGridViewModel DailySchedule { get; }

    /// <summary>
    /// Admin → Customer Info tab
    /// </summary>
    public CustomerInfoViewModel CustomerInfo { get; }

    /// <summary>
    /// ✅ ALIAS REQUIRED BY ApiTester wiring
    /// Exposes the SAME instance under the expected name.
    /// </summary>
    public CustomerInfoViewModel CustomerInfoViewModel => CustomerInfo;

    public ApiListGridViewModel ApiList { get; }

    // =========================================================
    // 3) GLOBAL COMMANDS
    // =========================================================

    public ICommand RunCommand { get; }

    // =========================================================
    // 4) CONSTRUCTOR (COMPOSITION ROOT)
    // =========================================================

    public MainViewModel(ISqlService sqlService, IConfiguration config)
    {
        _sqlService = sqlService;
        _config = config;

        // -----------------------------------------------------
        // Create child ViewModels ONCE
        // -----------------------------------------------------

        DailySchedule = new DailyScheduleGridViewModel(_config);
        CustomerInfo = new CustomerInfoViewModel(_config);
        ApiList = new ApiListGridViewModel(_config);

        // React to CustomerInfo loading
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

    private CustomerRow? _selectedCustomer;

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

            PropagateCustomerContext(value);
            _ = LoadTableOptionsAsync();
        }
    }

    public int? SelectedCustomerId => SelectedCustomer?.Id;

    public async Task LoadCustomersAsync()
    {
        var rows = await _sqlService.GetCustomersAsync();

        Customers.Clear();
        foreach (var row in rows)
            Customers.Add(row);

        if (Customers.Count > 0)
            SelectedCustomer = Customers[0];
    }

    private void PropagateCustomerContext(CustomerRow? customer)
    {
        DailySchedule.SetCustomer(customer);
        CustomerInfo.SetCustomer(customer);
        ApiList.SetCustomer(customer);
    }

    // =========================================================
    // 6) CUSTOMER INFO → API LIST LINK
    // =========================================================

    private void CustomerInfo_PropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName != nameof(CustomerInfoViewModel.Current))
            return;

        var catalog = CustomerInfo.Current?.InitialCatalog;

        Debug.WriteLine(
            $"[MainVM] CustomerInfo.Current changed. InitialCatalog = '{catalog}'");

        ApiList.SetInitialCatalog(catalog);
    }

    // =========================================================
    // 7) DIRECTION
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

            _ = LoadTableOptionsAsync();
        }
    }

    public bool IsOutbound => SelectedDirection == "Outbound";

    // =========================================================
    // 8) TABLE OPTIONS
    // =========================================================

    public ObservableCollection<string> TableOptions { get; } = new();

    private string? _selectedTableOption;

    public string? SelectedTableOption
    {
        get => _selectedTableOption;
        set
        {
            if (string.Equals(_selectedTableOption, value, StringComparison.Ordinal))
                return;

            _selectedTableOption = value;
            OnPropertyChanged();

            ApiList.SetTableOption(value);
        }
    }

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

        SelectedTableOption = "ALL";
    }

    // =========================================================
    // 9) SEND TO CIS
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
    // 10) RUN COMMAND
    // =========================================================

    private bool CanRun() =>
        SelectedCustomer != null
        && !string.IsNullOrWhiteSpace(SelectedDirection)
        && !string.IsNullOrWhiteSpace(SelectedTableOption);

    private void Run()
    {
        var message =
$@"You are about to run:

Direction: {SelectedDirection}
Customer: {SelectedCustomer?.Customer}
Group / Table: {SelectedTableOption}
Send to CIS: {(SendToCis ? "Yes" : "No")}";

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

        var args =
            $"{SelectedCustomer?.Customer} {SelectedTableOption} {(SendToCis ? "Y" : "N")}";

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
    // 11) INotifyPropertyChanged
    // =========================================================

    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}
