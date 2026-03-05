using DSD.Common.Models;
using DSD.Common.Services;
using DSD.UI.Repositories;
using DSD.UI.Models;
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

public class MainViewModel : INotifyPropertyChanged
{
    // =========================================================
    // Fields / Services
    // =========================================================
    private readonly IConfiguration _config;
    private readonly ISqlService _sqlService;
    private readonly DailyScheduleRepository _dailyScheduleRepo;

    // =========================================================
    // Constructor / Commands
    // =========================================================
    public ICommand RunCommand { get; }

    public MainViewModel(ISqlService sqlService, IConfiguration config)
    {
        _sqlService = sqlService;
        _config = config;

        // If your repository needs ISqlService, wire it once here
        _dailyScheduleRepo = new DailyScheduleRepository(_config);

        // Defaults so UI state is stable
        _selectedDirection = "Inbound";
        TableOptions.Add("ALL");
        SelectedTableOption = "ALL";
        _sendToCis = true;

        RunCommand = new DSD.UI.RelayCommand(Run, CanRun);
    }

    // =========================================================
    // Run Command Logic
    // =========================================================
    private bool CanRun()
    {
        return SelectedCustomer != null &&
               !string.IsNullOrWhiteSpace(SelectedDirection) &&
               !string.IsNullOrWhiteSpace(SelectedTableOption);
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

        var result = MessageBox.Show(
            message,
            "Confirm Run",
            MessageBoxButton.OKCancel,
            MessageBoxImage.Warning);

        if (result != MessageBoxResult.OK)
            return;

        ExecuteRun();
    }

    private void ExecuteRun()
    {
        // NOTE: In your JSON you showed full paths (C:\CIS\APPS\...)
        // In that case you should NOT Path.Combine with BaseDirectory.
        // Use the string as-is.
        var exePath = SelectedDirection == "Inbound"
            ? _config["InboundPath"]
            : _config["OutboundPath"];

        if (string.IsNullOrWhiteSpace(exePath))
        {
            MessageBox.Show(
                "InboundPath/OutboundPath is missing from appsettings.json",
                "Configuration Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
            return;
        }

        if (!File.Exists(exePath))
        {
            MessageBox.Show(
                $"Could not find executable at:\n{exePath}",
                "Executable Not Found",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
            return;
        }

        var customer = SelectedCustomer?.Customer ?? "";
        var group = SelectedTableOption ?? "ALL";
        var sendToCis = SendToCis ? "Y" : "N";
        var arguments = $"{customer} {group} {sendToCis}";

        try
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = exePath,
                Arguments = arguments,
                UseShellExecute = true,
                WorkingDirectory = Path.GetDirectoryName(exePath)!,
                CreateNoWindow = false
            };

            Process.Start(startInfo);
        }
        catch (Exception ex)
        {
            MessageBox.Show(
                $"Failed to start process:\n{ex.Message}",
                "Execution Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
        }
    }

    // =========================================================
    // Customers
    // =========================================================
    public ObservableCollection<CustomerRow> Customers { get; } = new();

    private CustomerRow? _selectedCustomer;
    public CustomerRow? SelectedCustomer
    {
        get => _selectedCustomer;
        set
        {
            //if (ReferenceEquals(_selectedCustomer, value)) return;

            if(_selectedCustomer?.Id == value?.Id)
            return;

            _selectedCustomer = value;

            OnPropertyChanged();
            OnPropertyChanged(nameof(SelectedCustomerId));

            // Refresh dependent UI/data when customer changes
            _ = LoadTableOptionsAsync();
            _ = LoadDailyScheduleAsync();   // ✅ Tab 1: Daily Schedule
        }
    }

    public int? SelectedCustomerId => SelectedCustomer?.Id;

    // =========================================================
    // Direction (Inbound/Outbound)
    // =========================================================
    public ObservableCollection<string> Directions { get; } =
        new() { "Inbound", "Outbound" };

    private string _selectedDirection;
    public string SelectedDirection
    {
        get => _selectedDirection;
        set
        {
            if (string.Equals(_selectedDirection, value, StringComparison.Ordinal)) return;
            _selectedDirection = value;

            OnPropertyChanged();
            OnPropertyChanged(nameof(IsOutbound));

            // Refresh dependent UI/data when direction changes
            _ = LoadTableOptionsAsync();
        }
    }

    public bool IsOutbound => SelectedDirection == "Outbound";

    // =========================================================
    // Outbound tables/groups
    // =========================================================
    public ObservableCollection<string> TableOptions { get; } = new();

    private string? _selectedTableOption;
    public string? SelectedTableOption
    {
        get => _selectedTableOption;
        set
        {
            if (string.Equals(_selectedTableOption, value, StringComparison.Ordinal)) return;
            _selectedTableOption = value;
            OnPropertyChanged();
        }
    }

    // =========================================================
    // Send To CIS
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
    // TAB 1 - Daily Schedule (DataGrid backing)
    // =========================================================
    public ObservableCollection<DailyScheduleRow> Table1Items { get; } = new();

    private DailyScheduleRow? _selectedTable1Item;
    public DailyScheduleRow? SelectedTable1Item
    {
        get => _selectedTable1Item;
        set
        {
            if (ReferenceEquals(_selectedTable1Item, value)) return;
            _selectedTable1Item = value;
            OnPropertyChanged();
        }
    }

    // =========================================================
    // Load Methods
    // =========================================================
    //public void ForceAddTestRow()
    //{
    //    Table1Items.Add(new DailyScheduleRow
    //    {
    //        Cust = "Ralph",
    //        Job = "TEST JOB",
    //        TargetComputer = "LOCAL",
    //        ScheduleTime = TimeSpan.FromHours(12),
    //        ExecuteWeekDays = "MTWTF",
    //        IsActive = true,
    //        RUNGROUP = "TEST",
    //        SendCIS = true
    //    });
    //}

    public async Task LoadCustomersAsync()
    {

        //MessageBox.Show("LoadCustomersAsync called");
        //ForceAddTestRow();
        var rows = await _sqlService.GetCustomersAsync();
        //MessageBox.Show("LoadDailyScheduleAsync entered");
        Customers.Clear();
        foreach (var row in rows)
            Customers.Add(row);

        if (Customers.Count > 0)
            SelectedCustomer = Customers[0];   // triggers LoadTableOptionsAsync + LoadDailyScheduleAsync
        else
        {
            // ensure consistent UI state
            await LoadTableOptionsAsync();
            Table1Items.Clear();
        }
    }

    private async Task LoadTableOptionsAsync()
    {
        TableOptions.Clear();
        TableOptions.Add("ALL");

        if (IsOutbound &&
            SelectedCustomer is not null &&
            !string.IsNullOrWhiteSpace(SelectedCustomer.InitialCatalog))
        {
            var tables = await _sqlService.GetOutboundTableNamesAsync(SelectedCustomer.InitialCatalog);

            foreach (var t in tables)
            {
                if (string.IsNullOrWhiteSpace(t)) continue;
                if (string.Equals(t, "ALL", StringComparison.OrdinalIgnoreCase)) continue;
                if (!TableOptions.Contains(t)) TableOptions.Add(t);
            }
        }

        // Always reset to ALL when list changes
        SelectedTableOption = "ALL";
        OnPropertyChanged(nameof(TableOptions));
        OnPropertyChanged(nameof(SelectedTableOption));
    }

    //private async Task LoadDailyScheduleAsync()
    //{
    //    Table1Items.Clear();

    //    //if (SelectedCustomer == null)
    //    //    return;

    //    var rows = await _dailyScheduleRepo.GetByCustomerAsync(SelectedCustomer.Customer);

    //    //foreach (var row in rows)
    //    //    Table1Items.Add(row);
    //    Application.Current.Dispatcher.Invoke(() =>
    //    {
    //        Table1Items.Clear();
    //        foreach (var row in rows)
    //            Table1Items.Add(row);
    //    });
    //}
    private async Task LoadDailyScheduleAsync()
    {
        if (SelectedCustomer == null)
            return;

        var rows = await _dailyScheduleRepo.GetByCustomerAsync(SelectedCustomer.Customer);

        Table1Items.Clear();

        foreach (var row in rows)
            Table1Items.Add(row);
    }
    // =========================================================
    // INotifyPropertyChanged
    // =========================================================
    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}