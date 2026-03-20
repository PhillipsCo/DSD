using DSD.Common.Services;
using DSD.UI.ViewModels;
using DSD.UI.Views;
using Microsoft.Extensions.Configuration;
using System;
using System.Windows;

namespace DSD.UI
{
    /// <summary>
    /// MainWindow
    /// ----------
    /// This is the root window of the application.
    /// 
    /// Responsibilities:
    /// 1. Load configuration (appsettings.json)
    /// 2. Create core services (SqlService, repositories)
    /// 3. Create the MainViewModel
    /// 4. Create child ViewModels that REQUIRE constructor parameters
    /// 5. Inject those ViewModels into their corresponding Views
    /// 
    /// IMPORTANT:
    /// WPF does NOT create ViewModels automatically.
    /// Any ViewModel with constructor parameters MUST be created here (or in MainViewModel).
    /// </summary>
    public partial class MainWindow : Window
    {
        // Holds configuration loaded from appsettings.json
        private readonly IConfiguration _config;

        public MainWindow()
        {
            // -------------------------------
            // 1️⃣ Load XAML and create controls
            // -------------------------------
            // This parses MainWindow.xaml and creates the visual tree.
            // If something in XAML is wrong, it will throw here.
            InitializeComponent();

            // -------------------------------------
            // 2️⃣ Build configuration (appsettings)
            // -------------------------------------
            _config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile(
                    "appsettings.json",
                    optional: false,      // app must fail if missing
                    reloadOnChange: true) // auto-reload if file changes
                .Build();

            // -------------------------------------
            // 3️⃣ Create core services
            // -------------------------------------
            // SqlService is a shared service used by MainViewModel
            ISqlService sqlService = new SqlService(_config);

            // -------------------------------------
            // 4️⃣ Create MainViewModel
            // -------------------------------------
            // This is the PRIMARY ViewModel for the window.
            // It owns:
            //  - Customers list
            //  - SelectedCustomer
            //  - CustomerInfoViewModel
            var mainVm = new MainViewModel(sqlService, _config);

            // Assign MainViewModel as the DataContext of the window
            // All bindings in MainWindow.xaml flow from this.
            DataContext = mainVm;

            // -------------------------------------
            // 5️⃣ CREATE ApiTesterViewModel (CRITICAL STEP)
            // -------------------------------------
            // ⚠️ This does NOT happen automatically.
            // ⚠️ If this is missing, the API Tester button will do nothing.

            // Repository used by ApiTesterViewModel
            var apiTesterRepo = new ApiTesterRepository();

            // Create the ApiTesterViewModel
            // IMPORTANT:
            // We pass in the SAME CustomerInfoViewModel used by the Customer Info tab
            var apiTesterVm = new ApiTesterViewModel(
                apiTesterRepo,
                mainVm.CustomerInfoViewModel
            );

            // -------------------------------------
            // 6️⃣ CREATE ApiTesterView AND INJECT ViewModel
            // -------------------------------------
            // We do NOT create ApiTesterView in XAML.
            // We create it here so we can pass the ViewModel in.
            ApiTesterTab.Content = new ApiTesterView(apiTesterVm);

            // -------------------------------------
            // 7️⃣ Load initial data after window is shown
            // -------------------------------------
            Loaded += async (_, __) =>
            {
                // Load customers once UI is ready
                await mainVm.LoadCustomersAsync();
            };
        }
    }
}