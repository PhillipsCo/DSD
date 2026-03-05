
using DSD.Common.Services;
using DSD.UI.ViewModels;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.IO;
using System.Windows;
//using static Org.BouncyCastle.Math.EC.ECCurve;

namespace DSD.UI
{
    public partial class MainWindow : Window
    {
        private readonly IConfiguration _config;
        public MainWindow()
        {
            InitializeComponent();
            //MessageBox.Show($"MainWindow created: {GetHashCode()}");

            _config = new ConfigurationBuilder()
                        .SetBasePath(AppContext.BaseDirectory)
                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                        .Build();

            //var appsettingsPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");

            //MessageBox.Show(
            //    $"BaseDirectory:\n{AppContext.BaseDirectory}\n\n" +
            //    $"appsettings exists? {File.Exists(appsettingsPath)}\n\n" +
            //    $"InboundPath from config:\n{_config["InboundPath"]}\n\n" +
            //    $"OutboundPath from config:\n{_config["OutboundPath"]}",
            //    "Config Diagnostic");





            ISqlService sqlService = new SqlService(_config);

            DataContext = new MainViewModel(sqlService, _config);

            Loaded += async (_, __) =>
                await ((MainViewModel)DataContext).LoadCustomersAsync();




        }


    }
}
