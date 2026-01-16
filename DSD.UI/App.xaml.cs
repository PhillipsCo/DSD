
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Windows;
using DSD.Common.Services;
using DSD.Inbound.Runners;
using DSD.Outbound.Runners;
using DSD.UI;
namespace DSD.UI
{
    public partial class App : Application
    {
        private IServiceProvider _serviceProvider;

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            // Load configuration
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            // Configure DI
            var services = new ServiceCollection();
            services.AddSingleton<IConfiguration>(configuration);

            // Register services from Common
            services.AddTransient<SqlService>();
            services.AddTransient<ApiExecutorService>();
            services.AddTransient<CsvHelperService>();
            services.AddTransient<FtpService>();
            services.AddTransient<EmailService>();

            // Register runners
            services.AddTransient<InboundAppRunner>();
            services.AddTransient<OutboundAppRunner>();

            // Register MainWindow
            services.AddTransient<MainWindow>();

            _serviceProvider = services.BuildServiceProvider();

            var mainWindow = _serviceProvider.GetRequiredService<MainWindow>();
            mainWindow.Show();
        }
    }
}
