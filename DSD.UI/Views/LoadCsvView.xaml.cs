using DSD.Common.Services;
using DSD.UI.ViewModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace DSD.UI.Views
{
    /// <summary>
    /// Interaction logic for LoadCsvView.xaml
    /// </summary>
    public partial class LoadCsvView : UserControl
    {
        private readonly SqlService _sqlService;
        public LoadCsvView()
        {
            InitializeComponent();


            var config = new ConfigurationBuilder()
                        .SetBasePath(AppContext.BaseDirectory)
                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                        .Build();

            _sqlService = new SqlService(config);


        }

        private void Browse_Click(object sender, RoutedEventArgs e)
        {
            var dlg = new OpenFolderDialog
            {
                Title = "Select folder containing CSV files",
                
            };

            if (dlg.ShowDialog() == true)
            {
                FilePathTextBox.Text = dlg.FolderName;
                StatusTextBlock.Text = "";
            }
        }

        // This MUST match Click="Send_Click" in XAML
        private async void Send_Click(object sender, RoutedEventArgs e)
        {
            var filePath = FilePathTextBox.Text;

            if (string.IsNullOrWhiteSpace(filePath))
            {
                StatusTextBlock.Text = "Please select a CSV file first.";
                return;
            }


            var catalog =
                ((MainViewModel)DataContext)
                    .CustomerInfo
                    .Current
                    .InitialCatalog;

            if (string.IsNullOrWhiteSpace(catalog))
            {
                StatusTextBlock.Text = "InitialCatalog is empty. Check Customer Info tab.";
                return;
            }

            if (!Directory.EnumerateFiles(filePath, "*.csv").Any())
            {
                StatusTextBlock.Text = "No CSV files found in the selected folder.";
                return;
            }

            await Task.Run(() =>
                    _sqlService.InsertCSV(catalog, filePath));

            StatusTextBlock.Text = "Done.";




        }


    }
}
