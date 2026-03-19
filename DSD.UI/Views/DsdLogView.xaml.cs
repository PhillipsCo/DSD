using DSD.Common.Services;
using DSD.Common.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
    /// Interaction logic for DsdLogView.xaml
    /// </summary>
    /// 
    
    public partial class DsdLogView : UserControl
    {
        //private readonly SqlService _sqlService;
        private readonly DsdLogRepository _repo;
        public ObservableCollection<DsdLog> Logs { get; } = new();

        public DateTime SelectedDate{get; set;}
        public DsdLogView()
        {
            InitializeComponent();

            SelectedDate = DateTime.Today;

            DataContext = this;

            var config = new ConfigurationBuilder()
                        .SetBasePath(AppContext.BaseDirectory)
                        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                        .Build();
            _repo = new DsdLogRepository(config);
            //_sqlService = new SqlService(config);
            //Loaded += DsdLogView_Loaded;
            Loaded += async (_, __) => await LoadLogsAsync();
        }


        private async void DatePicker_SelectedDateChanged(object sender, SelectionChangedEventArgs e)
        {
            // If the user clears the date, SelectedDate might be default/null depending on your binding.
            // If you used DateTime (non-nullable), it will never be null.
            await LoadLogsAsync();
        }

        private async Task LoadLogsAsync()
        {
            var date = DateOnly.FromDateTime(SelectedDate);
            var rows = await _repo.GetByDateAsync(date);

            Logs.Clear();
            foreach (var row in rows)
                Logs.Add(row);
        }




        //private void DsdLogView_Loaded(object sender, RoutedEventArgs e)
        //{
        //    throw new NotImplementedException();
        //}
    }
}
