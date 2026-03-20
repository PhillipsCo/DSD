using DSD.Common.Models;
using DSD.Common.Services;
using DSD.UI.ViewModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Graph.Models;
using System;
using System.Windows;
using System.Windows.Controls;

namespace DSD.UI.Views
{
    public partial class ApiTesterView : UserControl
    {

        public ApiTesterView()
        {
            InitializeComponent();
        }

        public ApiTesterView(ApiTesterViewModel viewModel) : this()
        {
            DataContext = viewModel;
        }



    }


}

