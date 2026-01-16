
using System.Diagnostics;
using System.IO;
using System.Windows;

namespace DSD.UI
{
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void RunInbound_Click(object sender, RoutedEventArgs e)
        {
            // Get values from text boxes
            string customer = CustomerInput.Text;
            string group = GroupInput.Text;
            string cis = CISInput.Text;

            // Run DSD.Inbound with arguments
            RunConsoleApp(@"C:\CIS\Programs\DSD\DSD.Inbound\bin\Release\net8.0\DSD.Inbound.exe", customer, group, cis);
        }

        private void RunOutbound_Click(object sender, RoutedEventArgs e)
        {
            // Get values from text boxes
            string customer = CustomerInput.Text;
            string group = GroupInput.Text;
            string cis = CISInput.Text;

            // Run DSD.Outbound with arguments
            RunConsoleApp(@"C:\CIS\Programs\DSD\DSD.Outbound\bin\Release\net8.0\DSD.Outbound.exe", customer, group, cis);
        }

        private void RunConsoleApp(string exeName, string customer, string group, string cis)
        {
            string exePath = Path.Combine(Directory.GetCurrentDirectory(), exeName);

            if (File.Exists(exePath))
            {
                Process.Start(new ProcessStartInfo
                {
                    FileName = exePath,
                    Arguments = $"{customer} {group} {cis}", // Pass arguments to console app
                    UseShellExecute = true
                });
            }
            else
            {
                MessageBox.Show($"Executable not found: {exePath}");
            }
        }
    }
}
