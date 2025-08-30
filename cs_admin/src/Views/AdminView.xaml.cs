using System.Windows;
using System.Windows.Controls;
using TwitchSongAdmin.ViewModels;

namespace TwitchSongAdmin.Views;

public partial class AdminView : UserControl
{
    public AdminView() { InitializeComponent(); }

    private void SaveToken_Click(object sender, RoutedEventArgs e)
    {
        if (DataContext is AdminViewModel vm)
            vm.AdminToken = TokenBox.Password; // TODO: secure storage later
    }

    private void BrowseUnzip_Click(object sender, RoutedEventArgs e)
    {
        if (DataContext is AdminViewModel vm)
        {
            var dlg = new Microsoft.Win32.OpenFileDialog { Filter = "Executable|*.exe;*.cmd;*.bat|All|*.*" };
            if (dlg.ShowDialog() == true) vm.UnzipToolPath = dlg.FileName;
        }
    }
}