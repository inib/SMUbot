using System.Windows;
using TwitchSongAdmin.ViewModels;

namespace TwitchSongAdmin;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        DataContext = new MainViewModel();
    }
}