using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using System.Windows.Input;
using TwitchSongAdmin.Models;
using TwitchSongAdmin.Services;
using TwitchSongAdmin.Utils;

namespace TwitchSongAdmin.ViewModels;

public class AdminViewModel : INotifyPropertyChanged
{
    private readonly ApiClient _api;
    private readonly QueueViewModel _queue;
    private Settings _settings = SettingsStore.Load();

    private string _apiBaseUrl = "http://localhost:8000";
    private string _adminToken = string.Empty;    

    public int MaxEntries { get; set; } = 50;
    public int MaxPerUser { get; set; } = 3;
    public bool PrioOnly { get; set; } = false;
    
    public string ApiBaseUrl { get => _settings.ApiBaseUrl; set { _settings.ApiBaseUrl = value; OnPropertyChanged(); } }
    public string AdminToken { get => _settings.AdminToken; set { _settings.AdminToken = value; OnPropertyChanged(); } }
    public int ChannelId { get => _settings.ChannelId; set { _settings.ChannelId = value; OnPropertyChanged(); _queue.ChannelId = value; } }
    public string DownloadScriptPath { get => _settings.DownloadScriptPath; set { _settings.DownloadScriptPath = value; OnPropertyChanged(); } }
    public string UnzipToolPath { get => _settings.UnzipToolPath; set { _settings.UnzipToolPath = value; OnPropertyChanged(); _queue.UnzipToolPath = value; } }

    public ICommand ApplySettingsCommand { get; }
    public ICommand LoadSettingsCommand { get; }
    public ICommand SaveSettingsCommand { get; }

    public AdminViewModel(ApiClient api, QueueViewModel queue)
    {
        _api = api; _queue = queue;
        // push initial config to other VMs
        _api.Configure(ApiBaseUrl, AdminToken);
        _queue.ChannelId = ChannelId;
        _queue.UnzipToolPath = UnzipToolPath;

        ApplySettingsCommand = new RelayCommand(_ => _api.Configure(ApiBaseUrl, AdminToken));
        LoadSettingsCommand = new RelayCommand(_ => LoadSettings());
        SaveSettingsCommand = new RelayCommand(_ => SettingsStore.Save(_settings));
    }

    private void LoadSettings()
    {
        _settings = SettingsStore.Load();
        OnPropertyChanged(nameof(ApiBaseUrl));
        OnPropertyChanged(nameof(AdminToken));
        OnPropertyChanged(nameof(ChannelId));
        OnPropertyChanged(nameof(DownloadScriptPath));
        OnPropertyChanged(nameof(UnzipToolPath));

        _api.Configure(ApiBaseUrl, AdminToken);
        _queue.ChannelId = ChannelId;
        _queue.UnzipToolPath = UnzipToolPath;
    }

    private void LoadChannelsStub()
    {
        // TODO: GET /channels and fill Channels; keep stub for now
    }


    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged([CallerMemberName] string? n = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(n));
}