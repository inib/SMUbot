using System.Collections.ObjectModel;
using TwitchSongAdmin.Models;

namespace TwitchSongAdmin.ViewModels;

public class LibraryViewModel
{
    public ObservableCollection<Song> StemsAvailable { get; } = new();
    public ObservableCollection<Song> DownloadedOnly { get; } = new();

    public void RefreshFromCrawler()
    {
        // TODO: call Crawler to populate collections
    }
}