using TwitchSongAdmin.Services;

namespace TwitchSongAdmin.ViewModels;

public class MainViewModel
{
    public ApiClient Api { get; }

    public QueueViewModel Queue { get; }
    public AdminViewModel Admin { get; }
    public LibraryViewModel Library { get; }

    public MainViewModel()
    {
        Api = new ApiClient();
        Api.Configure("http://localhost:8000", ""); // adjusted via Admin tab later

        Queue = new QueueViewModel(Api) { ChannelId = 1 };
        Admin = new AdminViewModel(Api, Queue);
        Library = new LibraryViewModel();
    }
}