namespace TwitchSongAdmin.Models;

public class Settings
{
    public string ApiBaseUrl { get; set; } = "http://localhost:8000";
    public string AdminToken { get; set; } = "";
    public int ChannelId { get; set; } = 1;
    public string DownloadScriptPath { get; set; } = ""; // existing
    public string UnzipToolPath { get; set; } = "";      // NEW
}