namespace TwitchSongAdmin.Models;

public class User
{
    public int Id { get; set; }
    public string Username { get; set; } = string.Empty;
    public string TwitchId { get; set; } = string.Empty;

    public int PrioPoints { get; set; }
}