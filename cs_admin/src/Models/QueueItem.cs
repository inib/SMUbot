public class QueueItem
{
    public int Id { get; set; }
    public int SongId { get; set; }
    public int UserId { get; set; }
    public bool IsPriority { get; set; }
    public bool Played { get; set; }

    public string Artist { get; set; } = string.Empty;   // for UI convenience
    public string Title  { get; set; } = string.Empty;
    public string Requester { get; set; } = string.Empty;

    public string DisplayLine1 => $"{Artist} - {Title}";
    public string DisplayLine2 => $"by {Requester}";
}