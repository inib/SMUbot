using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using System.IO;
using TwitchSongAdmin.Models;

namespace TwitchSongAdmin.Services
{
    public static class SettingsStore
    {
        private static string Dir =>
            System.IO.Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "TwitchSongAdmin");
        private static string FilePath => System.IO.Path.Combine(Dir, "settings.json");

        public static Settings Load()
        {
            try
            {
                if (!System.IO.File.Exists(FilePath)) return new Settings();
                var json = System.IO.File.ReadAllText(FilePath);
                return JsonSerializer.Deserialize<Settings>(json) ?? new Settings();
            }
            catch { return new Settings(); }
        }

        public static void Save(Settings s)
        {
            try
            {
                Directory.CreateDirectory(Dir);
                var json = JsonSerializer.Serialize(s, new JsonSerializerOptions { WriteIndented = true });
                System.IO.File.WriteAllText(FilePath, json);
            }
            catch { /* ignore */ }
        }
    }
}
