using System;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;

namespace WindbellTank
{
    public class ErrorData
    {
        public string code { get; set; }
        public string message { get; set; }
    }

    public class TankData
    {
        public int tank_id { get; set; }
        public string product_code { get; set; }
        public decimal? oil_level { get; set; }
        public decimal? water_level { get; set; }
        public decimal? temperature { get; set; }
        public decimal? volume { get; set; }
        public decimal? water_volume { get; set; }
        public decimal? tc_volume { get; set; }
        public decimal? capacity { get; set; }
        public decimal? Ullage { get; set; }
        public string sensor_status { get; set; }
        public ErrorData error { get; set; }
    }

    public class AtgMetadata
    {
        public string request_id { get; set; }
        public string timestamp { get; set; }
    }

    public class AtgResponse
    {
        public bool success { get; set; }
        public AtgMetadata metadata { get; set; }
        public List<TankData> data { get; set; }
    }

    class Program
    {
        private static bool _tableEnsured = false;
        private static readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private static string _lastAtgDataCache = null; // YENİ: Eyni datanın təkrar-təkrar bazaya yazılmasının qarşısını alacaq yaddaş

        // Gələcəkdə mesajları vahid formatda çıxarmaq üçün loqlama funksiyası (Vaqt göstəricisi ilə)
        static void Log(string message, ConsoleColor color = ConsoleColor.White)
        {
            Console.ForegroundColor = color;
            Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}");
            Console.ResetColor();
        }

        static string GetConnectionString()
        {
            string machineName = Environment.MachineName;
            // Xətaya düşməməsi üçün Connection Timeout parametrini əlavə edirik (Uzun müddət gözləməməsi üçün)
            return $"Server={machineName};Database=ofisServer;User Id=sa;Password=374474;Encrypt=False;Connection Timeout=10;";
        }

        static int GetTankCountFromDatabase()
        {
            try
            {
                using (var conn = new SqlConnection(GetConnectionString()))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand("SELECT COUNT(*) FROM TankConfig", conn))
                    {
                        var res = cmd.ExecuteScalar();
                        if (res != null && res != DBNull.Value)
                        {
                            int count = Convert.ToInt32(res);
                            if (count > 0) return count;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"\u26A0 Verilənlər bazasından çən sayı oxunarkən xəta: {ex.Message}", ConsoleColor.Yellow);
            }
            return 1; // Default - Heç nə tapılmazsa 1 qayıdırıq ki, xəta atmasın və ən azı 1 çəni yoxlasın.
        }

        static string GetIpFromDatabase()
        {
            try
            {
                using (var conn = new SqlConnection(GetConnectionString()))
                {
                    conn.Open();
                    // SQL Injection və yaza bağlı problemləri önləmək üçün 'isnull' və 'len' istifadə edilir
                    using (var cmd = new SqlCommand("SELECT TOP 1 ip FROM TankConfig WHERE len(isnull(ip, '')) > 0", conn))
                    {
                        var res = cmd.ExecuteScalar();
                        if (res != null && res != DBNull.Value)
                        {
                            return res.ToString().Trim();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"\u26A0 Verilənlər bazasından məlumat oxunarkən xəta: {ex.Message}", ConsoleColor.Yellow);
            }
            return null;
        }

        static void UpdateIpInDatabase(string newIp)
        {
            try
            {
                string connString = GetConnectionString();
                Log($"SQL Serverə qoşulur (IP yeniləmək üçün)...", ConsoleColor.Cyan);

                using (var conn = new SqlConnection(connString))
                {
                    conn.Open();
                    // GƏLƏCƏYƏ DÖNÜK DÜZƏLİŞ: Parametrizə edilmiş SQL (SQL Injection qarşısını alır və təhlükəsizdir)
                    string updateSql = "UPDATE TankConfig SET ip = @ip";
                    using (var cmd = new SqlCommand(updateSql, conn))
                    {
                        cmd.Parameters.AddWithValue("@ip", newIp);
                        int rows = cmd.ExecuteNonQuery();
                        
                        if (rows == 0)
                        {
                            // Əgər cədvəl tamamilə boşdursa
                            string insertSql = "INSERT INTO TankConfig (ip) VALUES (@ip)";
                            using (var insertCmd = new SqlCommand(insertSql, conn))
                            {
                                insertCmd.Parameters.AddWithValue("@ip", newIp);
                                int inserted = insertCmd.ExecuteNonQuery();
                                Log($"\u2714 Cədvəl boş idi, {inserted} yeni sətir əlavə olundu və IP yazıldı: {newIp}", ConsoleColor.Green);
                            }
                        }
                        else
                        {
                            Log($"\u2714 IP ünvan bazada olan bütün {rows} sətrə '{newIp}' olaraq uğurla yeniləndi.", ConsoleColor.Green);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Log("=========================================", ConsoleColor.Red);
                Log($"\u26A0 DİQQƏT! VERİLƏNLƏR BAZASINA IP YAZILARKƏN XƏTA BAŞ VERDİ!", ConsoleColor.Red);
                Log($"Xəta mesajı: {ex.Message}", ConsoleColor.Red);
                Log($"Səbəb ola bilər: Cədvəl yoxdur, icazə yoxdur, və ya Server Adı səhvdir.", ConsoleColor.Red);
                Log("=========================================", ConsoleColor.Red);
                Console.WriteLine();
            }
        }

        static void EnsureDatabaseTables()
        {
            return; // Ləğv edildi, çünki AtgData əvəzinə hazır TankConfig işlədirik

            try
            {
                using (var conn = new SqlConnection(GetConnectionString()))
                {
                    conn.Open();
                    // GƏLƏCƏYƏ DÖNÜK: ErrorMessage nvarchar(max) olaraq təyin edilib ki, kəsilmə(Truncation) baş verməsin
                    string createTableSql = @"
                        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[AtgData]') AND type in (N'U'))
                        BEGIN
                            CREATE TABLE [dbo].[AtgData](
                                [Id] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
                                [RequestTimestamp] [nvarchar](50) NULL,
                                [RequestId] [nvarchar](50) NULL,
                                [TankId] [int] NULL,
                                [ProductCode] [nvarchar](50) NULL,
                                [OilLevel] [float] NULL,
                                [WaterLevel] [float] NULL,
                                [Temperature] [float] NULL,
                                [Volume] [float] NULL,
                                [WaterVolume] [float] NULL,
                                [TcVolume] [float] NULL,
                                [Capacity] [float] NULL,
                                [Ullage] [float] NULL,
                                [SensorStatus] [nvarchar](50) NULL,
                                [ErrorCode] [nvarchar](50) NULL,
                                [ErrorMessage] [nvarchar](max) NULL,
                                [CreatedAt] [datetime] DEFAULT GETDATE()
                            )
                        END";

                    using (var cmd = new SqlCommand(createTableSql, conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    _tableEnsured = true;
                }
            }
            catch (Exception ex)
            {
                Log($"\u26A0 AtgData cədvəli qurularkən və ya yoxlanılarkən xəta: {ex.Message}", ConsoleColor.Red);
            }
        }

        static void SaveAtgDataToDatabase(AtgResponse response)
        {
            if (response?.data == null || response.data.Count == 0) return;

            EnsureDatabaseTables();

            try
            {
                using (var conn = new SqlConnection(GetConnectionString()))
                {
                    conn.Open();
                    string upsertQuery = @"
                        IF EXISTS (SELECT 1 FROM TankConfig WHERE TankOid = @TankOid)
                        BEGIN
                            UPDATE TankConfig
                            SET 
                                YanacaqCode = ISNULL(@YanacaqCode, YanacaqCode),
                                TankCapacity = @TankCapacity,
                                TankLength = @TankLength,
                                CurrentVolume = @CurrentVolume,
                                waterleve = @WaterLevel,
                                temperature = @Temperature,
                                watervolume = @WaterVolume,
                                tcvolume = @TcVolume,
                                ullage = @Ullage,
                                sensorStatus = @SensorStatus,
                                error = @Error,
                                LastUpdate = GETDATE()
                            WHERE TankOid = @TankOid
                        END
                        ELSE
                        BEGIN
                            INSERT INTO TankConfig (TankOid, YanacaqCode, TankCapacity, TankLength, CurrentVolume, waterleve, temperature, watervolume, tcvolume, ullage, sensorStatus, error, ModificationDate, LastUpdate)
                            VALUES (@TankOid, @YanacaqCode, @TankCapacity, @TankLength, @CurrentVolume, @WaterLevel, @Temperature, @WaterVolume, @TcVolume, @Ullage, @SensorStatus, @Error, GETDATE(), GETDATE());
                        END";

                    int successCount = 0;
                    
                    // GƏLƏCƏYƏ DÖNÜK DÜZƏLİŞ: Transaction istifadə edirik ki, məlumatların bir hissəsi yazılıb, digər hissəsi xəta verdikdə məlumat bazası korlanmasın (Bütünlüklə qəbul və ya ləğv edilir).
                    using (var transaction = conn.BeginTransaction())
                    {
                        try
                        {
                            foreach (var tank in response.data)
                            {
                                if (tank == null) continue;

                                using (var cmd = new SqlCommand(upsertQuery, conn, transaction))
                                {
                                    cmd.Parameters.AddWithValue("@TankOid", tank.tank_id);

                                    object yanacaqCodeVal = DBNull.Value;
                                    if (!string.IsNullOrEmpty(tank.product_code) && int.TryParse(tank.product_code, out int pCode))
                                    {
                                        yanacaqCodeVal = pCode;
                                    }
                                    cmd.Parameters.AddWithValue("@YanacaqCode", yanacaqCodeVal);

                                    cmd.Parameters.AddWithValue("@TankCapacity", (object)tank.capacity ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@TankLength", (object)tank.oil_level ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@CurrentVolume", (object)tank.volume ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@WaterLevel", (object)tank.water_level ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@Temperature", (object)tank.temperature ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@WaterVolume", (object)tank.water_volume ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@TcVolume", (object)tank.tc_volume ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@Ullage", (object)tank.Ullage ?? DBNull.Value);
                                    cmd.Parameters.AddWithValue("@SensorStatus", (object)tank.sensor_status ?? "");

                                    string errorDesc = "";
                                    if (tank.error != null && (!string.IsNullOrEmpty(tank.error.code) || !string.IsNullOrEmpty(tank.error.message)))
                                    {
                                        errorDesc = $"[{tank.error.code}] {tank.error.message}";
                                    }
                                    cmd.Parameters.AddWithValue("@Error", errorDesc);

                                    cmd.ExecuteNonQuery();
                                    successCount++;
                                }
                            }
                            transaction.Commit();
                            Log($"\u2714 {successCount} çən məlumatı TankConfig cədvəlinə uğurla yazıldı.", ConsoleColor.Green);
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            Log($"\u26A0 Məlumatlar verilənlər bazasına yazılarkən xəta baş verdi, tam geriyə qaytarıldı (Rollback): {ex.Message}", ConsoleColor.Red);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"\u26A0 Database-ə bağlanmaq mümkün olmadı: {ex.Message}", ConsoleColor.Red);
            }
        }

        static async Task Main(string[] args)
        {
            // Konsolda Azərbaycan dilini (Ü,Ö,Ğ,Ç,Ş,I,Ə) tam dəstəkləmək üçün
            Console.OutputEncoding = Encoding.UTF8;

            // Proqramın məcburi/təhlükəsiz dayandırılması (Ctrl+C basıldıqda)
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Log("Proqram istifadəçi tərəfindən dayandırılır...", ConsoleColor.DarkYellow);
                _cts.Cancel();
            };

            int devicePort = 5656;
            string deviceIp = null;

            Log("--- Windbell WB-SS200 Test Başladı ---", ConsoleColor.Cyan);

            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    int tankCount = GetTankCountFromDatabase();

                    Log($"\n--- Oxunacaq çən sayı: {tankCount} ---", ConsoleColor.DarkGray);

                    int maxRetries = 3;
                    bool connectionSuccess = false;

                    if (string.IsNullOrEmpty(deviceIp))
                    {
                        deviceIp = GetIpFromDatabase();
                    }

                    if (string.IsNullOrEmpty(deviceIp))
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write($"\nBazada IP ünvanı tapılmadı.\nZəhmət olmasa IP ünvanı daxil edin: ");
                        Console.ResetColor();

                        // Ctrl+C edilibsə bura kəsiləcək. Konsolu dondurmaması üçün sadəcə oxuma gözləyirik.
                        string inputIp = Console.ReadLine()?.Trim();
                        if (!string.IsNullOrEmpty(inputIp))
                        {
                            deviceIp = inputIp;
                            UpdateIpInDatabase(deviceIp);
                        }
                        else 
                        {
                            Log("IP ünvan daxil edilmədi. Cəhd 2 saniyə sonra yenilənəcək...", ConsoleColor.Yellow);
                            try { await Task.Delay(2000, _cts.Token); } catch (TaskCanceledException) { break; }
                            continue;
                        }
                    }

                    Log($"[{deviceIp}:{devicePort}] cihazına qoşulmağa cəhd edilir...", ConsoleColor.Cyan);

                    for (int attempt = 1; attempt <= maxRetries; attempt++)
                    {
                        if (_cts.Token.IsCancellationRequested) break;

                        try
                        {
                            using (TcpClient client = new TcpClient())
                            {
                                // Qoşulmağa cəhd. 5 saniyə timeout - donmaların (sonsuz gözləmələrin) qarşısını almaq üçün təhlükəsizdir
                                var connectTask = client.ConnectAsync(deviceIp, devicePort);
                                if (await Task.WhenAny(connectTask, Task.Delay(5000, _cts.Token)) != connectTask)
                                {
                                    throw new Exception("Bağlantı vaxtı bitdi (Timeout - 5 san). Server və ya cihaz əlçatmaz ola bilər.");
                                }

                                if (!client.Connected)
                                {
                                    throw new Exception("Bağlantı qurula bilmədi.");
                                }

                                using (NetworkStream stream = client.GetStream())
                                {
                                    // 1. Sorğu komandası 
                                    var tankList = new List<string>();
                                    for (int i = 1; i <= tankCount; i++)
                                    {
                                        tankList.Add($"\"Tank{i}\"");
                                    }
                                    string request = $"{{\"tanks\": [{string.Join(", ", tankList)}], \"requestType\": \"status\"}}";
                                    byte[] requestBytes = Encoding.UTF8.GetBytes(request);
                                    await stream.WriteAsync(requestBytes, 0, requestBytes.Length, _cts.Token);

                                    // 2. Cavabı tam göndərilənədək yığıb oxumaq
                                    StringBuilder responseBuilder = new StringBuilder();
                                    byte[] buffer = new byte[8192];
                                    AtgResponse result = null;
                                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

                                    while (true)
                                    {
                                        var readTask = stream.ReadAsync(buffer, 0, buffer.Length, _cts.Token);
                                        // GƏLƏCƏYƏ DÖNÜK YOXLANIŞ: Əgər cihaz datanı yarımçıq göndərib susarsa proqram donmasın. Max 10 saniyə.
                                        if (await Task.WhenAny(readTask, Task.Delay(10000, _cts.Token)) != readTask) 
                                        {
                                            throw new Exception("Cihazdan növbəti məlumatın gəlməsi gecikdi (Oxuma Timeout).");
                                        }

                                        int bytesRead = await readTask;
                                        if (bytesRead == 0)
                                        {
                                            throw new Exception("Bağlantı qarşı tərəfdən gözlənilmədən kəsildi.");
                                        }

                                        string chunk = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                                        responseBuilder.Append(chunk);

                                        // GƏLƏCƏYƏ DÖNÜK DÜZƏLİŞ: Davamlı sonsuz (zibil) data gələrsə RAM(Yaddaş) daşmasının qarşısını alırıq.
                                        if (responseBuilder.Length > 5 * 1024 * 1024) 
                                        {
                                            throw new Exception("Cihazdan gələn məlumat həddindən artıq böyükdür (Yaddaş qorunması tetikləndi).");
                                        }

                                        string currentResponse = responseBuilder.ToString();

                                        try
                                        {
                                            // Təkrar gələn zibil null xarakterlərini təmizləyirik (Bəzi cihazlar \0 əlavə edir)
                                            string trimmed = currentResponse.Replace("\0", "").Trim();
                                            // Performanslı Yoxlama: JSON-un tam bitməsini ancaq `{` ilə başlayıb `}` ilə bitməsindən müəyyən edirik. 
                                            // Lazımsız Exception-ların (CPU yükləməsinin) qarşısını alır.
                                            if (trimmed.StartsWith("{") && trimmed.EndsWith("}"))
                                            {
                                                result = JsonSerializer.Deserialize<AtgResponse>(currentResponse, options);
                                                if (result != null)
                                                {
                                                    break; // Tam ölçülü və etibarlı JSON oxundu
                                                }
                                            }
                                        }
                                        catch (JsonException)
                                        {
                                            // JSON hələ yarımçıqdır (sintaksis tam deyil), oxumağa davam edir (Buffer böyütmə normaldır)
                                        }
                                    }

                                    // 3. JSON-u ekrana çıxarmaq
                                    if (result != null && result.data != null)
                                    {
                                        // "eyni məlumatların 2-ci dəfə bazaya gedib consol-da çıxmama" xətasının dəqiq həlli: 
                                        // Yalnız tank məlumatları fiziki olaraq dəyişdikdə bazaya insert edirik.
                                        string currentDataJson = JsonSerializer.Serialize(result.data.Where(t => t != null).OrderBy(t => t.tank_id));
                                        
                                        bool isDataChanged = currentDataJson != _lastAtgDataCache;
                                        if (isDataChanged)
                                        {
                                            SaveAtgDataToDatabase(result);
                                            _lastAtgDataCache = currentDataJson;
                                        }
                                        else
                                        {
                                            // Eyni məlumat gəlibsə təkrar insert qadağan edilir, yalnız console-da tarix yenillənib göstərilir
                                            Log("[\u2139] Çən göstəriciləri əvvəlki oxuma ilə tam eynidir. Ekranı doldurmamaq və bazanı (DB) təkrarlamamaq üçün əlavə edilmədi.", ConsoleColor.DarkGray);
                                        }

                                        if (isDataChanged)
                                        {
                                            try 
                                            {
                                                Console.WriteLine($"\n================ CİHAZ (ATG) MƏLUMATLARI ({DateTime.Now:HH:mm:ss}) ================");
                                            if (result.metadata != null)
                                            {
                                                Console.WriteLine($"   Datanın vaxtı: {result.metadata.timestamp} | Sorğu ID: {result.metadata.request_id}");
                                            }
                                            Console.WriteLine(new string('=', 67));

                                            var receivedTanks = result.data.Where(t => t != null).Select(t => t.tank_id).ToList();
                                            var missingTanks = new List<int>();
                                            for (int i = 1; i <= tankCount; i++)
                                            {
                                                if (!receivedTanks.Contains(i))
                                                    missingTanks.Add(i);
                                            }

                                            if (missingTanks.Count > 0)
                                            {
                                                Console.ForegroundColor = ConsoleColor.Yellow;
                                                Console.WriteLine($" [\u26A0] XƏBƏRDARLIQ: Cihazdan aşağıdakı çənlərin məlumatı heç gəlmədi: {string.Join(", ", missingTanks)}");
                                                Console.ResetColor();
                                                Console.WriteLine(new string('-', 67));
                                            }

                                            foreach (var tank in result.data.Where(t => t != null).OrderBy(t => t.tank_id))
                                            {
                                                if (tank.error != null)
                                                {
                                                    Console.ForegroundColor = ConsoleColor.Red;
                                                    Console.WriteLine($" [ÇƏN {tank.tank_id}] XƏTA GƏLDİ");
                                                    Console.WriteLine($" Səbəb: {tank.error.message} (Kod: {tank.error.code})");
                                                    Console.ResetColor();
                                                }
                                                else
                                                {
                                                    bool isMissingParams = tank.oil_level == null || tank.volume == null || tank.temperature == null;

                                                    Console.ForegroundColor = ConsoleColor.Cyan;
                                                    Console.WriteLine($" [ÇƏN {tank.tank_id}] MƏHSUL: {tank.product_code ?? "Bilinmir"} | STATUS: {tank.sensor_status?.ToUpper() ?? "BİLİNMİR"}");
                                                    Console.ResetColor();

                                                    if (isMissingParams)
                                                    {
                                                        Console.ForegroundColor = ConsoleColor.Yellow;
                                                        Console.WriteLine($"  [\u26A0] Diqqət: Çəndən gələn məlumatda bəzi fiziki dəyərlər (həcm, səviyyə və ya temperatur) yarımçıqdır!");
                                                        Console.ResetColor();
                                                    }

                                                    Console.WriteLine($"  ► Səviyyə:   Yanacaq: {tank.oil_level?.ToString() ?? "?"} mm | Su: {tank.water_level?.ToString() ?? "?"} mm | Boşluq (Ullage): {tank.Ullage?.ToString() ?? "?"} mm");
                                                    Console.WriteLine($"  ► Həcm:      Təmiz həcm (Tc): {tank.tc_volume?.ToString() ?? "?"} L | Ümumi həcm: {tank.volume?.ToString() ?? "?"} L | Su həcmi: {tank.water_volume?.ToString() ?? "?"} L");
                                                    Console.WriteLine($"  ► Əlavə:     Tutum (Capacity): {tank.capacity?.ToString() ?? "?"} L | Temperatur: {tank.temperature?.ToString() ?? "?"} °C");
                                                }
                                                Console.WriteLine(new string('-', 67));
                                            }
                                        }
                                            catch(Exception consoleEx)
                                            {
                                                // Konsol çıxarışında hər hansı bug yaranarsa proqramın donmasının qarşısını almaq üçün təhlükəsizlik
                                                Log($"[\u26A0] Konsola məlumat yazılarkən xəta: {consoleEx.Message}", ConsoleColor.Yellow);
                                            }
                                        }
                                    }
                                }
                            }

                            connectionSuccess = true;
                            break; 
                        }
                        catch (Exception ex)
                        {
                            Log($"Xəta (Cəhd {attempt}/{maxRetries}): {ex.Message}", ConsoleColor.Red);

                            if (attempt < maxRetries && !_cts.Token.IsCancellationRequested)
                            {
                                Log("2 saniyə sonra yenidən cəhd edilir...", ConsoleColor.DarkYellow);
                                try { await Task.Delay(2000, _cts.Token); } catch (TaskCanceledException) { break; }
                            }
                        }
                    } // for loop sonu
                    
                    if (_cts.Token.IsCancellationRequested) break;

                    if (!connectionSuccess)
                    {
                        Log($"\n[BAĞLANTI İTKİSİ] Bütün {maxRetries} cəhdin hamısı uğursuz oldu.", ConsoleColor.DarkRed);

                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.Write("Zəhmət olmasa yeni IP ünvanı daxil edin (Boş buraxıb ENTER bassanız 30 saniyə gözləyəcək): ");
                        Console.ResetColor();
                        
                        string newIp = Console.ReadLine()?.Trim();
                        if (!string.IsNullOrEmpty(newIp))
                        {
                            deviceIp = newIp;
                            UpdateIpInDatabase(deviceIp);
                        }
                        else
                        {
                            Log("Yeni ip ünvanı daxil edilmədi. 30 saniyə sonra təkrar yoxlanılacaq...", ConsoleColor.Cyan);
                            try { await Task.Delay(30000, _cts.Token); } catch (TaskCanceledException) { break; }
                        }
                    }
                    else
                    {
                        Log("\nMəlumat oxuma tamamlandı. Yenidən oxumaq üçün 30 saniyə gözlənilir... (Dayandırmaq üçün Ctrl+C basın)", ConsoleColor.Green);
                        try { await Task.Delay(30000, _cts.Token); } catch (TaskCanceledException) { break; }
                    }
                }
            }
            catch (TaskCanceledException)
            {
                Log("Proqram istifadəçi tərəfindən (Ctrl+C) dayandırıldı.", ConsoleColor.Yellow);
            }
            catch (Exception ex)
            {
                Log($"Kritik xəta: {ex.Message}", ConsoleColor.DarkRed);
            }
            finally
            {
                Log("Proqramdan çıxılır...", ConsoleColor.White);
            }
        }
    }
}