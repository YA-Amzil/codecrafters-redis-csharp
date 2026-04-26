using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Globalization;
using System.Security.Cryptography;

class RedisServer
{
    static Dictionary<string, (string value, DateTime? expiry)> store = new Dictionary<string, (string, DateTime?)>();
    static Dictionary<string, List<string>> listStore = new Dictionary<string, List<string>>();
    static Dictionary<string, List<StreamEntry>> streamStore = new Dictionary<string, List<StreamEntry>>();
    static bool isReplica = false;
    static string replicaHost = string.Empty;
    static int replicaPort = 0;
    static string configDir = string.Empty;
    static string configDbfilename = string.Empty;
    static TcpClient? masterClient;
    static NetworkStream? masterStream;
    static readonly string masterReplId = GenerateReplicationId();
    static readonly long masterReplOffset = 0;
    static readonly byte[] EmptyRdbBytes = LoadEmptyRdbBytes();
    static readonly object replicaLock = new object();
    static readonly object listLock = new object();
    static readonly object streamLock = new object();
    static readonly List<Socket> replicaSockets = new List<Socket>();
    static readonly Dictionary<Socket, long> replicaAckOffsets = new Dictionary<Socket, long>();
    static long masterReplicationOffset = 0;
    static long replicaProcessedOffset = 0;
    static Dictionary<string, Queue<BlockedPopRequest>> blockedPopWaiters = new Dictionary<string, Queue<BlockedPopRequest>>();
    static Dictionary<string, List<BlockedXReadRequest>> blockedXReadWaiters = new Dictionary<string, List<BlockedXReadRequest>>();

    class StreamEntry
    {
        public string Id = string.Empty;
        public List<KeyValuePair<string, string>> Fields = new List<KeyValuePair<string, string>>();
    }

    class BlockedPopRequest
    {
        public AutoResetEvent Signal = new AutoResetEvent(false);
        public string? Value;
        public bool IsCompleted;
    }

    class BlockedXReadRequest
    {
        public AutoResetEvent Signal = new AutoResetEvent(false);
        public string StartIdExclusive;
        public List<StreamEntry>? NewEntries;
        public bool IsCompleted;
        public BlockedXReadRequest(string startId) { StartIdExclusive = startId; }
    }

    // ── Per-connection transaction state ─────────────────────────────────────
    // Each client gets its own instance created in HandleClient.
    // This means transactions are fully isolated: Client A's MULTI never
    // affects Client B.
    class ClientState
    {
        public bool InTransaction = false;
        public List<List<string>> QueuedCommands = new List<List<string>>();
    }

    static string GenerateReplicationId()
    {
        byte[] bytes = RandomNumberGenerator.GetBytes(20);
        return Convert.ToHexString(bytes);
    }

    static void LoadRdbFile()
    {
        if (string.IsNullOrEmpty(configDir) || string.IsNullOrEmpty(configDbfilename))
            return;

        string rdbPath = Path.Combine(configDir, configDbfilename);
        if (!File.Exists(rdbPath))
            return;

        try
        {
            byte[] rdbBytes = File.ReadAllBytes(rdbPath);
            ParseRdbFile(rdbBytes);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading RDB file: {ex.Message}");
        }
    }

    // ── ParseRdbFile ──────────────────────────────────────────────────────────
    // Key fixes vs original:
    //   1. Added 0xFB handler (hash table size info) — was missing entirely,
    //      causing all subsequent key-value reads to be offset by 2 bytes.
    //   2. 0xFC / 0xFD now use BitConverter (little-endian) and pass the
    //      parsed expiry DateTime to TryReadKeyValue instead of a bool flag.
    static void ParseRdbFile(byte[] b)
    {
        if (b.Length < 9) return;
        if (b[0] != 'R' || b[1] != 'E' || b[2] != 'D' || b[3] != 'I' || b[4] != 'S') return;

        int pos = 9; // skip "REDIS" + 4-digit version

        while (pos < b.Length)
        {
            byte op = b[pos++];

            if (op == 0xFF) break; // End of file

            if (op == 0xFA) // Auxiliary field: skip name + value
            {
                if (!TryReadString(b, ref pos, out _)) return;
                if (!TryReadString(b, ref pos, out _)) return;
                continue;
            }

            if (op == 0xFE) // Select DB: skip size-encoded DB index
            {
                if (!TryReadLength(b, ref pos, out _)) return;
                continue;
            }

            // FIX 1: 0xFB was missing. It introduces two size-encoded values
            // (key hash table size, expiry hash table size). Without this handler
            // the parser treated those bytes as value-type opcodes and lost sync.
            if (op == 0xFB)
            {
                if (!TryReadLength(b, ref pos, out _)) return; // key table size
                if (!TryReadLength(b, ref pos, out _)) return; // expiry table size
                continue;
            }

            // FIX 2: expiry timestamps are little-endian; use BitConverter.
            if (op == 0xFC) // Expiry in milliseconds (8-byte uint64, little-endian)
            {
                if (pos + 8 > b.Length) return;
                ulong ms = BitConverter.ToUInt64(b, pos); pos += 8;
                if (pos >= b.Length) return;
                byte vt = b[pos++];
                DateTime exp = DateTimeOffset.FromUnixTimeMilliseconds((long)ms).UtcDateTime;
                TryReadKeyValue(b, ref pos, vt, exp);
                continue;
            }

            if (op == 0xFD) // Expiry in seconds (4-byte uint32, little-endian)
            {
                if (pos + 4 > b.Length) return;
                uint s = BitConverter.ToUInt32(b, pos); pos += 4;
                if (pos >= b.Length) return;
                byte vt = b[pos++];
                DateTime exp = DateTimeOffset.FromUnixTimeSeconds(s).UtcDateTime;
                TryReadKeyValue(b, ref pos, vt, exp);
                continue;
            }

            // Regular key-value: op byte IS the value type
            TryReadKeyValue(b, ref pos, op, null);
        }
    }

    static bool TryReadKeyValue(byte[] b, ref int pos, byte valueType, DateTime? expiry)
    {
        if (!TryReadString(b, ref pos, out string key)) return false;

        if (valueType == 0) // String
        {
            if (!TryReadString(b, ref pos, out string value)) return false;
            store[key] = (value, expiry);
        }
        else if (valueType == 1) // List
        {
            if (!TryReadLength(b, ref pos, out int len)) return false;
            List<string> list = new List<string>();
            for (int i = 0; i < len; i++)
            {
                if (!TryReadString(b, ref pos, out string elem)) return false;
                list.Add(elem);
            }
            lock (listLock) { listStore[key] = list; }
        }
        return true;
    }

    // ── TryReadString ─────────────────────────────────────────────────────────
    // FIX 3: Integer-encoded strings use prefix 0xC0/0xC1/0xC2 (top 2 bits = 11,
    // lower 6 bits = sub-type 0/1/2). The old code used 0xFF/0xFE/0xFD which are
    // RDB file opcodes — completely wrong for string encoding.
    // Integers are stored in little-endian order.
    static bool TryReadString(byte[] b, ref int pos, out string result)
    {
        result = string.Empty;
        if (pos >= b.Length) return false;

        byte lb = b[pos++];
        int top = lb & 0xC0;

        if (top == 0x00) // 6-bit length
        {
            int len = lb & 0x3F;
            if (pos + len > b.Length) return false;
            result = Encoding.UTF8.GetString(b, pos, len); pos += len;
            return true;
        }

        if (top == 0x40) // 14-bit length (big-endian)
        {
            if (pos >= b.Length) return false;
            int len = ((lb & 0x3F) << 8) | b[pos++];
            if (pos + len > b.Length) return false;
            result = Encoding.UTF8.GetString(b, pos, len); pos += len;
            return true;
        }

        if (top == 0x80) // 32-bit length (big-endian, skip first byte)
        {
            if (pos + 4 > b.Length) return false;
            int len = (b[pos] << 24) | (b[pos+1] << 16) | (b[pos+2] << 8) | b[pos+3]; pos += 4;
            if (pos + len > b.Length) return false;
            result = Encoding.UTF8.GetString(b, pos, len); pos += len;
            return true;
        }

        // top == 0xC0: special integer encoding
        int enc = lb & 0x3F;

        if (enc == 0) // 0xC0: 8-bit unsigned integer
        {
            if (pos >= b.Length) return false;
            result = b[pos++].ToString();
            return true;
        }
        if (enc == 1) // 0xC1: 16-bit little-endian integer
        {
            if (pos + 2 > b.Length) return false;
            result = (b[pos] | (b[pos+1] << 8)).ToString(); pos += 2;
            return true;
        }
        if (enc == 2) // 0xC2: 32-bit little-endian integer
        {
            if (pos + 4 > b.Length) return false;
            result = (b[pos] | (b[pos+1] << 8) | (b[pos+2] << 16) | (b[pos+3] << 24)).ToString(); pos += 4;
            return true;
        }

        return false; // 0xC3 = LZF compressed, not needed
    }

    static bool TryReadLength(byte[] b, ref int pos, out int length)
    {
        length = 0;
        if (pos >= b.Length) return false;
        byte lb = b[pos++];
        int top = lb & 0xC0;
        if (top == 0x00) { length = lb & 0x3F; return true; }
        if (top == 0x40) { if (pos >= b.Length) return false; length = ((lb & 0x3F) << 8) | b[pos++]; return true; }
        if (top == 0x80) { if (pos + 4 > b.Length) return false; length = (b[pos] << 24) | (b[pos+1] << 16) | (b[pos+2] << 8) | b[pos+3]; pos += 4; return true; }
        return false;
    }

    static byte[] LoadEmptyRdbBytes()
    {
        foreach (string path in new[] { Path.Combine(AppContext.BaseDirectory, "empty_rdb.base64"), Path.Combine(AppContext.BaseDirectory, "src", "empty_rdb.base64") })
            if (File.Exists(path)) return Convert.FromBase64String(File.ReadAllText(path).Trim());
        return Convert.FromBase64String("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
    }

    static string SendAndReceive(NetworkStream stream, string msg)
    {
        byte[] d = Encoding.UTF8.GetBytes(msg); stream.Write(d, 0, d.Length); stream.Flush();
        byte[] buf = new byte[512]; int n = stream.Read(buf, 0, buf.Length);
        return Encoding.UTF8.GetString(buf, 0, n);
    }

    static void SendOnly(NetworkStream stream, string msg)
    { byte[] d = Encoding.UTF8.GetBytes(msg); stream.Write(d, 0, d.Length); stream.Flush(); }

    static string ReadRespLine(NetworkStream stream)
    {
        List<byte> bytes = new List<byte>();
        while (true)
        {
            int b = stream.ReadByte();
            if (b < 0) throw new IOException("Connection closed");
            bytes.Add((byte)b);
            int c = bytes.Count;
            if (c >= 2 && bytes[c-2] == '\r' && bytes[c-1] == '\n') break;
        }
        return Encoding.UTF8.GetString(bytes.ToArray(), 0, bytes.Count - 2);
    }

    static void ReadExact(NetworkStream stream, byte[] buf, int len)
    {
        int read = 0;
        while (read < len) { int n = stream.Read(buf, read, len - read); if (n <= 0) throw new IOException("Connection closed"); read += n; }
    }

    static void ReceiveFullResyncAndRdb(NetworkStream stream)
    {
        string line = ReadRespLine(stream);
        if (!line.StartsWith("+FULLRESYNC")) throw new InvalidOperationException($"Unexpected: {line}");
        string rdbHeader = ReadRespLine(stream);
        if (!rdbHeader.StartsWith("$")) throw new InvalidOperationException($"Unexpected RDB header: {rdbHeader}");
        if (!int.TryParse(rdbHeader.Substring(1), out int len) || len < 0) throw new InvalidOperationException("Invalid RDB length");
        byte[] rdb = new byte[len]; ReadExact(stream, rdb, len);
    }

    static int FindCrlf(List<byte> buf, int start)
    {
        for (int i = start; i + 1 < buf.Count; i++) if (buf[i] == '\r' && buf[i+1] == '\n') return i;
        return -1;
    }

    static bool TryParseRespArray(List<byte> buf, out List<string> args, out int consumed)
    {
        args = new List<string>(); consumed = 0;
        if (buf.Count == 0 || buf[0] != '*') return false;
        int le = FindCrlf(buf, 0); if (le < 0) return false;
        if (!int.TryParse(Encoding.UTF8.GetString(buf.GetRange(1, le-1).ToArray()), out int count) || count < 0) throw new InvalidOperationException("Invalid array length");
        int idx = le + 2;
        for (int i = 0; i < count; i++)
        {
            if (idx >= buf.Count || buf[idx] != '$') return false;
            int lle = FindCrlf(buf, idx); if (lle < 0) return false;
            if (!int.TryParse(Encoding.UTF8.GetString(buf.GetRange(idx+1, lle-(idx+1)).ToArray()), out int len) || len < 0) throw new InvalidOperationException("Invalid bulk length");
            idx = lle + 2;
            if (buf.Count < idx + len + 2) return false;
            args.Add(Encoding.UTF8.GetString(buf.GetRange(idx, len).ToArray()));
            idx += len;
            if (buf[idx] != '\r' || buf[idx+1] != '\n') throw new InvalidOperationException("Invalid RESP terminator");
            idx += 2;
        }
        consumed = idx; return true;
    }

    static void ProcessMasterReplicationStream(NetworkStream stream)
    {
        byte[] rb = new byte[4096]; List<byte> pending = new List<byte>();
        while (true)
        {
            int n = stream.Read(rb, 0, rb.Length); if (n <= 0) break;
            for (int i = 0; i < n; i++) pending.Add(rb[i]);
            while (TryParseRespArray(pending, out List<string> cmdArgs, out int consumed))
            {
                pending.RemoveRange(0, consumed);
                if (cmdArgs.Count == 0) continue;
                if (cmdArgs.Count == 3 && cmdArgs[0].Equals("REPLCONF", StringComparison.OrdinalIgnoreCase) && cmdArgs[1].Equals("GETACK", StringComparison.OrdinalIgnoreCase) && cmdArgs[2] == "*")
                { SendOnly(stream, BuildReplConfAck(replicaProcessedOffset)); replicaProcessedOffset += consumed; continue; }
                DispatchCommand(cmdArgs[0].ToUpperInvariant(), cmdArgs);
                replicaProcessedOffset += consumed;
            }
        }
    }

    static void SendAll(Socket socket, byte[] data) { int sent = 0; while (sent < data.Length) sent += socket.Send(data, sent, data.Length - sent, SocketFlags.None); }
    static void RegisterReplica(Socket s) { lock (replicaLock) { if (!replicaSockets.Contains(s)) replicaSockets.Add(s); replicaAckOffsets[s] = 0; } }
    static void UnregisterReplica(Socket s) { lock (replicaLock) { replicaSockets.Remove(s); replicaAckOffsets.Remove(s); } }

    static int CountReplicasAtOrAboveOffset(long offset)
    {
        lock (replicaLock) { int c = 0; foreach (Socket r in replicaSockets) if (replicaAckOffsets.TryGetValue(r, out long a) && a >= offset) c++; return c; }
    }

    static void SendGetAckToReplicas()
    {
        byte[] payload = Encoding.UTF8.GetBytes(BuildRespArray(new List<string> { "REPLCONF", "GETACK", "*" }));
        lock (replicaLock) { for (int i = replicaSockets.Count - 1; i >= 0; i--) { try { SendAll(replicaSockets[i], payload); } catch { replicaSockets.RemoveAt(i); } } }
    }

    static string BuildRespArray(List<string> args) { StringBuilder sb = new StringBuilder(); sb.Append($"*{args.Count}\r\n"); foreach (string a in args) sb.Append($"${a.Length}\r\n{a}\r\n"); return sb.ToString(); }
    static string BuildReplConfAck(long offset) => BuildRespArray(new List<string> { "REPLCONF", "ACK", offset.ToString() });
    static bool ShouldPropagate(string cmd, string resp) => cmd is "SET" or "INCR" or "RPUSH" or "LPUSH" or "LPOP" or "XADD" && !resp.StartsWith("-") && resp != "$-1\r\n" && resp != "*0\r\n";

    static void PropagateWriteCommand(List<string> args)
    {
        byte[] payload = Encoding.UTF8.GetBytes(BuildRespArray(args));
        lock (replicaLock) { masterReplicationOffset += payload.Length; for (int i = replicaSockets.Count - 1; i >= 0; i--) { try { SendAll(replicaSockets[i], payload); } catch { replicaSockets.RemoveAt(i); } } }
    }

    static void ConnectToMaster(string host, int masterPort, int listeningPort)
    {
        masterClient = new TcpClient(); masterClient.Connect(host, masterPort); masterStream = masterClient.GetStream();
        SendAndReceive(masterStream, "*1\r\n$4\r\nPING\r\n");
        string portStr = listeningPort.ToString();
        SendAndReceive(masterStream, $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${portStr.Length}\r\n{portStr}\r\n");
        SendAndReceive(masterStream, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        SendOnly(masterStream, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        ReceiveFullResyncAndRdb(masterStream);
        Thread t = new Thread(() => ProcessMasterReplicationStream(masterStream)); t.IsBackground = true; t.Start();
    }

    static void Main(string[] args)
    {
        int port = 6379;
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" && i + 1 < args.Length) { int.TryParse(args[++i], out port); }
            else if (args[i] == "--replicaof" && i + 1 < args.Length)
            {
                isReplica = true;
                string[] p = args[++i].Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (p.Length >= 2 && int.TryParse(p[1], out int rp)) { replicaHost = p[0]; replicaPort = rp; }
            }
            else if (args[i] == "--dir" && i + 1 < args.Length) { configDir = args[++i]; }
            else if (args[i] == "--dbfilename" && i + 1 < args.Length) { configDbfilename = args[++i]; }
        }

        LoadRdbFile();
        if (isReplica) ConnectToMaster(replicaHost, replicaPort, port);

        TcpListener server = new TcpListener(IPAddress.Any, port);
        server.Start();
        Console.WriteLine($"Redis server started on port {port}...");

        while (true)
        {
            try { Socket client = server.AcceptSocket(); Console.WriteLine("Client connected."); new Thread(() => HandleClient(client)).Start(); }
            catch (Exception ex) { Console.WriteLine($"Server error: {ex.Message}"); break; }
        }
        server.Stop();
    }

    static void HandleClient(Socket client)
    {
        byte[] buffer = new byte[1024];
        ClientState state = new ClientState();

        while (true)
        {
            try
            {
                int n = client.Receive(buffer);
                if (n == 0) { Console.WriteLine("Client disconnected."); break; }

                string input = Encoding.UTF8.GetString(buffer, 0, n);
                List<string> args = ParseRESP(input);
                if (args.Count == 0) continue;

                string command = args[0].ToUpperInvariant();
                string response;

                if (command == "REPLCONF" && args.Count >= 3 && args[1].Equals("ACK", StringComparison.OrdinalIgnoreCase) && long.TryParse(args[2], out long ackOff))
                { lock (replicaLock) { if (replicaSockets.Contains(client)) replicaAckOffsets[client] = ackOff; } continue; }

                if (command == "MULTI")
                {
                    if (state.InTransaction) response = "-ERR MULTI calls can not be nested\r\n";
                    else { state.InTransaction = true; state.QueuedCommands.Clear(); response = "+OK\r\n"; }
                }
                else if (command == "EXEC") { response = HandleExec(state); }
                else if (command == "DISCARD")
                {
                    if (!state.InTransaction) response = "-ERR DISCARD without MULTI\r\n";
                    else { state.InTransaction = false; state.QueuedCommands.Clear(); response = "+OK\r\n"; }
                }
                else if (state.InTransaction) { state.QueuedCommands.Add(args); response = "+QUEUED\r\n"; }
                else
                {
                    if (command == "PSYNC")
                    {
                        response = HandlePSync(args);
                        SendAll(client, Encoding.UTF8.GetBytes(response));
                        if (response.StartsWith("+FULLRESYNC"))
                        {
                            SendAll(client, Encoding.ASCII.GetBytes($"${EmptyRdbBytes.Length}\r\n"));
                            SendAll(client, EmptyRdbBytes);
                            RegisterReplica(client);
                        }
                        continue;
                    }
                    response = DispatchCommand(command, args);
                }

                client.Send(Encoding.UTF8.GetBytes(response));
                if (ShouldPropagate(command, response)) PropagateWriteCommand(args);
            }
            catch (SocketException) { Console.WriteLine("Client connection lost."); break; }
            catch (Exception ex) { Console.WriteLine($"Client error: {ex.Message}"); break; }
        }
        UnregisterReplica(client);
        client.Close();
    }

    static string HandleExec(ClientState state)
    {
        if (!state.InTransaction) return "-ERR EXEC without MULTI\r\n";
        List<string> results = new List<string>();
        foreach (List<string> q in state.QueuedCommands) results.Add(DispatchCommand(q[0].ToUpperInvariant(), q));
        state.InTransaction = false; state.QueuedCommands.Clear();
        StringBuilder sb = new StringBuilder(); sb.Append($"*{results.Count}\r\n"); foreach (string r in results) sb.Append(r);
        return sb.ToString();
    }

    static string DispatchCommand(string command, List<string> args)
    {
        switch (command)
        {
            case "PING":     return "+PONG\r\n";
            case "ECHO":     return args.Count > 1 ? $"${args[1].Length}\r\n{args[1]}\r\n" : "$-1\r\n";
            case "REPLCONF": return "+OK\r\n";
            case "PSYNC":    return HandlePSync(args);
            case "WAIT":     return HandleWait(args);
            case "CONFIG":   return HandleConfig(args);
            case "KEYS":     return HandleKeys(args);
            case "SET":      return HandleSet(args);
            case "GET":      return HandleGet(args);
            case "INFO":     return HandleInfo(args);
            case "INCR":     return HandleIncr(args);
            case "RPUSH":    return HandleRPush(args);
            case "LPUSH":    return HandleLPush(args);
            case "LRANGE":   return HandleLRange(args);
            case "LLEN":     return HandleLLen(args);
            case "LPOP":     return HandleLPop(args);
            case "BLPOP":    return HandleBLPop(args);
            case "TYPE":     return HandleType(args);
            case "XADD":     return HandleXAdd(args);
            case "XRANGE":   return HandleXRange(args);
            case "XREAD":    return HandleXRead(args);
            default:         return "-ERR unknown command\r\n";
        }
    }

    static string HandleSet(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'SET'\r\n";
        string key = args[1], value = args[2]; DateTime? expiry = null;
        for (int i = 3; i < args.Count - 1; i++)
            if (args[i].Equals("PX", StringComparison.OrdinalIgnoreCase) && int.TryParse(args[i+1], out int ms)) { expiry = DateTime.UtcNow.AddMilliseconds(ms); i++; }
        store[key] = (value, expiry);
        return "+OK\r\n";
    }

    static string HandleGet(List<string> args)
    {
        if (args.Count < 2) return "-ERR wrong number of arguments for 'GET'\r\n";
        if (store.ContainsKey(args[1])) { var (v, ex) = store[args[1]]; if (ex.HasValue && DateTime.UtcNow > ex.Value) { store.Remove(args[1]); return "$-1\r\n"; } return $"${v.Length}\r\n{v}\r\n"; }
        return "$-1\r\n";
    }

    static string HandleInfo(List<string> args)
    {
        if (args.Count >= 2 && args[1].Equals("replication", StringComparison.OrdinalIgnoreCase))
        {
            string body = $"role:{(isReplica ? "slave" : "master")}\r\nmaster_replid:{masterReplId}\r\nmaster_repl_offset:{masterReplOffset}";
            return $"${body.Length}\r\n{body}\r\n";
        }
        return "$0\r\n\r\n";
    }

    static string HandleConfig(List<string> args)
    {
        if (args.Count >= 3 && args[1].Equals("GET", StringComparison.OrdinalIgnoreCase))
        {
            string p = args[2].ToLowerInvariant();
            if (p == "dir") return $"*2\r\n$3\r\ndir\r\n${configDir.Length}\r\n{configDir}\r\n";
            if (p == "dbfilename") return $"*2\r\n$10\r\ndbfilename\r\n${configDbfilename.Length}\r\n{configDbfilename}\r\n";
            return "*0\r\n";
        }
        return "-ERR Unknown subcommand\r\n";
    }

    static string HandleKeys(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'KEYS'\r\n";
        if (args[1] != "*") return "*0\r\n";
        List<string> keys = new List<string>();
        foreach (var kvp in store) { var (_, ex) = kvp.Value; if (!ex.HasValue || DateTime.UtcNow <= ex.Value) keys.Add(kvp.Key); }
        lock (listLock) foreach (string k in listStore.Keys) if (!keys.Contains(k)) keys.Add(k);
        lock (streamLock) foreach (string k in streamStore.Keys) if (!keys.Contains(k)) keys.Add(k);
        StringBuilder sb = new StringBuilder(); sb.Append($"*{keys.Count}\r\n"); foreach (string k in keys) sb.Append($"${k.Length}\r\n{k}\r\n");
        return sb.ToString();
    }

    static string HandlePSync(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'PSYNC'\r\n";
        return $"+FULLRESYNC {masterReplId} {masterReplOffset}\r\n";
    }

    static string HandleWait(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'WAIT'\r\n";
        if (!int.TryParse(args[1], out int req) || !int.TryParse(args[2], out int tms)) return "-ERR value is not an integer or out of range\r\n";
        if (req == 0) return ":0\r\n";
        long target; lock (replicaLock) { target = masterReplicationOffset; }
        int acked = CountReplicasAtOrAboveOffset(target);
        if (acked >= req) return $":{acked}\r\n";
        SendGetAckToReplicas();
        DateTime dl = DateTime.UtcNow.AddMilliseconds(tms);
        while (DateTime.UtcNow < dl) { acked = CountReplicasAtOrAboveOffset(target); if (acked >= req) break; Thread.Sleep(10); }
        return $":{CountReplicasAtOrAboveOffset(target)}\r\n";
    }

    static string HandleIncr(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'INCR'\r\n";
        string key = args[1];
        if (store.TryGetValue(key, out var entry))
        {
            if (entry.expiry.HasValue && DateTime.UtcNow > entry.expiry.Value) { store[key] = ("1", null); return ":1\r\n"; }
            if (!long.TryParse(entry.value, out long cur)) return "-ERR value is not an integer or out of range\r\n";
            store[key] = ((cur+1).ToString(), entry.expiry); return $":{cur+1}\r\n";
        }
        store[key] = ("1", null); return ":1\r\n";
    }

    static string HandleRPush(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'RPUSH'\r\n";
        int n; lock (listLock) { if (!listStore.ContainsKey(args[1])) listStore[args[1]] = new List<string>(); for (int i = 2; i < args.Count; i++) listStore[args[1]].Add(args[i]); n = listStore[args[1]].Count; ServeBlockedClientsFromList(args[1]); }
        return $":{n}\r\n";
    }

    static string HandleLPush(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'LPUSH'\r\n";
        int n; lock (listLock) { if (!listStore.ContainsKey(args[1])) listStore[args[1]] = new List<string>(); for (int i = 2; i < args.Count; i++) listStore[args[1]].Insert(0, args[i]); n = listStore[args[1]].Count; ServeBlockedClientsFromList(args[1]); }
        return $":{n}\r\n";
    }

    static string HandleLRange(List<string> args)
    {
        if (args.Count != 4) return "-ERR wrong number of arguments for 'LRANGE'\r\n";
        if (!int.TryParse(args[2], out int start) || !int.TryParse(args[3], out int stop)) return "-ERR value is not an integer or out of range\r\n";
        List<string> snap; lock (listLock) { if (!listStore.TryGetValue(args[1], out List<string>? list) || list.Count == 0) return "*0\r\n"; snap = new List<string>(list); }
        int cnt = snap.Count;
        if (start < 0) start = cnt + start; if (stop < 0) stop = cnt + stop; if (start < 0) start = 0; if (stop < 0) stop = 0;
        if (start >= cnt || start > stop) return "*0\r\n";
        int end = Math.Min(stop, cnt-1); StringBuilder sb = new StringBuilder(); sb.Append($"*{end-start+1}\r\n");
        for (int i = start; i <= end; i++) sb.Append($"${snap[i].Length}\r\n{snap[i]}\r\n");
        return sb.ToString();
    }

    static string HandleLLen(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'LLEN'\r\n";
        int len; lock (listLock) { len = listStore.TryGetValue(args[1], out List<string>? l) ? l.Count : 0; }
        return $":{len}\r\n";
    }

    static string HandleLPop(List<string> args)
    {
        if (args.Count < 2 || args.Count > 3) return "-ERR wrong number of arguments for 'LPOP'\r\n";
        string key = args[1];
        if (args.Count == 2) { lock (listLock) { if (!listStore.TryGetValue(key, out List<string>? sl) || sl.Count == 0) return "$-1\r\n"; string v = sl[0]; sl.RemoveAt(0); return $"${v.Length}\r\n{v}\r\n"; } }
        if (!int.TryParse(args[2], out int cnt)) return "-ERR value is not an integer or out of range\r\n";
        lock (listLock)
        {
            if (cnt <= 0 || !listStore.TryGetValue(key, out List<string>? list) || list.Count == 0) return "*0\r\n";
            int n = Math.Min(cnt, list.Count); StringBuilder sb = new StringBuilder(); sb.Append($"*{n}\r\n");
            for (int i = 0; i < n; i++) { string v = list[0]; list.RemoveAt(0); sb.Append($"${v.Length}\r\n{v}\r\n"); }
            return sb.ToString();
        }
    }

    static string HandleBLPop(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'BLPOP'\r\n";
        string key = args[1];
        if (!double.TryParse(args[2], NumberStyles.Float, CultureInfo.InvariantCulture, out double timeout) || timeout < 0) return "-ERR timeout is not a float or out of range\r\n";
        BlockedPopRequest req = new BlockedPopRequest();
        lock (listLock)
        {
            if (listStore.TryGetValue(key, out List<string>? list) && list.Count > 0) { string v = list[0]; list.RemoveAt(0); return EncodeKeyValueArray(key, v); }
            if (!blockedPopWaiters.ContainsKey(key)) blockedPopWaiters[key] = new Queue<BlockedPopRequest>();
            blockedPopWaiters[key].Enqueue(req);
        }
        bool sig = timeout == 0 ? req.Signal.WaitOne() : req.Signal.WaitOne(TimeSpan.FromSeconds(timeout));
        if (sig) return EncodeKeyValueArray(key, req.Value ?? string.Empty);
        lock (listLock) { if (req.IsCompleted) return EncodeKeyValueArray(key, req.Value ?? string.Empty); RemoveBlockedRequest(key, req); }
        return "*-1\r\n";
    }

    static string HandleType(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'TYPE'\r\n";
        string key = args[1];
        if (store.ContainsKey(key)) { var (_, ex) = store[key]; if (ex.HasValue && DateTime.UtcNow > ex.Value) { store.Remove(key); return "+none\r\n"; } return "+string\r\n"; }
        lock (listLock) { if (listStore.ContainsKey(key)) return "+list\r\n"; }
        lock (streamLock) { if (streamStore.ContainsKey(key)) return "+stream\r\n"; }
        return "+none\r\n";
    }

    static string HandleXAdd(List<string> args)
    {
        if (args.Count < 5 || ((args.Count - 3) % 2 != 0)) return "-ERR wrong number of arguments for 'XADD'\r\n";
        string key = args[1], id = args[2], finalId = id;
        lock (listLock) { if (listStore.ContainsKey(key)) return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"; }
        if (store.ContainsKey(key)) return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        if (id == "*")
        {
            lock (streamLock)
            {
                if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries)) { entries = new List<StreamEntry>(); streamStore[key] = entries; }
                ulong ms = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), seq = 0;
                if (entries.Count > 0) { string[] lp = entries[^1].Id.Split('-'); ulong lms = ulong.Parse(lp[0]), lseq = ulong.Parse(lp[1]); if (ms <= lms) { ms = lms; seq = lseq+1; } }
                finalId = $"{ms}-{seq}"; StreamEntry e = new StreamEntry { Id = finalId };
                for (int i = 3; i < args.Count; i += 2) e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i+1]));
                entries.Add(e); ServeBlockedXReadClients(key, entries);
            }
            return $"${finalId.Length}\r\n{finalId}\r\n";
        }
        if (id.EndsWith("-*"))
        {
            if (!ulong.TryParse(id.Substring(0, id.Length-2), out ulong ms)) return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            lock (streamLock)
            {
                if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries)) { entries = new List<StreamEntry>(); streamStore[key] = entries; }
                ulong seq = 0;
                if (entries.Count > 0) { string[] lp = entries[^1].Id.Split('-'); ulong lms = ulong.Parse(lp[0]), lseq = ulong.Parse(lp[1]); if (ms < lms) return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"; seq = ms == lms ? lseq+1 : (ulong)(ms == 0 ? 1 : 0); }
                else seq = (ulong)(ms == 0 ? 1 : 0);
                finalId = $"{ms}-{seq}"; StreamEntry e = new StreamEntry { Id = finalId };
                for (int i = 3; i < args.Count; i += 2) e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i+1]));
                entries.Add(e); ServeBlockedXReadClients(key, entries);
            }
            return $"${finalId.Length}\r\n{finalId}\r\n";
        }
        if (!TryParseExplicitStreamId(id, out ulong ems, out ulong eseq)) return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        if (ems == 0 && eseq == 0) return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        lock (streamLock)
        {
            if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries)) { entries = new List<StreamEntry>(); streamStore[key] = entries; }
            if (entries.Count > 0 && CompareStreamIds(finalId, entries[^1].Id) <= 0) return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            StreamEntry e = new StreamEntry { Id = finalId };
            for (int i = 3; i < args.Count; i += 2) e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i+1]));
            entries.Add(e); ServeBlockedXReadClients(key, entries);
        }
        return $"${finalId.Length}\r\n{finalId}\r\n";
    }

    static string HandleXRange(List<string> args)
    {
        if (args.Count != 4) return "-ERR wrong number of arguments for 'XRANGE'\r\n";
        if (!TryParseStreamRangeBound(args[2], true, out string startId) || !TryParseStreamRangeBound(args[3], false, out string endId)) return "-ERR Invalid stream ID specified as range\r\n";
        lock (streamLock)
        {
            if (!streamStore.TryGetValue(args[1], out List<StreamEntry>? entries) || entries.Count == 0) return "*0\r\n";
            List<StreamEntry> match = entries.FindAll(e => CompareStreamIds(e.Id, startId) >= 0 && CompareStreamIds(e.Id, endId) <= 0);
            if (match.Count == 0) return "*0\r\n";
            StringBuilder sb = new StringBuilder(); sb.Append($"*{match.Count}\r\n");
            foreach (StreamEntry e in match) { sb.Append("*2\r\n"); sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n"); sb.Append($"*{e.Fields.Count * 2}\r\n"); foreach (var f in e.Fields) { sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n"); sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n"); } }
            return sb.ToString();
        }
    }

    static string HandleXRead(List<string> args)
    {
        if (args.Count < 4) return "-ERR wrong number of arguments for 'XREAD'\r\n";
        bool isBlocking = false; int timeoutMs = 0; int si = 1;
        if (args[1].Equals("BLOCK", StringComparison.OrdinalIgnoreCase)) { if (args.Count < 5 || !int.TryParse(args[2], out timeoutMs) || timeoutMs < 0) return "-ERR timeout is not an integer or out of range\r\n"; isBlocking = true; si = 3; }
        if (!args[si].Equals("STREAMS", StringComparison.OrdinalIgnoreCase)) return "-ERR syntax error\r\n";
        int tac = args.Count - (si+1); if (tac % 2 != 0) return "-ERR syntax error\r\n";
        int sc = tac / 2; if (sc == 0) return "-ERR wrong number of arguments for 'XREAD'\r\n";
        List<string> keys = new List<string>(sc), sids = new List<string>(sc), rawIds = new List<string>(sc);
        for (int i = 0; i < sc; i++) keys.Add(args[si+1+i]);
        for (int i = 0; i < sc; i++) rawIds.Add(args[si+1+sc+i]);
        bool hasDollar = rawIds.Exists(id => id == "$");
        if (!hasDollar) { for (int i = 0; i < sc; i++) { if (!TryParseXReadId(rawIds[i], out string sid)) return "-ERR Invalid stream ID specified as stream command argument\r\n"; sids.Add(sid); } string imm = BuildXReadResponse(keys, sids, sc); if (imm != "*0\r\n") return imm; }
        if (!isBlocking) return "*0\r\n";
        string bk = keys[0]; BlockedXReadRequest req;
        lock (streamLock)
        {
            if (hasDollar) { sids.Clear(); for (int i = 0; i < sc; i++) { if (rawIds[i] == "$") { string lid = "0-0"; if (streamStore.TryGetValue(keys[i], out List<StreamEntry>? ex) && ex.Count > 0) lid = ex[^1].Id; sids.Add(lid); } else { if (!TryParseXReadId(rawIds[i], out string p2)) return "-ERR Invalid stream ID specified as stream command argument\r\n"; sids.Add(p2); } } }
            else { string rc = BuildXReadResponse(keys, sids, sc); if (rc != "*0\r\n") return rc; }
            req = new BlockedXReadRequest(sids[0]);
            if (!blockedXReadWaiters.ContainsKey(bk)) blockedXReadWaiters[bk] = new List<BlockedXReadRequest>();
            blockedXReadWaiters[bk].Add(req);
        }
        bool sig = timeoutMs == 0 ? req.Signal.WaitOne() : req.Signal.WaitOne(timeoutMs);
        if (!sig) { lock (streamLock) { if (!req.IsCompleted && blockedXReadWaiters.TryGetValue(bk, out List<BlockedXReadRequest>? wl)) { wl.Remove(req); if (wl.Count == 0) blockedXReadWaiters.Remove(bk); } } if (!req.IsCompleted) return "*-1\r\n"; }
        if (req.NewEntries == null || req.NewEntries.Count == 0) return "*-1\r\n";
        return BuildXReadResponseFromEntries(bk, req.NewEntries);
    }

    static string BuildXReadResponse(List<string> keys, List<string> startIds, int sc)
    {
        lock (streamLock)
        {
            List<(string, List<StreamEntry>)> res = new List<(string, List<StreamEntry>)>();
            for (int i = 0; i < sc; i++) { if (!streamStore.TryGetValue(keys[i], out List<StreamEntry>? se) || se.Count == 0) continue; List<StreamEntry> m = se.FindAll(e => CompareStreamIds(e.Id, startIds[i]) > 0); if (m.Count > 0) res.Add((keys[i], m)); }
            if (res.Count == 0) return "*0\r\n";
            StringBuilder sb = new StringBuilder(); sb.Append($"*{res.Count}\r\n");
            foreach ((string k, List<StreamEntry> ents) in res) { sb.Append("*2\r\n"); sb.Append($"${k.Length}\r\n{k}\r\n"); sb.Append($"*{ents.Count}\r\n"); foreach (StreamEntry e in ents) { sb.Append("*2\r\n"); sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n"); sb.Append($"*{e.Fields.Count * 2}\r\n"); foreach (var f in e.Fields) { sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n"); sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n"); } } }
            return sb.ToString();
        }
    }

    static string BuildXReadResponseFromEntries(string key, List<StreamEntry> entries)
    {
        StringBuilder sb = new StringBuilder(); sb.Append("*1\r\n*2\r\n"); sb.Append($"${key.Length}\r\n{key}\r\n"); sb.Append($"*{entries.Count}\r\n");
        foreach (StreamEntry e in entries) { sb.Append("*2\r\n"); sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n"); sb.Append($"*{e.Fields.Count * 2}\r\n"); foreach (var f in e.Fields) { sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n"); sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n"); } }
        return sb.ToString();
    }

    static void ServeBlockedXReadClients(string key, List<StreamEntry> all)
    {
        if (!blockedXReadWaiters.TryGetValue(key, out List<BlockedXReadRequest>? ws) || ws.Count == 0) return;
        List<BlockedXReadRequest> done = new List<BlockedXReadRequest>();
        foreach (BlockedXReadRequest w in ws) { List<StreamEntry> ne = all.FindAll(e => CompareStreamIds(e.Id, w.StartIdExclusive) > 0); if (ne.Count > 0) { w.NewEntries = ne; w.IsCompleted = true; w.Signal.Set(); done.Add(w); } }
        foreach (BlockedXReadRequest w in done) ws.Remove(w);
        if (ws.Count == 0) blockedXReadWaiters.Remove(key);
    }

    static bool TryParseExplicitStreamId(string id, out ulong ms, out ulong seq)
    {
        ms = seq = 0; if (id == "*") return false;
        string[] p = id.Split('-');
        return p.Length == 2 && ulong.TryParse(p[0], out ms) && ulong.TryParse(p[1], out seq);
    }

    static bool TryParseStreamRangeBound(string input, bool isStart, out string norm)
    {
        norm = string.Empty;
        if (isStart && input == "-") { norm = "0-0"; return true; }
        if (!isStart && input == "+") { norm = $"{ulong.MaxValue}-{ulong.MaxValue}"; return true; }
        string[] p = input.Split('-');
        if (p.Length == 1 && ulong.TryParse(p[0], out ulong ms1)) { norm = $"{ms1}-{(isStart ? 0 : ulong.MaxValue)}"; return true; }
        if (p.Length == 2 && ulong.TryParse(p[0], out ulong ms2) && ulong.TryParse(p[1], out ulong seq)) { norm = $"{ms2}-{seq}"; return true; }
        return false;
    }

    static bool TryParseXReadId(string input, out string norm)
    {
        norm = string.Empty; string[] p = input.Split('-');
        if (p.Length == 1 && ulong.TryParse(p[0], out ulong ms)) { norm = $"{ms}-0"; return true; }
        if (p.Length == 2 && ulong.TryParse(p[0], out ulong ms2) && ulong.TryParse(p[1], out ulong seq)) { norm = $"{ms2}-{seq}"; return true; }
        return false;
    }

    static int CompareStreamIds(string left, string right)
    {
        string[] l = left.Split('-'), r = right.Split('-');
        int c = ulong.Parse(l[0]).CompareTo(ulong.Parse(r[0]));
        return c != 0 ? c : ulong.Parse(l[1]).CompareTo(ulong.Parse(r[1]));
    }

    static void ServeBlockedClientsFromList(string key)
    {
        if (!listStore.TryGetValue(key, out List<string>? list) || list.Count == 0) return;
        if (!blockedPopWaiters.TryGetValue(key, out Queue<BlockedPopRequest>? ws) || ws.Count == 0) return;
        while (list.Count > 0 && ws.Count > 0) { BlockedPopRequest w = ws.Dequeue(); string v = list[0]; list.RemoveAt(0); w.Value = v; w.IsCompleted = true; w.Signal.Set(); }
        if (ws.Count == 0) blockedPopWaiters.Remove(key);
    }

    static string EncodeKeyValueArray(string key, string value) => $"*2\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n";

    static void RemoveBlockedRequest(string key, BlockedPopRequest req)
    {
        if (!blockedPopWaiters.TryGetValue(key, out Queue<BlockedPopRequest>? ws)) return;
        Queue<BlockedPopRequest> f = new Queue<BlockedPopRequest>();
        while (ws.Count > 0) { var c = ws.Dequeue(); if (!ReferenceEquals(c, req)) f.Enqueue(c); }
        if (f.Count == 0) blockedPopWaiters.Remove(key); else blockedPopWaiters[key] = f;
    }

    static List<string> ParseRESP(string input)
    {
        List<string> args = new List<string>();
        string[] lines = input.Split(new[] { "\r\n" }, StringSplitOptions.None);
        int i = 0;
        while (i < lines.Length)
        {
            if (lines[i].StartsWith("*")) { i++; }
            else if (lines[i].StartsWith("$")) { i++; if (i < lines.Length) { args.Add(lines[i]); i++; } }
            else { i++; }
        }
        return args;
    }
}