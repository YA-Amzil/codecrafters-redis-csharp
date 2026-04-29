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
    static readonly object pubSubLock = new object();
    static readonly object sortedSetLock = new object();
    static readonly List<Socket> replicaSockets = new List<Socket>();
    static readonly Dictionary<Socket, long> replicaAckOffsets = new Dictionary<Socket, long>();
    static readonly Dictionary<string, List<Socket>> channelSubscribers = new Dictionary<string, List<Socket>>();

    static readonly Dictionary<string, Dictionary<string, double>> sortedSets =
        new Dictionary<string, Dictionary<string, double>>();

    static readonly object aclLock = new object();
    static readonly Dictionary<string, User> users = new Dictionary<string, User>();
    static readonly object versionLock = new object();
    static long globalVersion = 0;
    static readonly Dictionary<string, long> keyVersions = new Dictionary<string, long>();
    static long masterReplicationOffset = 0;
    static long replicaProcessedOffset = 0;

    static Dictionary<string, Queue<BlockedPopRequest>> blockedPopWaiters =
        new Dictionary<string, Queue<BlockedPopRequest>>();

    static Dictionary<string, List<BlockedXReadRequest>> blockedXReadWaiters =
        new Dictionary<string, List<BlockedXReadRequest>>();

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

        public BlockedXReadRequest(string startId)
        {
            StartIdExclusive = startId;
        }
    }

    // ── Per-connection state ──────────────────────────────────────────────────
    // CHANGED: Added SubscribedChannels to track which channels this client
    // has subscribed to. HashSet ensures duplicates don't increase the count.
    // Added InSubscribedMode to enforce subscribed mode restrictions.
    class ClientState
    {
        public bool InTransaction = false;
        public List<List<string>> QueuedCommands = new List<List<string>>();
        public HashSet<string> SubscribedChannels = new HashSet<string>();
        public bool InSubscribedMode = false;
        public bool IsAuthenticated = false;
        public Dictionary<string, long> WatchedKeys = new Dictionary<string, long>(); // key -> version at watch time
    }

    class User
    {
        public List<string> Flags = new List<string>();
        public List<string> Passwords = new List<string>();
    }

    static string GenerateReplicationId()
    {
        byte[] bytes = RandomNumberGenerator.GetBytes(20);
        return Convert.ToHexString(bytes);
    }

    static void LoadRdbFile()
    {
        if (string.IsNullOrEmpty(configDir) || string.IsNullOrEmpty(configDbfilename)) return;
        string rdbPath = Path.Combine(configDir, configDbfilename);
        if (!File.Exists(rdbPath)) return;
        try
        {
            ParseRdbFile(File.ReadAllBytes(rdbPath));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading RDB file: {ex.Message}");
        }
    }

    static void ParseRdbFile(byte[] b)
    {
        if (b.Length < 9) return;
        if (b[0] != 'R' || b[1] != 'E' || b[2] != 'D' || b[3] != 'I' || b[4] != 'S') return;
        int pos = 9;
        while (pos < b.Length)
        {
            byte op = b[pos++];
            if (op == 0xFF) break;
            if (op == 0xFA)
            {
                if (!TryReadString(b, ref pos, out _)) return;
                if (!TryReadString(b, ref pos, out _)) return;
                continue;
            }

            if (op == 0xFE)
            {
                if (!TryReadLength(b, ref pos, out _)) return;
                continue;
            }

            if (op == 0xFB)
            {
                if (!TryReadLength(b, ref pos, out _)) return;
                if (!TryReadLength(b, ref pos, out _)) return;
                continue;
            }

            if (op == 0xFC)
            {
                if (pos + 8 > b.Length) return;
                ulong ms = BitConverter.ToUInt64(b, pos);
                pos += 8;
                if (pos >= b.Length) return;
                byte vt = b[pos++];
                TryReadKeyValue(b, ref pos, vt, DateTimeOffset.FromUnixTimeMilliseconds((long)ms).UtcDateTime);
                continue;
            }

            if (op == 0xFD)
            {
                if (pos + 4 > b.Length) return;
                uint s = BitConverter.ToUInt32(b, pos);
                pos += 4;
                if (pos >= b.Length) return;
                byte vt = b[pos++];
                TryReadKeyValue(b, ref pos, vt, DateTimeOffset.FromUnixTimeSeconds(s).UtcDateTime);
                continue;
            }

            TryReadKeyValue(b, ref pos, op, null);
        }
    }

    static bool TryReadKeyValue(byte[] b, ref int pos, byte valueType, DateTime? expiry)
    {
        if (!TryReadString(b, ref pos, out string key)) return false;
        if (valueType == 0)
        {
            if (!TryReadString(b, ref pos, out string value)) return false;
            store[key] = (value, expiry);
        }
        else if (valueType == 1)
        {
            if (!TryReadLength(b, ref pos, out int len)) return false;
            List<string> list = new List<string>();
            for (int i = 0; i < len; i++)
            {
                if (!TryReadString(b, ref pos, out string elem)) return false;
                list.Add(elem);
            }

            lock (listLock)
            {
                listStore[key] = list;
            }
        }

        return true;
    }

    static bool TryReadString(byte[] b, ref int pos, out string result)
    {
        result = string.Empty;
        if (pos >= b.Length) return false;
        byte lb = b[pos++];
        int top = lb & 0xC0;
        if (top == 0x00)
        {
            int len = lb & 0x3F;
            if (pos + len > b.Length) return false;
            result = Encoding.UTF8.GetString(b, pos, len);
            pos += len;
            return true;
        }

        if (top == 0x40)
        {
            if (pos >= b.Length) return false;
            int len = ((lb & 0x3F) << 8) | b[pos++];
            if (pos + len > b.Length) return false;
            result = Encoding.UTF8.GetString(b, pos, len);
            pos += len;
            return true;
        }

        if (top == 0x80)
        {
            if (pos + 4 > b.Length) return false;
            int len = (b[pos] << 24) | (b[pos + 1] << 16) | (b[pos + 2] << 8) | b[pos + 3];
            pos += 4;
            if (pos + len > b.Length) return false;
            result = Encoding.UTF8.GetString(b, pos, len);
            pos += len;
            return true;
        }

        int enc = lb & 0x3F;
        if (enc == 0)
        {
            if (pos >= b.Length) return false;
            result = b[pos++].ToString();
            return true;
        }

        if (enc == 1)
        {
            if (pos + 2 > b.Length) return false;
            result = (b[pos] | (b[pos + 1] << 8)).ToString();
            pos += 2;
            return true;
        }

        if (enc == 2)
        {
            if (pos + 4 > b.Length) return false;
            result = (b[pos] | (b[pos + 1] << 8) | (b[pos + 2] << 16) | (b[pos + 3] << 24)).ToString();
            pos += 4;
            return true;
        }

        return false;
    }

    static bool TryReadLength(byte[] b, ref int pos, out int length)
    {
        length = 0;
        if (pos >= b.Length) return false;
        byte lb = b[pos++];
        int top = lb & 0xC0;
        if (top == 0x00)
        {
            length = lb & 0x3F;
            return true;
        }

        if (top == 0x40)
        {
            if (pos >= b.Length) return false;
            length = ((lb & 0x3F) << 8) | b[pos++];
            return true;
        }

        if (top == 0x80)
        {
            if (pos + 4 > b.Length) return false;
            length = (b[pos] << 24) | (b[pos + 1] << 16) | (b[pos + 2] << 8) | b[pos + 3];
            pos += 4;
            return true;
        }

        return false;
    }

    static byte[] LoadEmptyRdbBytes()
    {
        foreach (string path in new[]
                 {
                     Path.Combine(AppContext.BaseDirectory, "empty_rdb.base64"),
                     Path.Combine(AppContext.BaseDirectory, "src", "empty_rdb.base64")
                 })
            if (File.Exists(path))
                return Convert.FromBase64String(File.ReadAllText(path).Trim());
        return Convert.FromBase64String(
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
    }

    static string SendAndReceive(NetworkStream stream, string msg)
    {
        byte[] d = Encoding.UTF8.GetBytes(msg);
        stream.Write(d, 0, d.Length);
        stream.Flush();
        byte[] buf = new byte[512];
        int n = stream.Read(buf, 0, buf.Length);
        return Encoding.UTF8.GetString(buf, 0, n);
    }

    static void SendOnly(NetworkStream stream, string msg)
    {
        byte[] d = Encoding.UTF8.GetBytes(msg);
        stream.Write(d, 0, d.Length);
        stream.Flush();
    }

    static string ReadRespLine(NetworkStream stream)
    {
        List<byte> bytes = new List<byte>();
        while (true)
        {
            int b = stream.ReadByte();
            if (b < 0) throw new IOException("Connection closed");
            bytes.Add((byte)b);
            int c = bytes.Count;
            if (c >= 2 && bytes[c - 2] == '\r' && bytes[c - 1] == '\n') break;
        }

        return Encoding.UTF8.GetString(bytes.ToArray(), 0, bytes.Count - 2);
    }

    static void ReadExact(NetworkStream stream, byte[] buf, int len)
    {
        int read = 0;
        while (read < len)
        {
            int n = stream.Read(buf, read, len - read);
            if (n <= 0) throw new IOException("Connection closed");
            read += n;
        }
    }

    static void ReceiveFullResyncAndRdb(NetworkStream stream)
    {
        string line = ReadRespLine(stream);
        if (!line.StartsWith("+FULLRESYNC")) throw new InvalidOperationException($"Unexpected: {line}");
        string rdbHeader = ReadRespLine(stream);
        if (!rdbHeader.StartsWith("$")) throw new InvalidOperationException($"Unexpected RDB header: {rdbHeader}");
        if (!int.TryParse(rdbHeader.Substring(1), out int len) || len < 0)
            throw new InvalidOperationException("Invalid RDB length");
        byte[] rdb = new byte[len];
        ReadExact(stream, rdb, len);
    }

    static int FindCrlf(List<byte> buf, int start)
    {
        for (int i = start; i + 1 < buf.Count; i++)
            if (buf[i] == '\r' && buf[i + 1] == '\n')
                return i;
        return -1;
    }

    static bool TryParseRespArray(List<byte> buf, out List<string> args, out int consumed)
    {
        args = new List<string>();
        consumed = 0;
        if (buf.Count == 0 || buf[0] != '*') return false;
        int le = FindCrlf(buf, 0);
        if (le < 0) return false;
        if (!int.TryParse(Encoding.UTF8.GetString(buf.GetRange(1, le - 1).ToArray()), out int count) || count < 0)
            throw new InvalidOperationException("Invalid array length");
        int idx = le + 2;
        for (int i = 0; i < count; i++)
        {
            if (idx >= buf.Count || buf[idx] != '$') return false;
            int lle = FindCrlf(buf, idx);
            if (lle < 0) return false;
            if (!int.TryParse(Encoding.UTF8.GetString(buf.GetRange(idx + 1, lle - (idx + 1)).ToArray()), out int len) ||
                len < 0)
                throw new InvalidOperationException("Invalid bulk length");
            idx = lle + 2;
            if (buf.Count < idx + len + 2) return false;
            args.Add(Encoding.UTF8.GetString(buf.GetRange(idx, len).ToArray()));
            idx += len;
            if (buf[idx] != '\r' || buf[idx + 1] != '\n')
                throw new InvalidOperationException("Invalid RESP terminator");
            idx += 2;
        }

        consumed = idx;
        return true;
    }

    static void ProcessMasterReplicationStream(NetworkStream stream)
    {
        byte[] rb = new byte[4096];
        List<byte> pending = new List<byte>();
        while (true)
        {
            int n = stream.Read(rb, 0, rb.Length);
            if (n <= 0) break;
            for (int i = 0; i < n; i++) pending.Add(rb[i]);
            while (TryParseRespArray(pending, out List<string> cmdArgs, out int consumed))
            {
                pending.RemoveRange(0, consumed);
                if (cmdArgs.Count == 0) continue;
                if (cmdArgs.Count == 3 && cmdArgs[0].Equals("REPLCONF", StringComparison.OrdinalIgnoreCase) &&
                    cmdArgs[1].Equals("GETACK", StringComparison.OrdinalIgnoreCase) && cmdArgs[2] == "*")
                {
                    SendOnly(stream, BuildReplConfAck(replicaProcessedOffset));
                    replicaProcessedOffset += consumed;
                    continue;
                }

                DispatchCommand(cmdArgs[0].ToUpperInvariant(), cmdArgs);
                replicaProcessedOffset += consumed;
            }
        }
    }

    static void SendAll(Socket socket, byte[] data)
    {
        int sent = 0;
        while (sent < data.Length) sent += socket.Send(data, sent, data.Length - sent, SocketFlags.None);
    }

    static void RegisterReplica(Socket s)
    {
        lock (replicaLock)
        {
            if (!replicaSockets.Contains(s)) replicaSockets.Add(s);
            replicaAckOffsets[s] = 0;
        }
    }

    static void UnregisterReplica(Socket s)
    {
        lock (replicaLock)
        {
            replicaSockets.Remove(s);
            replicaAckOffsets.Remove(s);
        }
    }

    static void CleanupClientSubscriptions(Socket client)
    {
        lock (pubSubLock)
        {
            // Remove this client from all channel subscriber lists
            foreach (var kvp in channelSubscribers)
            {
                kvp.Value.Remove(client);
            }

            // Remove empty channel entries
            var emptyChannels = channelSubscribers.Where(kvp => kvp.Value.Count == 0).Select(kvp => kvp.Key).ToList();
            foreach (var channel in emptyChannels)
            {
                channelSubscribers.Remove(channel);
            }
        }
    }

    static int CountReplicasAtOrAboveOffset(long offset)
    {
        lock (replicaLock)
        {
            int c = 0;
            foreach (Socket r in replicaSockets)
                if (replicaAckOffsets.TryGetValue(r, out long a) && a >= offset)
                    c++;
            return c;
        }
    }

    static void SendGetAckToReplicas()
    {
        byte[] payload = Encoding.UTF8.GetBytes(BuildRespArray(new List<string> { "REPLCONF", "GETACK", "*" }));
        lock (replicaLock)
        {
            for (int i = replicaSockets.Count - 1; i >= 0; i--)
            {
                try
                {
                    SendAll(replicaSockets[i], payload);
                }
                catch
                {
                    replicaSockets.RemoveAt(i);
                }
            }
        }
    }

    static string BuildRespArray(List<string> args)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"*{args.Count}\r\n");
        foreach (string a in args) sb.Append($"${a.Length}\r\n{a}\r\n");
        return sb.ToString();
    }

    static string BuildReplConfAck(long offset) =>
        BuildRespArray(new List<string> { "REPLCONF", "ACK", offset.ToString() });

    static bool ShouldPropagate(string cmd, string resp) =>
        cmd is "SET" or "INCR" or "RPUSH" or "LPUSH" or "LPOP" or "XADD" && !resp.StartsWith("-") &&
        resp != "$-1\r\n" && resp != "*0\r\n";

    static void PropagateWriteCommand(List<string> args)
    {
        byte[] payload = Encoding.UTF8.GetBytes(BuildRespArray(args));
        lock (replicaLock)
        {
            masterReplicationOffset += payload.Length;
            for (int i = replicaSockets.Count - 1; i >= 0; i--)
            {
                try
                {
                    SendAll(replicaSockets[i], payload);
                }
                catch
                {
                    replicaSockets.RemoveAt(i);
                }
            }
        }
    }

    static void ConnectToMaster(string host, int masterPort, int listeningPort)
    {
        masterClient = new TcpClient();
        masterClient.Connect(host, masterPort);
        masterStream = masterClient.GetStream();
        SendAndReceive(masterStream, "*1\r\n$4\r\nPING\r\n");
        string portStr = listeningPort.ToString();
        SendAndReceive(masterStream,
            $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${portStr.Length}\r\n{portStr}\r\n");
        SendAndReceive(masterStream, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        SendOnly(masterStream, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        ReceiveFullResyncAndRdb(masterStream);
        Thread t = new Thread(() => ProcessMasterReplicationStream(masterStream));
        t.IsBackground = true;
        t.Start();
    }

    static void Main(string[] args)
    {
        // Initialize default user with nopass flag and empty passwords
        User defaultUser = new User();
        defaultUser.Flags.Add("nopass");
        lock (aclLock)
        {
            users["default"] = defaultUser;
        }

        int port = 6379;
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" && i + 1 < args.Length)
            {
                int.TryParse(args[++i], out port);
            }
            else if (args[i] == "--replicaof" && i + 1 < args.Length)
            {
                isReplica = true;
                string[] p = args[++i].Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (p.Length >= 2 && int.TryParse(p[1], out int rp))
                {
                    replicaHost = p[0];
                    replicaPort = rp;
                }
            }
            else if (args[i] == "--dir" && i + 1 < args.Length)
            {
                configDir = args[++i];
            }
            else if (args[i] == "--dbfilename" && i + 1 < args.Length)
            {
                configDbfilename = args[++i];
            }
        }

        LoadRdbFile();
        if (isReplica) ConnectToMaster(replicaHost, replicaPort, port);
        TcpListener server = new TcpListener(IPAddress.Any, port);
        server.Start();
        Console.WriteLine($"Redis server started on port {port}...");
        while (true)
        {
            try
            {
                Socket client = server.AcceptSocket();
                Console.WriteLine("Client connected.");
                new Thread(() => HandleClient(client)).Start();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Server error: {ex.Message}");
                break;
            }
        }

        server.Stop();
    }

    // ── HandleClient ─────────────────────────────────────────────────────────
    // CHANGED: SUBSCRIBE is now handled here directly (not via DispatchCommand)
    // because it needs access to both the client Socket and ClientState.
    // For each channel in the arguments we send one response immediately,
    // then continue the receive loop so further SUBSCRIBE commands work too.
    static void HandleClient(Socket client)
    {
        byte[] buffer = new byte[1024];
        ClientState state = new ClientState();
        // Initialize authentication state based on whether default user requires password
        state.IsAuthenticated = IsDefaultUserAuthenticated();

        while (true)
        {
            try
            {
                int n = client.Receive(buffer);
                if (n == 0)
                {
                    Console.WriteLine("Client disconnected.");
                    break;
                }

                string input = Encoding.UTF8.GetString(buffer, 0, n);
                List<string> args = ParseRESP(input);
                if (args.Count == 0) continue;

                string command = args[0].ToUpperInvariant();
                string response;

                // Replica ACK — one-way, no reply
                if (command == "REPLCONF" && args.Count >= 3 &&
                    args[1].Equals("ACK", StringComparison.OrdinalIgnoreCase) &&
                    long.TryParse(args[2], out long ackOff))
                {
                    lock (replicaLock)
                    {
                        if (replicaSockets.Contains(client)) replicaAckOffsets[client] = ackOff;
                    }

                    continue;
                }

                // Commands that don't require authentication
                bool isAuthExempt = command == "AUTH" || command == "QUIT" || command == "RESET" ||
                                    (command == "PSYNC") || (command == "REPLCONF");

                // Check authentication for all commands except exempt ones
                if (!isAuthExempt && !state.IsAuthenticated)
                {
                    response = "-NOAUTH Authentication required.\r\n";
                    client.Send(Encoding.UTF8.GetBytes(response));
                    continue;
                }

                // ── SUBSCRIBE ────────────────────────────────────────────────
                // Handled here because it needs per-client channel state and
                // sends one response frame per channel, not one for the whole command.
                // HashSet.Add() returns false for duplicates → count stays the same.
                // When subscribing for the first time, enter subscribed mode.
                if (command == "SUBSCRIBE")
                {
                    if (args.Count < 2)
                    {
                        client.Send(Encoding.UTF8.GetBytes("-ERR wrong number of arguments for 'SUBSCRIBE'\r\n"));
                        continue;
                    }

                    for (int j = 1; j < args.Count; j++)
                    {
                        string channel = args[j];
                        bool wasEmpty = state.SubscribedChannels.Count == 0;
                        bool isNewSubscription =
                            state.SubscribedChannels.Add(channel); // Returns false if already present

                        // Update global channel subscriber mapping
                        if (isNewSubscription)
                        {
                            lock (pubSubLock)
                            {
                                if (!channelSubscribers.ContainsKey(channel))
                                    channelSubscribers[channel] = new List<Socket>();
                                if (!channelSubscribers[channel].Contains(client))
                                    channelSubscribers[channel].Add(client);
                            }
                        }

                        if (wasEmpty) state.InSubscribedMode = true; // Enter subscribed mode on first subscription
                        client.Send(
                            Encoding.UTF8.GetBytes(BuildSubscribeResponse(channel, state.SubscribedChannels.Count)));
                    }

                    continue; // skip the generic send below
                }

                // ── UNSUBSCRIBE ──────────────────────────────────────────────
                // Remove subscriptions; if no channels left, exit subscribed mode.
                if (command == "UNSUBSCRIBE")
                {
                    StringBuilder sb = new StringBuilder();
                    if (args.Count == 1) // No channels specified → unsubscribe from all
                    {
                        List<string> channels = new List<string>(state.SubscribedChannels);
                        foreach (string channel in channels)
                        {
                            state.SubscribedChannels.Remove(channel);
                            lock (pubSubLock)
                            {
                                if (channelSubscribers.TryGetValue(channel, out List<Socket>? subscribers))
                                {
                                    subscribers.Remove(client);
                                    if (subscribers.Count == 0) channelSubscribers.Remove(channel);
                                }
                            }

                            sb.Append(BuildUnsubscribeResponse(channel, state.SubscribedChannels.Count));
                        }
                    }
                    else
                    {
                        for (int j = 1; j < args.Count; j++)
                        {
                            string channel = args[j];
                            state.SubscribedChannels.Remove(channel);
                            lock (pubSubLock)
                            {
                                if (channelSubscribers.TryGetValue(channel, out List<Socket>? subscribers))
                                {
                                    subscribers.Remove(client);
                                    if (subscribers.Count == 0) channelSubscribers.Remove(channel);
                                }
                            }

                            sb.Append(BuildUnsubscribeResponse(channel, state.SubscribedChannels.Count));
                        }
                    }

                    if (state.SubscribedChannels.Count == 0) state.InSubscribedMode = false;
                    client.Send(Encoding.UTF8.GetBytes(sb.ToString()));
                    continue;
                }

                // ── Subscribed Mode Restrictions ─────────────────────────────────
                // In subscribed mode, only certain commands are allowed.
                if (state.InSubscribedMode)
                {
                    // Allowed commands: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT, RESET
                    if (command != "SUBSCRIBE" && command != "UNSUBSCRIBE" && command != "PSUBSCRIBE" &&
                        command != "PUNSUBSCRIBE" && command != "PING" && command != "QUIT" && command != "RESET")
                    {
                        response =
                            $"-ERR Can't execute '{command.ToLower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
                        client.Send(Encoding.UTF8.GetBytes(response));
                        continue;
                    }
                }

                // ── PING in Subscribed Mode ───────────────────────────────────────
                // In subscribed mode, PING returns a different format: ["pong", ""]
                // instead of the normal +PONG\r\n
                if (command == "PING" && state.InSubscribedMode)
                {
                    response = "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
                    client.Send(Encoding.UTF8.GetBytes(response));
                    continue;
                }

                if (command == "MULTI")
                {
                    if (state.InTransaction)
                        response = "-ERR MULTI calls can not be nested\r\n";
                    else
                    {
                        state.InTransaction = true;
                        state.QueuedCommands.Clear();
                        response = "+OK\r\n";
                    }
                }
                else if (command == "EXEC")
                {
                    response = HandleExec(state);
                }
                else if (command == "DISCARD")
                {
                    if (!state.InTransaction)
                        response = "-ERR DISCARD without MULTI\r\n";
                    else
                    {
                        state.InTransaction = false;
                        state.QueuedCommands.Clear();
                        response = "+OK\r\n";
                    }
                }
                else if (command == "WATCH")
                {
                    if (state.InTransaction)
                    {
                        response = "-ERR WATCH inside MULTI is not allowed\r\n";
                    }
                    else
                    {
                        // Store the watched keys with their current versions
                        if (args.Count >= 2)
                        {
                            lock (versionLock)
                            {
                                for (int i = 1; i < args.Count; i++)
                                {
                                    string key = args[i];
                                    long version = keyVersions.TryGetValue(key, out long v) ? v : 0;
                                    state.WatchedKeys[key] = version;
                                }
                            }

                            response = "+OK\r\n";
                        }
                        else
                        {
                            response = "-ERR wrong number of arguments for 'WATCH'\r\n";
                        }
                    }
                }
                else if (state.InTransaction)
                {
                    state.QueuedCommands.Add(args);
                    response = "+QUEUED\r\n";
                }
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
                    // Update authentication state if AUTH command succeeded
                    if (command == "AUTH" && response == "+OK\r\n")
                    {
                        state.IsAuthenticated = true;
                    }
                }

                client.Send(Encoding.UTF8.GetBytes(response));
                if (ShouldPropagate(command, response)) PropagateWriteCommand(args);
            }
            catch (SocketException)
            {
                Console.WriteLine("Client connection lost.");
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Client error: {ex.Message}");
                break;
            }
        }

        CleanupClientSubscriptions(client);
        UnregisterReplica(client);
        client.Close();
    }

    // ── BuildSubscribeResponse ────────────────────────────────────────────────
    // Builds the 3-element RESP array for one SUBSCRIBE confirmation.
    // Format: ["subscribe", <channel>, <total subscribed count for this client>]
    static string BuildSubscribeResponse(string channel, int count)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("*3\r\n");
        sb.Append("$9\r\nsubscribe\r\n");
        sb.Append($"${channel.Length}\r\n{channel}\r\n");
        sb.Append($":{count}\r\n");
        return sb.ToString();
    }

    // ── BuildUnsubscribeResponse ──────────────────────────────────────────────
    // Builds the 3-element RESP array for one UNSUBSCRIBE confirmation.
    // Format: ["unsubscribe", <channel>, <total subscribed count for this client>]
    static string BuildUnsubscribeResponse(string channel, int count)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("*3\r\n");
        sb.Append("$11\r\nunsubscribe\r\n");
        sb.Append($"${channel.Length}\r\n{channel}\r\n");
        sb.Append($":{count}\r\n");
        return sb.ToString();
    }

    // ── BuildMessageResponse ──────────────────────────────────────────────────
    // Builds the 3-element RESP array for a published message.
    // Format: ["message", <channel>, <message_contents>]
    static string BuildMessageResponse(string channel, string message)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("*3\r\n");
        sb.Append("$7\r\nmessage\r\n");
        sb.Append($"${channel.Length}\r\n{channel}\r\n");
        sb.Append($"${message.Length}\r\n{message}\r\n");
        return sb.ToString();
    }

    static string HandleExec(ClientState state)
    {
        if (!state.InTransaction) return "-ERR EXEC without MULTI\r\n";

        // Check if any watched keys were modified
        bool transactionAborted = false;
        lock (versionLock)
        {
            foreach (var watchedKey in state.WatchedKeys)
            {
                string key = watchedKey.Key;
                long versionAtWatch = watchedKey.Value;
                long currentVersion = keyVersions.TryGetValue(key, out long v) ? v : 0;

                // If the version changed, the key was modified
                if (currentVersion != versionAtWatch)
                {
                    transactionAborted = true;
                    break;
                }
            }
        }

        // Clear watched keys (regardless of abort or success)
        state.WatchedKeys.Clear();

        // If transaction was aborted, return null array
        if (transactionAborted)
        {
            state.InTransaction = false;
            state.QueuedCommands.Clear();
            return "*-1\r\n";
        }

        // Execute all queued commands
        List<string> results = new List<string>();
        foreach (List<string> q in state.QueuedCommands) results.Add(DispatchCommand(q[0].ToUpperInvariant(), q));

        state.InTransaction = false;
        state.QueuedCommands.Clear();

        // Build response array
        StringBuilder sb = new StringBuilder();
        sb.Append($"*{results.Count}\r\n");
        foreach (string r in results) sb.Append(r);
        return sb.ToString();
    }

    // ── DispatchCommand ───────────────────────────────────────────────────────
    // CHANGED: SUBSCRIBE removed — it now lives in HandleClient where it has
    // access to per-client state and can send multiple response frames.
    static string DispatchCommand(string command, List<string> args)
    {
        switch (command)
        {
            case "PING":
                return "+PONG\r\n";
            case "ECHO":
                return args.Count > 1 ? $"${args[1].Length}\r\n{args[1]}\r\n" : "$-1\r\n";
            case "REPLCONF":
                return "+OK\r\n";
            case "PSYNC":
                return HandlePSync(args);
            case "WAIT":
                return HandleWait(args);
            case "CONFIG":
                return HandleConfig(args);
            case "KEYS":
                return HandleKeys(args);
            case "SET":
                return HandleSet(args);
            case "GET":
                return HandleGet(args);
            case "INFO":
                return HandleInfo(args);
            case "INCR":
                return HandleIncr(args);
            case "RPUSH":
                return HandleRPush(args);
            case "LPUSH":
                return HandleLPush(args);
            case "LRANGE":
                return HandleLRange(args);
            case "LLEN":
                return HandleLLen(args);
            case "LPOP":
                return HandleLPop(args);
            case "BLPOP":
                return HandleBLPop(args);
            case "TYPE":
                return HandleType(args);
            case "XADD":
                return HandleXAdd(args);
            case "XRANGE":
                return HandleXRange(args);
            case "XREAD":
                return HandleXRead(args);
            case "PUBLISH":
                return HandlePublish(args);
            case "ZADD":
                return HandleZAdd(args);
            case "ZRANK":
                return HandleZRank(args);
            case "ZSCORE":
                return HandleZScore(args);
            case "ZRANGE":
                return HandleZRange(args);
            case "ZCARD":
                return HandleZCard(args);
            case "ZREM":
                return HandleZRem(args);
            case "GEOADD":
                return HandleGeoadd(args);
            case "GEOPOS":
                return HandleGeopos(args);
            case "GEODIST":
                return HandleGeodist(args);
            case "GEOSEARCH":
                return HandleGeosearch(args);
            case "ACL":
                return HandleAcl(args);
            case "AUTH":
                return HandleAuth(args);
            default:
                return "-ERR unknown command\r\n";
        }
    }

    static string HandleSet(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'SET'\r\n";
        string key = args[1], value = args[2];
        DateTime? expiry = null;
        for (int i = 3; i < args.Count - 1; i++)
            if (args[i].Equals("PX", StringComparison.OrdinalIgnoreCase) && int.TryParse(args[i + 1], out int ms))
            {
                expiry = DateTime.UtcNow.AddMilliseconds(ms);
                i++;
            }

        store[key] = (value, expiry);

        // Increment version for this key
        lock (versionLock)
        {
            keyVersions[key] = ++globalVersion;
        }

        return "+OK\r\n";
    }

    static string HandleGet(List<string> args)
    {
        if (args.Count < 2) return "-ERR wrong number of arguments for 'GET'\r\n";
        if (store.ContainsKey(args[1]))
        {
            var (v, ex) = store[args[1]];
            if (ex.HasValue && DateTime.UtcNow > ex.Value)
            {
                store.Remove(args[1]);
                return "$-1\r\n";
            }

            return $"${v.Length}\r\n{v}\r\n";
        }

        return "$-1\r\n";
    }

    static string HandleInfo(List<string> args)
    {
        if (args.Count >= 2 && args[1].Equals("replication", StringComparison.OrdinalIgnoreCase))
        {
            string body =
                $"role:{(isReplica ? "slave" : "master")}\r\nmaster_replid:{masterReplId}\r\nmaster_repl_offset:{masterReplOffset}";
            return $"${body.Length}\r\n{body}\r\n";
        }

        return "$0\r\n\r\n";
    }

    static string HandleAcl(List<string> args)
    {
        if (args.Count >= 2 && args[1].Equals("WHOAMI", StringComparison.OrdinalIgnoreCase))
        {
            const string username = "default";
            return $"${username.Length}\r\n{username}\r\n";
        }

        if (args.Count >= 3 && args[1].Equals("GETUSER", StringComparison.OrdinalIgnoreCase))
        {
            string username = args[2];
            // For now, only support the "default" user
            if (username.Equals("default", StringComparison.OrdinalIgnoreCase))
            {
                lock (aclLock)
                {
                    if (users.TryGetValue("default", out User? user))
                    {
                        // Build RESP array: ["flags", [flagsArray], "passwords", [passwordsArray]]
                        StringBuilder sb = new StringBuilder();
                        sb.Append("*4\r\n");

                        // Add "flags" property name
                        sb.Append("$5\r\nflags\r\n");

                        // Add flags array
                        sb.Append($"*{user.Flags.Count}\r\n");
                        foreach (string flag in user.Flags)
                        {
                            sb.Append($"${flag.Length}\r\n{flag}\r\n");
                        }

                        // Add "passwords" property name
                        sb.Append("$9\r\npasswords\r\n");

                        // Add passwords array
                        sb.Append($"*{user.Passwords.Count}\r\n");
                        foreach (string password in user.Passwords)
                        {
                            sb.Append($"${password.Length}\r\n{password}\r\n");
                        }

                        return sb.ToString();
                    }
                }
            }

            // User not found
            return "$-1\r\n";
        }

        if (args.Count >= 4 && args[1].Equals("SETUSER", StringComparison.OrdinalIgnoreCase))
        {
            string username = args[2];
            if (!username.Equals("default", StringComparison.OrdinalIgnoreCase))
            {
                return "-ERR Only the 'default' user is supported\r\n";
            }

            lock (aclLock)
            {
                if (!users.TryGetValue("default", out User? user))
                {
                    user = new User();
                    users["default"] = user;
                }

                // Parse the remaining arguments for password rules
                for (int i = 3; i < args.Count; i++)
                {
                    string rule = args[i];
                    if (rule.StartsWith(">"))
                    {
                        // Add password rule: >password
                        string password = rule.Substring(1);
                        string passwordHash = ComputeSha256(password);
                        user.Passwords.Add(passwordHash);
                        // Remove nopass flag when a password is set
                        user.Flags.Remove("nopass");
                    }
                    else if (rule.Equals("+nopass", StringComparison.OrdinalIgnoreCase))
                    {
                        // Add nopass flag
                        if (!user.Flags.Contains("nopass"))
                        {
                            user.Flags.Add("nopass");
                        }
                    }
                    else if (rule.Equals("-nopass", StringComparison.OrdinalIgnoreCase))
                    {
                        // Remove nopass flag
                        user.Flags.Remove("nopass");
                    }
                }
            }

            return "+OK\r\n";
        }

        return "-ERR Unknown subcommand\r\n";
    }

    static string ComputeSha256(string input)
    {
        using (SHA256 sha256 = SHA256.Create())
        {
            byte[] hash = sha256.ComputeHash(Encoding.UTF8.GetBytes(input));
            return Convert.ToHexString(hash).ToLower();
        }
    }

    static bool IsDefaultUserAuthenticated()
    {
        // Check if the default user has the nopass flag
        // If it does, new connections are authenticated. If not, they require AUTH.
        lock (aclLock)
        {
            if (users.TryGetValue("default", out User? user))
            {
                return user.Flags.Contains("nopass");
            }
        }

        return true; // Default to authenticated if user doesn't exist (shouldn't happen)
    }

    static string HandleAuth(List<string> args)
    {
        if (args.Count != 3)
        {
            return "-ERR wrong number of arguments for 'AUTH'\r\n";
        }

        string username = args[1];
        string password = args[2];

        // Compute SHA-256 hash of the provided password
        string passwordHash = ComputeSha256(password);

        lock (aclLock)
        {
            // Check if user exists
            if (!users.TryGetValue(username, out User? user))
            {
                return "-WRONGPASS invalid username-password pair or user is disabled.\r\n";
            }

            // Check if the password hash matches any password in the user's list
            if (user.Passwords.Contains(passwordHash))
            {
                return "+OK\r\n";
            }

            // Also check for nopass flag - if present and user has no passwords, allow
            if (user.Flags.Contains("nopass") && user.Passwords.Count == 0)
            {
                return "+OK\r\n";
            }

            return "-WRONGPASS invalid username-password pair or user is disabled.\r\n";
        }
    }

    static string HandleConfig(List<string> args)
    {
        if (args.Count >= 3 && args[1].Equals("GET", StringComparison.OrdinalIgnoreCase))
        {
            string p = args[2].ToLowerInvariant();
            if (p == "dir") return $"*2\r\n$3\r\ndir\r\n${configDir.Length}\r\n{configDir}\r\n";
            if (p == "dbfilename")
                return $"*2\r\n$10\r\ndbfilename\r\n${configDbfilename.Length}\r\n{configDbfilename}\r\n";
            return "*0\r\n";
        }

        return "-ERR Unknown subcommand\r\n";
    }

    static string HandleKeys(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'KEYS'\r\n";
        if (args[1] != "*") return "*0\r\n";
        List<string> keys = new List<string>();
        foreach (var kvp in store)
        {
            var (_, ex) = kvp.Value;
            if (!ex.HasValue || DateTime.UtcNow <= ex.Value) keys.Add(kvp.Key);
        }

        lock (listLock)
            foreach (string k in listStore.Keys)
                if (!keys.Contains(k))
                    keys.Add(k);
        lock (streamLock)
            foreach (string k in streamStore.Keys)
                if (!keys.Contains(k))
                    keys.Add(k);
        lock (sortedSetLock)
            foreach (string k in sortedSets.Keys)
                if (!keys.Contains(k))
                    keys.Add(k);
        StringBuilder sb = new StringBuilder();
        sb.Append($"*{keys.Count}\r\n");
        foreach (string k in keys) sb.Append($"${k.Length}\r\n{k}\r\n");
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
        if (!int.TryParse(args[1], out int req) || !int.TryParse(args[2], out int tms))
            return "-ERR value is not an integer or out of range\r\n";
        if (req == 0) return ":0\r\n";
        long target;
        lock (replicaLock)
        {
            target = masterReplicationOffset;
        }

        int acked = CountReplicasAtOrAboveOffset(target);
        if (acked >= req) return $":{acked}\r\n";
        SendGetAckToReplicas();
        DateTime dl = DateTime.UtcNow.AddMilliseconds(tms);
        while (DateTime.UtcNow < dl)
        {
            acked = CountReplicasAtOrAboveOffset(target);
            if (acked >= req) break;
            Thread.Sleep(10);
        }

        return $":{CountReplicasAtOrAboveOffset(target)}\r\n";
    }

    static string HandleIncr(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'INCR'\r\n";
        string key = args[1];
        if (store.TryGetValue(key, out var entry))
        {
            if (entry.expiry.HasValue && DateTime.UtcNow > entry.expiry.Value)
            {
                store[key] = ("1", null);
                lock (versionLock)
                {
                    keyVersions[key] = ++globalVersion;
                }

                return ":1\r\n";
            }

            if (!long.TryParse(entry.value, out long cur)) return "-ERR value is not an integer or out of range\r\n";
            store[key] = ((cur + 1).ToString(), entry.expiry);
            lock (versionLock)
            {
                keyVersions[key] = ++globalVersion;
            }

            return $":{cur + 1}\r\n";
        }

        store[key] = ("1", null);
        lock (versionLock)
        {
            keyVersions[key] = ++globalVersion;
        }

        return ":1\r\n";
    }

    static string HandlePublish(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'PUBLISH'\r\n";
        string channel = args[1];
        string message = args[2];
        int subscriberCount = 0;
        List<Socket> subscribers = new List<Socket>();

        lock (pubSubLock)
        {
            if (channelSubscribers.TryGetValue(channel, out List<Socket>? subs))
            {
                subscriberCount = subs.Count;
                subscribers.AddRange(subs); // Copy the list to avoid lock contention
            }
        }

        // Deliver message to each subscriber
        string messageResponse = BuildMessageResponse(channel, message);
        byte[] messageBytes = Encoding.UTF8.GetBytes(messageResponse);

        foreach (Socket subscriber in subscribers)
        {
            try
            {
                subscriber.Send(messageBytes);
            }
            catch (Exception ex)
            {
                // If sending fails, the client will be cleaned up when it disconnects
                Console.WriteLine($"Failed to deliver message to subscriber: {ex.Message}");
            }
        }

        return $":{subscriberCount}\r\n";
    }

    static string HandleZAdd(List<string> args)
    {
        // ZADD key score member [score member ...]
        if (args.Count < 4 || (args.Count % 2) != 0) return "-ERR wrong number of arguments for 'ZADD'\r\n";

        string key = args[1];
        int newMembersAdded = 0;

        lock (sortedSetLock)
        {
            if (!sortedSets.ContainsKey(key)) sortedSets[key] = new Dictionary<string, double>();

            var members = sortedSets[key];

            // Process pairs of (score, member)
            for (int i = 2; i < args.Count; i += 2)
            {
                if (!double.TryParse(args[i], NumberStyles.Float, CultureInfo.InvariantCulture, out double score))
                    return "-ERR value is not a valid float\r\n";

                string member = args[i + 1];

                // Only count if it's a new member (not already in the set)
                if (!members.ContainsKey(member)) newMembersAdded++;

                members[member] = score;
            }
        }

        return $":{newMembersAdded}\r\n";
    }

    static string HandleZRank(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'ZRANK'\r\n";

        string key = args[1];
        string member = args[2];

        lock (sortedSetLock)
        {
            // Check if key exists
            if (!sortedSets.TryGetValue(key, out var members)) return "$-1\r\n";

            // Check if member exists
            if (!members.ContainsKey(member)) return "$-1\r\n";

            // Get all members sorted by (score ascending, then member name lexicographically)
            var sortedMembers = members.OrderBy(kvp => kvp.Value) // Sort by score ascending
                .ThenBy(kvp => kvp.Key) // Then by member name lexicographically
                .ToList();

            // Find the rank (0-based index)
            for (int i = 0; i < sortedMembers.Count; i++)
            {
                if (sortedMembers[i].Key == member) return $":{i}\r\n";
            }
        }

        return "$-1\r\n";
    }

    static string HandleZScore(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'ZSCORE'\r\n";

        string key = args[1];
        string member = args[2];

        lock (sortedSetLock)
        {
            // Check if key exists
            if (!sortedSets.TryGetValue(key, out var members)) return "$-1\r\n";

            // Check if member exists in the set
            if (!members.TryGetValue(member, out double score)) return "$-1\r\n";

            // Return the score as a RESP bulk string
            string scoreStr = score.ToString("G", CultureInfo.InvariantCulture);
            return $"${scoreStr.Length}\r\n{scoreStr}\r\n";
        }
    }

    static string HandleZCard(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'ZCARD'\r\n";

        string key = args[1];

        lock (sortedSetLock)
        {
            // Check if key exists and return count, or 0 if doesn't exist
            if (sortedSets.TryGetValue(key, out var members))
                return $":{members.Count}\r\n";
            else
                return ":0\r\n";
        }
    }

    static string HandleZRange(List<string> args)
    {
        if (args.Count != 4) return "-ERR wrong number of arguments for 'ZRANGE'\r\n";

        string key = args[1];
        if (!int.TryParse(args[2], out int start) || !int.TryParse(args[3], out int stop))
            return "-ERR value is not an integer or out of range\r\n";

        lock (sortedSetLock)
        {
            // Check if key exists
            if (!sortedSets.TryGetValue(key, out var members)) return "*0\r\n";

            int count = members.Count;

            // Normalize negative indexes
            if (start < 0) start = count + start;
            if (stop < 0) stop = count + stop;

            // Clamp to valid range
            if (start < 0) start = 0;
            if (stop < 0) stop = 0;

            // Check boundary conditions
            if (start >= count || start > stop) return "*0\r\n";

            // Clamp stop to count-1
            stop = Math.Min(stop, count - 1);

            // Get all members sorted by (score ascending, then member name lexicographically)
            var sortedMembers = members.OrderBy(kvp => kvp.Value) // Sort by score ascending
                .ThenBy(kvp => kvp.Key) // Then by member name lexicographically
                .ToList();

            // Build response
            int responseCount = stop - start + 1;
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{responseCount}\r\n");

            for (int i = start; i <= stop; i++)
            {
                string member = sortedMembers[i].Key;
                sb.Append($"${member.Length}\r\n{member}\r\n");
            }

            return sb.ToString();
        }
    }

    static string HandleZRem(List<string> args)
    {
        // ZREM key member [member ...]
        if (args.Count < 3) return "-ERR wrong number of arguments for 'ZREM'\r\n";

        string key = args[1];
        int removedCount = 0;

        lock (sortedSetLock)
        {
            // Check if key exists
            if (!sortedSets.TryGetValue(key, out var members)) return ":0\r\n";

            // Remove each specified member
            for (int i = 2; i < args.Count; i++)
            {
                if (members.Remove(args[i])) removedCount++;
            }

            // If the set is now empty, remove the key entirely
            if (members.Count == 0) sortedSets.Remove(key);
        }

        return $":{removedCount}\r\n";
    }

    // ── Geohashing helpers for GEOADD ────────────────────────────────────────
    // These methods implement the geohashing algorithm to convert latitude and
    // longitude to a 52-bit score for storage in sorted sets.

    // Spreads a 32-bit integer to 64-bit by inserting 32 zero bits in-between
    static long SpreadInt32ToInt64(int v)
    {
        long result = v & 0xFFFFFFFF;
        result = (result | (result << 16)) & 0x0000FFFF0000FFFFL;
        result = (result | (result << 8)) & 0x00FF00FF00FF00FFL;
        result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0FL;
        result = (result | (result << 2)) & 0x3333333333333333L;
        result = (result | (result << 1)) & 0x5555555555555555L;
        return result;
    }

    // Interleaves the bits of latitude and longitude to create a 52-bit score
    static long Interleave(int latitude, int longitude)
    {
        long spreadLat = SpreadInt32ToInt64(latitude);
        long spreadLon = SpreadInt32ToInt64(longitude);
        long lonShifted = spreadLon << 1;
        return spreadLat | lonShifted;
    }

    // Encodes latitude and longitude to a geohash score
    static long EncodeGeohash(double latitude, double longitude)
    {
        const double MinLatitude = -85.05112878;
        const double MaxLatitude = 85.05112878;
        const double MinLongitude = -180.0;
        const double MaxLongitude = 180.0;
        const double LatitudeRange = MaxLatitude - MinLatitude;
        const double LongitudeRange = MaxLongitude - MinLongitude;

        // Normalize to [0, 2^26) range
        double normalizedLatitude = Math.Pow(2, 26) * (latitude - MinLatitude) / LatitudeRange;
        double normalizedLongitude = Math.Pow(2, 26) * (longitude - MinLongitude) / LongitudeRange;

        // Truncate to integers
        int normalizedLatitudeInt = (int)normalizedLatitude;
        int normalizedLongitudeInt = (int)normalizedLongitude;

        return Interleave(normalizedLatitudeInt, normalizedLongitudeInt);
    }

    // ────────────────────────────────────────────────────────────────────────
    // Geohashing decoder - reverses the encoding process

    // Compacts a 64-bit integer back to 32-bit by removing interleaved zeros
    static int CompactInt64ToInt32(long v)
    {
        v = v & 0x5555555555555555L;
        v = (v | (v >> 1)) & 0x3333333333333333L;
        v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0FL;
        v = (v | (v >> 4)) & 0x00FF00FF00FF00FFL;
        v = (v | (v >> 8)) & 0x0000FFFF0000FFFFL;
        v = (v | (v >> 16)) & 0x00000000FFFFFFFFL;
        return (int)v;
    }

    // Converts grid numbers (latitude and longitude as 26-bit integers) back to geographic coordinates
    static (double latitude, double longitude) ConvertGridNumbersToCoordinates(int latGrid, int lonGrid)
    {
        const double MinLatitude = -85.05112878;
        const double MaxLatitude = 85.05112878;
        const double MinLongitude = -180.0;
        const double MaxLongitude = 180.0;
        const double LatitudeRange = MaxLatitude - MinLatitude;
        const double LongitudeRange = MaxLongitude - MinLongitude;
        double GridSize = Math.Pow(2, 26); // 26 bits for each coordinate

        // Convert grid numbers to [0, 1) range, then scale to coordinate range
        double latNormalized = (latGrid + 0.5) / GridSize; // +0.5 to get center of cell
        double lonNormalized = (lonGrid + 0.5) / GridSize; // +0.5 to get center of cell

        double latitude = MinLatitude + latNormalized * LatitudeRange;
        double longitude = MinLongitude + lonNormalized * LongitudeRange;

        return (latitude, longitude);
    }

    // Decodes a geohash score back to (latitude, longitude)
    static (double latitude, double longitude) DecodeGeohash(long score)
    {
        // Extract the odd bits (latitude) and even bits (longitude)
        long latBits = score & 0x5555555555555555L; // Odd positions
        long lonBits = (score >> 1) & 0x5555555555555555L; // Even positions

        // Compact back to 32-bit integers
        int latGrid = CompactInt64ToInt32(latBits);
        int lonGrid = CompactInt64ToInt32(lonBits);

        return ConvertGridNumbersToCoordinates(latGrid, lonGrid);
    }

    static string HandleGeoadd(List<string> args)
    {
        // GEOADD key longitude latitude member [longitude latitude member ...]
        if (args.Count < 5) return "-ERR wrong number of arguments for 'GEOADD'\r\n";

        // Validate that we have an odd number of location triplets after the key
        // args[0] = "GEOADD", args[1] = key, then triplets of (longitude, latitude, member)
        if ((args.Count - 2) % 3 != 0) return "-ERR syntax error\r\n";

        // Validate all longitude/latitude pairs
        const double MaxLatitude = 85.05112878;
        const double MinLatitude = -85.05112878;
        const double MaxLongitude = 180.0;
        const double MinLongitude = -180.0;

        // Validate each triplet of (longitude, latitude, member)
        for (int i = 2; i < args.Count; i += 3)
        {
            if (!double.TryParse(args[i], out double longitude)) return "-ERR longitude is not a valid double\r\n";

            if (!double.TryParse(args[i + 1], out double latitude)) return "-ERR latitude is not a valid double\r\n";

            // Check longitude range: -180 to +180
            if (longitude < MinLongitude || longitude > MaxLongitude)
                return $"-ERR invalid longitude,latitude pair {longitude},{args[i + 1]}\r\n";

            // Check latitude range: -85.05112878 to +85.05112878
            if (latitude < MinLatitude || latitude > MaxLatitude)
                return $"-ERR invalid longitude,latitude pair {args[i]},{latitude}\r\n";
        }

        // All validations passed, now store the locations in the sorted set
        string key = args[1];
        int addedCount = 0;

        lock (sortedSetLock)
        {
            if (!sortedSets.ContainsKey(key)) sortedSets[key] = new Dictionary<string, double>();

            var members = sortedSets[key];

            // Add each location with calculated geohash score
            for (int i = 2; i < args.Count; i += 3)
            {
                double latitude = double.Parse(args[i + 1]);
                double longitude = double.Parse(args[i]);
                string member = args[i + 2];
                double score = (double)EncodeGeohash(latitude, longitude);

                if (!members.ContainsKey(member)) addedCount++;

                members[member] = score;
            }
        }

        return $":{addedCount}\r\n";
    }

    static string HandleGeopos(List<string> args)
    {
        // GEOPOS key member [member ...]
        if (args.Count < 3) return "-ERR wrong number of arguments for 'GEOPOS'\r\n";

        string key = args[1];
        StringBuilder sb = new StringBuilder();

        // Build response array with one entry per requested member
        int memberCount = args.Count - 2;
        sb.Append($"*{memberCount}\r\n");

        lock (sortedSetLock)
        {
            // Check if key exists
            if (!sortedSets.TryGetValue(key, out var members))
            {
                // Key doesn't exist, return null arrays for all members
                for (int i = 0; i < memberCount; i++) sb.Append("*-1\r\n");
            }
            else
            {
                // Key exists, check each member
                for (int i = 2; i < args.Count; i++)
                {
                    string member = args[i];
                    if (members.TryGetValue(member, out double score))
                    {
                        // Member exists, decode the geohash score to get coordinates
                        var (latitude, longitude) = DecodeGeohash((long)score);

                        // Format as bulk strings using InvariantCulture for consistent decimal representation
                        string lonStr = longitude.ToString("G", CultureInfo.InvariantCulture);
                        string latStr = latitude.ToString("G", CultureInfo.InvariantCulture);

                        sb.Append("*2\r\n");
                        sb.Append($"${lonStr.Length}\r\n{lonStr}\r\n"); // longitude
                        sb.Append($"${latStr.Length}\r\n{latStr}\r\n"); // latitude
                    }
                    else
                    {
                        // Member doesn't exist, return null array
                        sb.Append("*-1\r\n");
                    }
                }
            }
        }

        return sb.ToString();
    }

    // Haversine formula to calculate distance between two geographic points
    static double CalculateHaversineDistance(double lat1, double lon1, double lat2, double lon2)
    {
        const double EarthRadiusMeters = 6372797.560856; // Earth's radius in meters (Redis value)

        // Convert degrees to radians
        double lat1Rad = lat1 * Math.PI / 180.0;
        double lat2Rad = lat2 * Math.PI / 180.0;
        double deltaLatRad = (lat2 - lat1) * Math.PI / 180.0;
        double deltaLonRad = (lon2 - lon1) * Math.PI / 180.0;

        // Haversine formula
        double a = Math.Sin(deltaLatRad / 2.0) * Math.Sin(deltaLatRad / 2.0) + Math.Cos(lat1Rad) * Math.Cos(lat2Rad) *
            Math.Sin(deltaLonRad / 2.0) * Math.Sin(deltaLonRad / 2.0);

        double c = 2.0 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1.0 - a));
        double distance = EarthRadiusMeters * c;

        return distance;
    }

    static string HandleGeodist(List<string> args)
    {
        // GEODIST key member1 member2 [M|KM|FT|MI]
        if (args.Count < 4 || args.Count > 5) return "-ERR wrong number of arguments for 'GEODIST'\r\n";

        string key = args[1];
        string member1 = args[2];
        string member2 = args[3];

        // Default unit is meters, can be specified as M, KM, FT, MI
        string unit = "m"; // meters
        if (args.Count == 5)
        {
            unit = args[4].ToLowerInvariant();
            if (unit != "m" && unit != "km" && unit != "ft" && unit != "mi")
                return "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n";
        }

        lock (sortedSetLock)
        {
            // Check if key exists
            if (!sortedSets.TryGetValue(key, out var members)) return "$-1\r\n"; // Key doesn't exist

            // Check if both members exist
            if (!members.TryGetValue(member1, out double score1) || !members.TryGetValue(member2, out double score2))
                return "$-1\r\n"; // At least one member doesn't exist

            // Decode both scores to coordinates
            var (lat1, lon1) = DecodeGeohash((long)score1);
            var (lat2, lon2) = DecodeGeohash((long)score2);

            // Calculate distance in meters
            double distanceMeters = CalculateHaversineDistance(lat1, lon1, lat2, lon2);

            // Convert to requested unit if necessary
            double distance = unit switch
            {
                "m" => distanceMeters,
                "km" => distanceMeters / 1000.0,
                "ft" => distanceMeters * 3.28084,
                "mi" => distanceMeters / 1609.344,
                _ => distanceMeters
            };

            // Format distance with proper precision
            string distanceStr = distance.ToString("F4", CultureInfo.InvariantCulture);

            // Return as RESP bulk string
            return $"${distanceStr.Length}\r\n{distanceStr}\r\n";
        }
    }

    static string HandleGeosearch(List<string> args)
    {
        // GEOSEARCH key FROMLONLAT <longitude> <latitude> BYRADIUS <radius> <unit>
        if (args.Count < 8) return "-ERR wrong number of arguments for 'GEOSEARCH'\r\n";

        string key = args[1];

        // Parse FROMLONLAT option
        if (!args[2].Equals("FROMLONLAT", StringComparison.OrdinalIgnoreCase)) return "-ERR syntax error\r\n";

        if (!double.TryParse(args[3], NumberStyles.Float, CultureInfo.InvariantCulture, out double centerLon))
            return "-ERR longitude is not a valid double\r\n";

        if (!double.TryParse(args[4], NumberStyles.Float, CultureInfo.InvariantCulture, out double centerLat))
            return "-ERR latitude is not a valid double\r\n";

        // Parse BYRADIUS option
        if (!args[5].Equals("BYRADIUS", StringComparison.OrdinalIgnoreCase)) return "-ERR syntax error\r\n";

        if (!double.TryParse(args[6], NumberStyles.Float, CultureInfo.InvariantCulture, out double radius))
            return "-ERR radius is not a valid double\r\n";

        if (radius < 0) return "-ERR radius must be non-negative\r\n";

        string unit = args[7].ToLowerInvariant();
        if (unit != "m" && unit != "km" && unit != "ft" && unit != "mi")
            return "-ERR unsupported unit provided. please use M, KM, FT, MI\r\n";

        // Convert radius to meters
        double radiusMeters = unit switch
        {
            "m" => radius, // Convert to meters
            "km" => radius * 1000.0, // Convert to kilometers
            "ft" => radius / 3.28084, // Convert to foot
            "mi" => radius * 1609.344, // Convert to mile
            _ => radius
        };

        List<string> results = new List<string>();

        lock (sortedSetLock)
        {
            // Check if key exists
            if (!sortedSets.TryGetValue(key, out var members)) return "*0\r\n"; // Empty array if key doesn't exist

            // Iterate through all members and check distance
            foreach (var kvp in members)
            {
                string member = kvp.Key;
                double score = kvp.Value;

                // Decode geohash score to coordinates
                var (memberLat, memberLon) = DecodeGeohash((long)score);

                // Calculate distance from center point
                double distance = CalculateHaversineDistance(centerLat, centerLon, memberLat, memberLon);

                // If within radius, add to results
                if (distance <= radiusMeters) results.Add(member);
            }
        }

        // Build RESP array response
        StringBuilder sb = new StringBuilder();
        sb.Append($"*{results.Count}\r\n");
        foreach (string member in results)
        {
            sb.Append($"${member.Length}\r\n{member}\r\n");
        }

        return sb.ToString();
    }

    static string HandleRPush(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'RPUSH'\r\n";
        int n;
        lock (listLock)
        {
            if (!listStore.ContainsKey(args[1])) listStore[args[1]] = new List<string>();
            for (int i = 2; i < args.Count; i++) listStore[args[1]].Add(args[i]);
            n = listStore[args[1]].Count;
            ServeBlockedClientsFromList(args[1]);
            // Increment version for this key
            lock (versionLock)
            {
                keyVersions[args[1]] = ++globalVersion;
            }
        }

        return $":{n}\r\n";
    }

    static string HandleLPush(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'LPUSH'\r\n";
        int n;
        lock (listLock)
        {
            if (!listStore.ContainsKey(args[1])) listStore[args[1]] = new List<string>();
            for (int i = 2; i < args.Count; i++) listStore[args[1]].Insert(0, args[i]);
            n = listStore[args[1]].Count;
            ServeBlockedClientsFromList(args[1]);
            // Increment version for this key
            lock (versionLock)
            {
                keyVersions[args[1]] = ++globalVersion;
            }
        }

        return $":{n}\r\n";
    }

    static string HandleLRange(List<string> args)
    {
        if (args.Count != 4) return "-ERR wrong number of arguments for 'LRANGE'\r\n";
        if (!int.TryParse(args[2], out int start) || !int.TryParse(args[3], out int stop))
            return "-ERR value is not an integer or out of range\r\n";
        List<string> snap;
        lock (listLock)
        {
            if (!listStore.TryGetValue(args[1], out List<string>? list) || list.Count == 0) return "*0\r\n";
            snap = new List<string>(list);
        }

        int cnt = snap.Count;
        if (start < 0) start = cnt + start;
        if (stop < 0) stop = cnt + stop;
        if (start < 0) start = 0;
        if (stop < 0) stop = 0;
        if (start >= cnt || start > stop) return "*0\r\n";
        int end = Math.Min(stop, cnt - 1);
        StringBuilder sb = new StringBuilder();
        sb.Append($"*{end - start + 1}\r\n");
        for (int i = start; i <= end; i++) sb.Append($"${snap[i].Length}\r\n{snap[i]}\r\n");
        return sb.ToString();
    }

    static string HandleLLen(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'LLEN'\r\n";
        int len;
        lock (listLock)
        {
            len = listStore.TryGetValue(args[1], out List<string>? l) ? l.Count : 0;
        }

        return $":{len}\r\n";
    }

    static string HandleLPop(List<string> args)
    {
        if (args.Count < 2 || args.Count > 3) return "-ERR wrong number of arguments for 'LPOP'\r\n";
        string key = args[1];
        if (args.Count == 2)
        {
            lock (listLock)
            {
                if (!listStore.TryGetValue(key, out List<string>? sl) || sl.Count == 0) return "$-1\r\n";
                string v = sl[0];
                sl.RemoveAt(0);
                // Increment version for this key
                lock (versionLock)
                {
                    keyVersions[key] = ++globalVersion;
                }

                return $"${v.Length}\r\n{v}\r\n";
            }
        }

        if (!int.TryParse(args[2], out int cnt)) return "-ERR value is not an integer or out of range\r\n";
        lock (listLock)
        {
            if (cnt <= 0 || !listStore.TryGetValue(key, out List<string>? list) || list.Count == 0) return "*0\r\n";
            int n = Math.Min(cnt, list.Count);
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{n}\r\n");
            for (int i = 0; i < n; i++)
            {
                string v = list[0];
                list.RemoveAt(0);
                sb.Append($"${v.Length}\r\n{v}\r\n");
            }

            // Increment version for this key
            lock (versionLock)
            {
                keyVersions[key] = ++globalVersion;
            }

            return sb.ToString();
        }
    }

    static string HandleBLPop(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'BLPOP'\r\n";
        string key = args[1];
        if (!double.TryParse(args[2], NumberStyles.Float, CultureInfo.InvariantCulture, out double timeout) ||
            timeout < 0)
            return "-ERR timeout is not a float or out of range\r\n";
        BlockedPopRequest req = new BlockedPopRequest();
        lock (listLock)
        {
            if (listStore.TryGetValue(key, out List<string>? list) && list.Count > 0)
            {
                string v = list[0];
                list.RemoveAt(0);
                return EncodeKeyValueArray(key, v);
            }

            if (!blockedPopWaiters.ContainsKey(key)) blockedPopWaiters[key] = new Queue<BlockedPopRequest>();
            blockedPopWaiters[key].Enqueue(req);
        }

        bool sig = timeout == 0 ? req.Signal.WaitOne() : req.Signal.WaitOne(TimeSpan.FromSeconds(timeout));
        if (sig) return EncodeKeyValueArray(key, req.Value ?? string.Empty);
        lock (listLock)
        {
            if (req.IsCompleted) return EncodeKeyValueArray(key, req.Value ?? string.Empty);
            RemoveBlockedRequest(key, req);
        }

        return "*-1\r\n";
    }

    static string HandleType(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'TYPE'\r\n";
        string key = args[1];
        if (store.ContainsKey(key))
        {
            var (_, ex) = store[key];
            if (ex.HasValue && DateTime.UtcNow > ex.Value)
            {
                store.Remove(key);
                return "+none\r\n";
            }

            return "+string\r\n";
        }

        lock (listLock)
        {
            if (listStore.ContainsKey(key)) return "+list\r\n";
        }

        lock (streamLock)
        {
            if (streamStore.ContainsKey(key)) return "+stream\r\n";
        }

        lock (sortedSetLock)
        {
            if (sortedSets.ContainsKey(key)) return "+zset\r\n";
        }

        return "+none\r\n";
    }

    static string HandleXAdd(List<string> args)
    {
        if (args.Count < 5 || ((args.Count - 3) % 2 != 0)) return "-ERR wrong number of arguments for 'XADD'\r\n";
        string key = args[1], id = args[2], finalId = id;
        lock (listLock)
        {
            if (listStore.ContainsKey(key))
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }

        if (store.ContainsKey(key)) return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        if (id == "*")
        {
            lock (streamLock)
            {
                if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries))
                {
                    entries = new List<StreamEntry>();
                    streamStore[key] = entries;
                }

                ulong ms = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), seq = 0;
                if (entries.Count > 0)
                {
                    string[] lp = entries[^1].Id.Split('-');
                    ulong lms = ulong.Parse(lp[0]), lseq = ulong.Parse(lp[1]);
                    if (ms <= lms)
                    {
                        ms = lms;
                        seq = lseq + 1;
                    }
                }

                finalId = $"{ms}-{seq}";
                StreamEntry e = new StreamEntry { Id = finalId };
                for (int i = 3; i < args.Count; i += 2)
                    e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i + 1]));
                entries.Add(e);
                ServeBlockedXReadClients(key, entries);
            }

            return $"${finalId.Length}\r\n{finalId}\r\n";
        }

        if (id.EndsWith("-*"))
        {
            if (!ulong.TryParse(id.Substring(0, id.Length - 2), out ulong ms))
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            lock (streamLock)
            {
                if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries))
                {
                    entries = new List<StreamEntry>();
                    streamStore[key] = entries;
                }

                ulong seq = 0;
                if (entries.Count > 0)
                {
                    string[] lp = entries[^1].Id.Split('-');
                    ulong lms = ulong.Parse(lp[0]), lseq = ulong.Parse(lp[1]);
                    if (ms < lms)
                        return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    seq = ms == lms ? lseq + 1 : (ulong)(ms == 0 ? 1 : 0);
                }
                else
                    seq = (ulong)(ms == 0 ? 1 : 0);

                finalId = $"{ms}-{seq}";
                StreamEntry e = new StreamEntry { Id = finalId };
                for (int i = 3; i < args.Count; i += 2)
                    e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i + 1]));
                entries.Add(e);
                ServeBlockedXReadClients(key, entries);
            }

            return $"${finalId.Length}\r\n{finalId}\r\n";
        }

        if (!TryParseExplicitStreamId(id, out ulong ems, out ulong eseq))
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        if (ems == 0 && eseq == 0) return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        lock (streamLock)
        {
            if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries))
            {
                entries = new List<StreamEntry>();
                streamStore[key] = entries;
            }

            if (entries.Count > 0 && CompareStreamIds(finalId, entries[^1].Id) <= 0)
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            StreamEntry e = new StreamEntry { Id = finalId };
            for (int i = 3; i < args.Count; i += 2)
                e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i + 1]));
            entries.Add(e);
            ServeBlockedXReadClients(key, entries);
        }

        return $"${finalId.Length}\r\n{finalId}\r\n";
    }

    static string HandleXRange(List<string> args)
    {
        if (args.Count != 4) return "-ERR wrong number of arguments for 'XRANGE'\r\n";
        if (!TryParseStreamRangeBound(args[2], true, out string startId) ||
            !TryParseStreamRangeBound(args[3], false, out string endId))
            return "-ERR Invalid stream ID specified as range\r\n";
        lock (streamLock)
        {
            if (!streamStore.TryGetValue(args[1], out List<StreamEntry>? entries) || entries.Count == 0)
                return "*0\r\n";
            List<StreamEntry> match = entries.FindAll(e =>
                CompareStreamIds(e.Id, startId) >= 0 && CompareStreamIds(e.Id, endId) <= 0);
            if (match.Count == 0) return "*0\r\n";
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{match.Count}\r\n");
            foreach (StreamEntry e in match)
            {
                sb.Append("*2\r\n");
                sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n");
                sb.Append($"*{e.Fields.Count * 2}\r\n");
                foreach (var f in e.Fields)
                {
                    sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n");
                    sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n");
                }
            }

            return sb.ToString();
        }
    }

    static string HandleXRead(List<string> args)
    {
        if (args.Count < 4) return "-ERR wrong number of arguments for 'XREAD'\r\n";
        bool isBlocking = false;
        int timeoutMs = 0;
        int si = 1;
        if (args[1].Equals("BLOCK", StringComparison.OrdinalIgnoreCase))
        {
            if (args.Count < 5 || !int.TryParse(args[2], out timeoutMs) || timeoutMs < 0)
                return "-ERR timeout is not an integer or out of range\r\n";
            isBlocking = true;
            si = 3;
        }

        if (!args[si].Equals("STREAMS", StringComparison.OrdinalIgnoreCase)) return "-ERR syntax error\r\n";
        int tac = args.Count - (si + 1);
        if (tac % 2 != 0) return "-ERR syntax error\r\n";
        int sc = tac / 2;
        if (sc == 0) return "-ERR wrong number of arguments for 'XREAD'\r\n";
        List<string> keys = new List<string>(sc), sids = new List<string>(sc), rawIds = new List<string>(sc);
        for (int i = 0; i < sc; i++) keys.Add(args[si + 1 + i]);
        for (int i = 0; i < sc; i++) rawIds.Add(args[si + 1 + sc + i]);
        bool hasDollar = rawIds.Exists(id => id == "$");
        if (!hasDollar)
        {
            for (int i = 0; i < sc; i++)
            {
                if (!TryParseXReadId(rawIds[i], out string sid))
                    return "-ERR Invalid stream ID specified as stream command argument\r\n";
                sids.Add(sid);
            }

            string imm = BuildXReadResponse(keys, sids, sc);
            if (imm != "*0\r\n") return imm;
        }

        if (!isBlocking) return "*0\r\n";
        string bk = keys[0];
        BlockedXReadRequest req;
        lock (streamLock)
        {
            if (hasDollar)
            {
                sids.Clear();
                for (int i = 0; i < sc; i++)
                {
                    if (rawIds[i] == "$")
                    {
                        string lid = "0-0";
                        if (streamStore.TryGetValue(keys[i], out List<StreamEntry>? ex) && ex.Count > 0)
                            lid = ex[^1].Id;
                        sids.Add(lid);
                    }
                    else
                    {
                        if (!TryParseXReadId(rawIds[i], out string p2))
                            return "-ERR Invalid stream ID specified as stream command argument\r\n";
                        sids.Add(p2);
                    }
                }
            }
            else
            {
                string rc = BuildXReadResponse(keys, sids, sc);
                if (rc != "*0\r\n") return rc;
            }

            req = new BlockedXReadRequest(sids[0]);
            if (!blockedXReadWaiters.ContainsKey(bk)) blockedXReadWaiters[bk] = new List<BlockedXReadRequest>();
            blockedXReadWaiters[bk].Add(req);
        }

        bool sig = timeoutMs == 0 ? req.Signal.WaitOne() : req.Signal.WaitOne(timeoutMs);
        if (!sig)
        {
            lock (streamLock)
            {
                if (!req.IsCompleted && blockedXReadWaiters.TryGetValue(bk, out List<BlockedXReadRequest>? wl))
                {
                    wl.Remove(req);
                    if (wl.Count == 0) blockedXReadWaiters.Remove(bk);
                }
            }

            if (!req.IsCompleted) return "*-1\r\n";
        }

        if (req.NewEntries == null || req.NewEntries.Count == 0) return "*-1\r\n";
        return BuildXReadResponseFromEntries(bk, req.NewEntries);
    }

    static string BuildXReadResponse(List<string> keys, List<string> startIds, int sc)
    {
        lock (streamLock)
        {
            List<(string, List<StreamEntry>)> res = new List<(string, List<StreamEntry>)>();
            for (int i = 0; i < sc; i++)
            {
                if (!streamStore.TryGetValue(keys[i], out List<StreamEntry>? se) || se.Count == 0) continue;
                List<StreamEntry> m = se.FindAll(e => CompareStreamIds(e.Id, startIds[i]) > 0);
                if (m.Count > 0) res.Add((keys[i], m));
            }

            if (res.Count == 0) return "*0\r\n";
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{res.Count}\r\n");
            foreach ((string k, List<StreamEntry> ents) in res)
            {
                sb.Append("*2\r\n");
                sb.Append($"${k.Length}\r\n{k}\r\n");
                sb.Append($"*{ents.Count}\r\n");
                foreach (StreamEntry e in ents)
                {
                    sb.Append("*2\r\n");
                    sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n");
                    sb.Append($"*{e.Fields.Count * 2}\r\n");
                    foreach (var f in e.Fields)
                    {
                        sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n");
                        sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n");
                    }
                }
            }

            return sb.ToString();
        }
    }

    static string BuildXReadResponseFromEntries(string key, List<StreamEntry> entries)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("*1\r\n*2\r\n");
        sb.Append($"${key.Length}\r\n{key}\r\n");
        sb.Append($"*{entries.Count}\r\n");
        foreach (StreamEntry e in entries)
        {
            sb.Append("*2\r\n");
            sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n");
            sb.Append($"*{e.Fields.Count * 2}\r\n");
            foreach (var f in e.Fields)
            {
                sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n");
                sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n");
            }
        }

        return sb.ToString();
    }

    static void ServeBlockedXReadClients(string key, List<StreamEntry> all)
    {
        if (!blockedXReadWaiters.TryGetValue(key, out List<BlockedXReadRequest>? ws) || ws.Count == 0) return;
        List<BlockedXReadRequest> done = new List<BlockedXReadRequest>();
        foreach (BlockedXReadRequest w in ws)
        {
            List<StreamEntry> ne = all.FindAll(e => CompareStreamIds(e.Id, w.StartIdExclusive) > 0);
            if (ne.Count > 0)
            {
                w.NewEntries = ne;
                w.IsCompleted = true;
                w.Signal.Set();
                done.Add(w);
            }
        }

        foreach (BlockedXReadRequest w in done) ws.Remove(w);
        if (ws.Count == 0) blockedXReadWaiters.Remove(key);
    }

    static bool TryParseExplicitStreamId(string id, out ulong ms, out ulong seq)
    {
        ms = seq = 0;
        if (id == "*") return false;
        string[] p = id.Split('-');
        return p.Length == 2 && ulong.TryParse(p[0], out ms) && ulong.TryParse(p[1], out seq);
    }

    static bool TryParseStreamRangeBound(string input, bool isStart, out string norm)
    {
        norm = string.Empty;
        if (isStart && input == "-")
        {
            norm = "0-0";
            return true;
        }

        if (!isStart && input == "+")
        {
            norm = $"{ulong.MaxValue}-{ulong.MaxValue}";
            return true;
        }

        string[] p = input.Split('-');
        if (p.Length == 1 && ulong.TryParse(p[0], out ulong ms1))
        {
            norm = $"{ms1}-{(isStart ? 0 : ulong.MaxValue)}";
            return true;
        }

        if (p.Length == 2 && ulong.TryParse(p[0], out ulong ms2) && ulong.TryParse(p[1], out ulong seq))
        {
            norm = $"{ms2}-{seq}";
            return true;
        }

        return false;
    }

    static bool TryParseXReadId(string input, out string norm)
    {
        norm = string.Empty;
        string[] p = input.Split('-');
        if (p.Length == 1 && ulong.TryParse(p[0], out ulong ms))
        {
            norm = $"{ms}-0";
            return true;
        }

        if (p.Length == 2 && ulong.TryParse(p[0], out ulong ms2) && ulong.TryParse(p[1], out ulong seq))
        {
            norm = $"{ms2}-{seq}";
            return true;
        }

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
        while (list.Count > 0 && ws.Count > 0)
        {
            BlockedPopRequest w = ws.Dequeue();
            string v = list[0];
            list.RemoveAt(0);
            w.Value = v;
            w.IsCompleted = true;
            w.Signal.Set();
        }

        if (ws.Count == 0) blockedPopWaiters.Remove(key);
    }

    static string EncodeKeyValueArray(string key, string value) =>
        $"*2\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n";

    static void RemoveBlockedRequest(string key, BlockedPopRequest req)
    {
        if (!blockedPopWaiters.TryGetValue(key, out Queue<BlockedPopRequest>? ws)) return;
        Queue<BlockedPopRequest> f = new Queue<BlockedPopRequest>();
        while (ws.Count > 0)
        {
            var c = ws.Dequeue();
            if (!ReferenceEquals(c, req)) f.Enqueue(c);
        }

        if (f.Count == 0)
            blockedPopWaiters.Remove(key);
        else
            blockedPopWaiters[key] = f;
    }

    static List<string> ParseRESP(string input)
    {
        List<string> args = new List<string>();
        string[] lines = input.Split(new[] { "\r\n" }, StringSplitOptions.None);
        int i = 0;
        while (i < lines.Length)
        {
            if (lines[i].StartsWith("*"))
            {
                i++;
            }
            else if (lines[i].StartsWith("$"))
            {
                i++;
                if (i < lines.Length)
                {
                    args.Add(lines[i]);
                    i++;
                }
            }
            else
            {
                i++;
            }
        }

        return args;
    }
}