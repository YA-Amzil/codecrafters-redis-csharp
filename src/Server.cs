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
    static TcpClient? masterClient;
    static NetworkStream? masterStream;
    static readonly string masterReplId = GenerateReplicationId();
    static readonly long masterReplOffset = 0;
    static readonly byte[] EmptyRdbBytes = LoadEmptyRdbBytes();
    static readonly object replicaLock = new object();
    static readonly List<Socket> replicaSockets = new List<Socket>();
    static readonly object listLock = new object();
    static readonly object streamLock = new object();
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

    static byte[] LoadEmptyRdbBytes()
    {
        string[] candidatePaths =
        {
            Path.Combine(AppContext.BaseDirectory, "empty_rdb.base64"),
            Path.Combine(AppContext.BaseDirectory, "src", "empty_rdb.base64")
        };

        foreach (string path in candidatePaths)
        {
            if (File.Exists(path))
                return Convert.FromBase64String(File.ReadAllText(path).Trim());
        }

        // Fallback for environments that don't copy extra files into the build output.
        return Convert.FromBase64String("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
    }

    static string SendAndReceive(NetworkStream stream, string message)
    {
        byte[] data = Encoding.UTF8.GetBytes(message);
        stream.Write(data, 0, data.Length);
        stream.Flush();

        byte[] buffer = new byte[512];
        int bytesRead = stream.Read(buffer, 0, buffer.Length);
        return Encoding.UTF8.GetString(buffer, 0, bytesRead);
    }

    static void SendOnly(NetworkStream stream, string message)
    {
        byte[] data = Encoding.UTF8.GetBytes(message);
        stream.Write(data, 0, data.Length);
        stream.Flush();
    }

    static string ReadRespLine(NetworkStream stream)
    {
        List<byte> bytes = new List<byte>();
        while (true)
        {
            int b = stream.ReadByte();
            if (b < 0) throw new IOException("Connection closed while reading from master");
            bytes.Add((byte)b);
            int c = bytes.Count;
            if (c >= 2 && bytes[c - 2] == (byte)'\r' && bytes[c - 1] == (byte)'\n') break;
        }
        return Encoding.UTF8.GetString(bytes.ToArray(), 0, bytes.Count - 2);
    }

    static void ReadExact(NetworkStream stream, byte[] buffer, int length)
    {
        int read = 0;
        while (read < length)
        {
            int n = stream.Read(buffer, read, length - read);
            if (n <= 0) throw new IOException("Connection closed while reading from master");
            read += n;
        }
    }

    static void ReceiveFullResyncAndRdb(NetworkStream stream)
    {
        string fullResyncLine = ReadRespLine(stream);
        if (!fullResyncLine.StartsWith("+FULLRESYNC", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected PSYNC response from master: {fullResyncLine}");

        string rdbHeader = ReadRespLine(stream);
        if (!rdbHeader.StartsWith("$", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected RDB bulk header from master: {rdbHeader}");

        if (!int.TryParse(rdbHeader.Substring(1), out int rdbLength) || rdbLength < 0)
            throw new InvalidOperationException($"Invalid RDB length from master: {rdbHeader}");

        byte[] rdbBytes = new byte[rdbLength];
        ReadExact(stream, rdbBytes, rdbLength);
    }

    static int FindCrlf(List<byte> buffer, int start)
    {
        for (int i = start; i + 1 < buffer.Count; i++)
        {
            if (buffer[i] == (byte)'\r' && buffer[i + 1] == (byte)'\n') return i;
        }
        return -1;
    }

    static bool TryParseRespArray(List<byte> buffer, out List<string> args, out int consumed)
    {
        args = new List<string>();
        consumed = 0;
        if (buffer.Count == 0) return false;
        if (buffer[0] != (byte)'*') return false;

        int lineEnd = FindCrlf(buffer, 0);
        if (lineEnd < 0) return false;
        if (!int.TryParse(Encoding.UTF8.GetString(buffer.GetRange(1, lineEnd - 1).ToArray()), out int count) || count < 0)
            throw new InvalidOperationException("Invalid RESP array length from master");

        int index = lineEnd + 2;
        for (int i = 0; i < count; i++)
        {
            if (index >= buffer.Count) return false;
            if (buffer[index] != (byte)'$') throw new InvalidOperationException("Invalid RESP bulk string from master");

            int lenEnd = FindCrlf(buffer, index);
            if (lenEnd < 0) return false;
            if (!int.TryParse(Encoding.UTF8.GetString(buffer.GetRange(index + 1, lenEnd - (index + 1)).ToArray()), out int len) || len < 0)
                throw new InvalidOperationException("Invalid RESP bulk length from master");

            index = lenEnd + 2;
            if (buffer.Count < index + len + 2) return false;

            string value = Encoding.UTF8.GetString(buffer.GetRange(index, len).ToArray());
            args.Add(value);
            index += len;

            if (buffer[index] != (byte)'\r' || buffer[index + 1] != (byte)'\n')
                throw new InvalidOperationException("Invalid RESP bulk terminator from master");
            index += 2;
        }

        consumed = index;
        return true;
    }

    static void ProcessMasterReplicationStream(NetworkStream stream)
    {
        byte[] readBuffer = new byte[4096];
        List<byte> pending = new List<byte>();

        while (true)
        {
            int n = stream.Read(readBuffer, 0, readBuffer.Length);
            if (n <= 0) break;
            for (int i = 0; i < n; i++) pending.Add(readBuffer[i]);

            while (TryParseRespArray(pending, out List<string> cmdArgs, out int consumed))
            {
                pending.RemoveRange(0, consumed);
                if (cmdArgs.Count == 0) continue;
                DispatchCommand(cmdArgs[0].ToUpperInvariant(), cmdArgs);
            }
        }
    }

    static void SendAll(Socket socket, byte[] data)
    {
        int sent = 0;
        while (sent < data.Length)
            sent += socket.Send(data, sent, data.Length - sent, SocketFlags.None);
    }

    static void RegisterReplica(Socket socket)
    {
        lock (replicaLock)
        {
            if (!replicaSockets.Contains(socket)) replicaSockets.Add(socket);
        }
    }

    static void UnregisterReplica(Socket socket)
    {
        lock (replicaLock)
        {
            replicaSockets.Remove(socket);
        }
    }

    static string BuildRespArray(List<string> args)
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"*{args.Count}\r\n");
        foreach (string arg in args) sb.Append($"${arg.Length}\r\n{arg}\r\n");
        return sb.ToString();
    }

    static bool ShouldPropagate(string command, string response)
    {
        return command is "SET" or "INCR" or "RPUSH" or "LPUSH" or "LPOP" or "XADD"
            && !response.StartsWith("-", StringComparison.Ordinal)
            && response != "$-1\r\n"
            && response != "*0\r\n";
    }

    static void PropagateWriteCommand(List<string> args)
    {
        byte[] payload = Encoding.UTF8.GetBytes(BuildRespArray(args));
        lock (replicaLock)
        {
            for (int i = replicaSockets.Count - 1; i >= 0; i--)
            {
                Socket replica = replicaSockets[i];
                try { SendAll(replica, payload); }
                catch { replicaSockets.RemoveAt(i); }
            }
        }
    }

    static void ConnectToMaster(string host, int masterPort, int listeningPort)
    {
        // Keep this connection open for the replication handshake and next stages.
        masterClient = new TcpClient();
        masterClient.Connect(host, masterPort);
        masterStream = masterClient.GetStream();

        string pong = SendAndReceive(masterStream, "*1\r\n$4\r\nPING\r\n");
        if (!pong.StartsWith("+PONG", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected PING response from master: {pong.Trim()}");

        string portStr = listeningPort.ToString();
        string replConfPort = SendAndReceive(
            masterStream,
            $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${portStr.Length}\r\n{portStr}\r\n");
        if (!replConfPort.StartsWith("+OK", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected REPLCONF listening-port response from master: {replConfPort.Trim()}");

        string replConfCapa = SendAndReceive(
            masterStream,
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        if (!replConfCapa.StartsWith("+OK", StringComparison.Ordinal))
            throw new InvalidOperationException($"Unexpected REPLCONF capa response from master: {replConfCapa.Trim()}");

        // Handshake step 3: request full synchronization from an unknown offset.
        SendOnly(masterStream, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        ReceiveFullResyncAndRdb(masterStream);

        // Replica applies propagated commands silently; no responses are sent back.
        Thread replicaThread = new Thread(() => ProcessMasterReplicationStream(masterStream));
        replicaThread.IsBackground = true;
        replicaThread.Start();
    }

    // ── HandlePort ─────────────────────────────────────────────────────────
    // Configure listening port
    static void Main(string[] args)
    {
        int port = 6379; // default port
    
        // Parse command-line arguments for --port and --replicaof flags
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" && i + 1 < args.Length)
            {
                if (int.TryParse(args[i + 1], out int parsedPort))
                {
                    port = parsedPort;
                    i++; // skip the port value we just consumed
                }
                else
                {
                    Console.WriteLine("Error: --port requires a valid integer argument");
                    return;
                }
            }
            else if (args[i] == "--replicaof" && i + 1 < args.Length)
            {
                isReplica = true;
                string[] replicaParts = args[i + 1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (replicaParts.Length >= 2 && int.TryParse(replicaParts[1], out int parsedReplicaPort))
                {
                    replicaHost = replicaParts[0];
                    replicaPort = parsedReplicaPort;
                }
                else
                {
                    Console.WriteLine("Error: --replicaof requires 'host port'");
                    return;
                }
                i++; // skip the replica target value (e.g. "localhost 6379")
            }
        }
    
        if (isReplica)
        {
            ConnectToMaster(replicaHost, replicaPort, port);
        }
    
        TcpListener server = new TcpListener(IPAddress.Any, port);
        server.Start();
        Console.WriteLine($"Redis server started on port {port}...");

        while (true)
        {
            try
            {
                Socket client = server.AcceptSocket();
                Console.WriteLine("Client connected.");
                Thread clientThread = new Thread(() => HandleClient(client));
                clientThread.Start();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Server error: {ex.Message}");
                break;
            }
        }

        server.Stop();
        Console.WriteLine("Server stopped.");
    }

    // ── HandleClient ─────────────────────────────────────────────────────────
    // Each connection gets its own ClientState. MULTI/EXEC/DISCARD are handled
    // here directly. Every other command is either queued (when InTransaction)
    // or dispatched immediately via DispatchCommand.
    static void HandleClient(Socket client)
    {
        byte[] buffer = new byte[1024];
        ClientState state = new ClientState();   // fresh state per connection

        while (true)
        {
            try
            {
                int bytesReceived = client.Receive(buffer);
                if (bytesReceived == 0) { Console.WriteLine("Client disconnected."); break; }

                string input = Encoding.UTF8.GetString(buffer, 0, bytesReceived);
                Console.WriteLine($"Received raw input:\n{input}");

                List<string> args = ParseRESP(input);
                if (args.Count == 0) continue;

                string command = args[0].ToUpperInvariant();
                string response;

                // ── MULTI ────────────────────────────────────────────────────
                // Starts a transaction. Subsequent commands are queued, not run.
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
                // ── EXEC ─────────────────────────────────────────────────────
                // Executes all queued commands and returns an array of results.
                else if (command == "EXEC")
                {
                    response = HandleExec(state);
                }
                // ── DISCARD ──────────────────────────────────────────────────
                // Cancels the transaction and empties the queue.
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
                // ── Inside a transaction: queue the command ───────────────────
                // The command is not executed now. Redis replies "+QUEUED" to
                // let the client know it has been accepted into the queue.
                else if (state.InTransaction)
                {
                    state.QueuedCommands.Add(args);
                    response = "+QUEUED\r\n";
                }
                // ── Normal command dispatch ───────────────────────────────────
                else
                {
                    if (command == "PSYNC")
                    {
                        response = HandlePSync(args);
                        SendAll(client, Encoding.UTF8.GetBytes(response));

                        if (response.StartsWith("+FULLRESYNC", StringComparison.Ordinal))
                        {
                            byte[] rdbHeader = Encoding.ASCII.GetBytes($"${EmptyRdbBytes.Length}\r\n");
                            SendAll(client, rdbHeader);
                            SendAll(client, EmptyRdbBytes);
                            RegisterReplica(client);
                        }

                        continue;
                    }

                    response = DispatchCommand(command, args);
                }

                client.Send(Encoding.UTF8.GetBytes(response));

                if (ShouldPropagate(command, response))
                    PropagateWriteCommand(args);
            }
            catch (SocketException) { Console.WriteLine("Client connection lost."); break; }
            catch (Exception ex) { Console.WriteLine($"Client error: {ex.Message}"); break; }
        }

        UnregisterReplica(client);
        client.Close();
    }

    // ── HandleExec ───────────────────────────────────────────────────────────
    // Runs every queued command in order and wraps all results in a RESP array.
    // Example: two queued commands → "*2\r\n<result1><result2>"
    static string HandleExec(ClientState state)
    {
        if (!state.InTransaction)
            return "-ERR EXEC without MULTI\r\n";

        List<string> results = new List<string>();
        foreach (List<string> queuedArgs in state.QueuedCommands)
            results.Add(DispatchCommand(queuedArgs[0].ToUpperInvariant(), queuedArgs));

        state.InTransaction = false;
        state.QueuedCommands.Clear();

        StringBuilder response = new StringBuilder();
        response.Append($"*{results.Count}\r\n");
        foreach (string result in results)
            response.Append(result);

        return response.ToString();
    }

    // ── DispatchCommand ───────────────────────────────────────────────────────
    // Single place that maps a command name to its handler.
    // Used both for immediate execution and inside EXEC.
    static string DispatchCommand(string command, List<string> args)
    {
        switch (command)
        {
            case "PING":   return "+PONG\r\n";
            case "ECHO":   return args.Count > 1 ? $"${args[1].Length}\r\n{args[1]}\r\n" : "$-1\r\n";
            case "REPLCONF": return "+OK\r\n";
            case "PSYNC":  return HandlePSync(args);
            case "SET":    return HandleSet(args);
            case "GET":    return HandleGet(args);
            case "INFO":   return HandleInfo(args);
            case "INCR":   return HandleIncr(args);
            case "RPUSH":  return HandleRPush(args);
            case "LPUSH":  return HandleLPush(args);
            case "LRANGE": return HandleLRange(args);
            case "LLEN":   return HandleLLen(args);
            case "LPOP":   return HandleLPop(args);
            case "BLPOP":  return HandleBLPop(args);
            case "TYPE":   return HandleType(args);
            case "XADD":   return HandleXAdd(args);
            case "XRANGE": return HandleXRange(args);
            case "XREAD":  return HandleXRead(args);
            default:       return "-ERR unknown command\r\n";
        }
    }

    // ── SET ───────────────────────────────────────────────────────────────────
    static string HandleSet(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'SET'\r\n";
        string key = args[1], value = args[2];
        DateTime? expiry = null;
        for (int i = 3; i < args.Count - 1; i++)
        {
            if (args[i].Equals("PX", StringComparison.OrdinalIgnoreCase))
            {
                if (int.TryParse(args[i + 1], out int ms)) { expiry = DateTime.UtcNow.AddMilliseconds(ms); i++; }
                else return "-ERR PX value must be an integer\r\n";
            }
        }
        store[key] = (value, expiry);
        return "+OK\r\n";
    }

    // ── GET ───────────────────────────────────────────────────────────────────
    static string HandleGet(List<string> args)
    {
        if (args.Count < 2) return "-ERR wrong number of arguments for 'GET'\r\n";
        string key = args[1];
        if (store.ContainsKey(key))
        {
            var (value, expiry) = store[key];
            if (expiry.HasValue && DateTime.UtcNow > expiry.Value) { store.Remove(key); return "$-1\r\n"; }
            return $"${value.Length}\r\n{value}\r\n";
        }
        return "$-1\r\n";
    }
    
    // ── INFO ───────────────────────────────────────────────────────────────────
    static string HandleInfo(List<string> args)
    {
        // INFO zonder argumenten is ook toegestaan, maar we focussen op 'replication'
        if (args.Count >= 2 && args[1].Equals("replication", StringComparison.OrdinalIgnoreCase))
        {
            string role = isReplica ? "slave" : "master";
            StringBuilder content = new StringBuilder();
            content.AppendLine($"role:{role}");
            content.AppendLine($"master_replid:{masterReplId}");
            content.AppendLine($"master_repl_offset:{masterReplOffset}");

            string body = content.ToString().TrimEnd('\r', '\n');
            return $"${body.Length}\r\n{body}\r\n";
        }
    
        // Voor andere secties of geen argument: stuur lege bulk string terug
        // (voor deze stage hoeven we andere secties nog niet te ondersteunen)
        return "$0\r\n\r\n";
    }

    static string HandlePSync(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'PSYNC'\r\n";
        if (args[1] != "?" || args[2] != "-1") return "-ERR unsupported PSYNC arguments\r\n";
        return $"+FULLRESYNC {masterReplId} {masterReplOffset}\r\n";
    }

    // ── INCR ──────────────────────────────────────────────────────────────────
    static string HandleIncr(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'INCR'\r\n";
        string key = args[1];
        if (store.TryGetValue(key, out var entry))
        {
            if (entry.expiry.HasValue && DateTime.UtcNow > entry.expiry.Value)
            {
                store.Remove(key);
                store[key] = ("1", null);
                return ":1\r\n";
            }
            if (!long.TryParse(entry.value, out long current))
                return "-ERR value is not an integer or out of range\r\n";
            long newValue = current + 1;
            store[key] = (newValue.ToString(), entry.expiry);
            return $":{newValue}\r\n";
        }
        store[key] = ("1", null);
        return ":1\r\n";
    }

    // ── RPUSH ─────────────────────────────────────────────────────────────────
    static string HandleRPush(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'RPUSH'\r\n";
        string key = args[1];
        int newLength;
        lock (listLock)
        {
            if (!listStore.ContainsKey(key)) listStore[key] = new List<string>();
            for (int i = 2; i < args.Count; i++) listStore[key].Add(args[i]);
            newLength = listStore[key].Count;
            ServeBlockedClientsFromList(key);
        }
        return $":{newLength}\r\n";
    }

    // ── LPUSH ─────────────────────────────────────────────────────────────────
    static string HandleLPush(List<string> args)
    {
        if (args.Count < 3) return "-ERR wrong number of arguments for 'LPUSH'\r\n";
        string key = args[1];
        int newLength;
        lock (listLock)
        {
            if (!listStore.ContainsKey(key)) listStore[key] = new List<string>();
            for (int i = 2; i < args.Count; i++) listStore[key].Insert(0, args[i]);
            newLength = listStore[key].Count;
            ServeBlockedClientsFromList(key);
        }
        return $":{newLength}\r\n";
    }

    // ── LRANGE ────────────────────────────────────────────────────────────────
    static string HandleLRange(List<string> args)
    {
        if (args.Count != 4) return "-ERR wrong number of arguments for 'LRANGE'\r\n";
        string key = args[1];
        if (!int.TryParse(args[2], out int start) || !int.TryParse(args[3], out int stop))
            return "-ERR value is not an integer or out of range\r\n";
        List<string> snapshot;
        lock (listLock)
        {
            if (!listStore.TryGetValue(key, out List<string>? list) || list.Count == 0) return "*0\r\n";
            snapshot = new List<string>(list);
        }
        int count = snapshot.Count;
        if (start < 0) start = count + start;
        if (stop < 0)  stop  = count + stop;
        if (start < 0) start = 0;
        if (stop < 0)  stop  = 0;
        if (start >= count || start > stop) return "*0\r\n";
        int end = Math.Min(stop, count - 1);
        StringBuilder sb = new StringBuilder();
        sb.Append($"*{end - start + 1}\r\n");
        for (int i = start; i <= end; i++) sb.Append($"${snapshot[i].Length}\r\n{snapshot[i]}\r\n");
        return sb.ToString();
    }

    // ── LLEN ──────────────────────────────────────────────────────────────────
    static string HandleLLen(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'LLEN'\r\n";
        int length;
        lock (listLock) { length = listStore.TryGetValue(args[1], out List<string>? list) ? list.Count : 0; }
        return $":{length}\r\n";
    }

    // ── LPOP ──────────────────────────────────────────────────────────────────
    static string HandleLPop(List<string> args)
    {
        if (args.Count < 2 || args.Count > 3) return "-ERR wrong number of arguments for 'LPOP'\r\n";
        string key = args[1];
        if (args.Count == 2)
        {
            lock (listLock)
            {
                if (!listStore.TryGetValue(key, out List<string>? sl) || sl.Count == 0) return "$-1\r\n";
                string v = sl[0]; sl.RemoveAt(0);
                return $"${v.Length}\r\n{v}\r\n";
            }
        }
        if (!int.TryParse(args[2], out int count)) return "-ERR value is not an integer or out of range\r\n";
        lock (listLock)
        {
            if (count <= 0 || !listStore.TryGetValue(key, out List<string>? list) || list.Count == 0) return "*0\r\n";
            int n = Math.Min(count, list.Count);
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{n}\r\n");
            for (int i = 0; i < n; i++) { string v = list[0]; list.RemoveAt(0); sb.Append($"${v.Length}\r\n{v}\r\n"); }
            return sb.ToString();
        }
    }

    // ── BLPOP ─────────────────────────────────────────────────────────────────
    static string HandleBLPop(List<string> args)
    {
        if (args.Count != 3) return "-ERR wrong number of arguments for 'BLPOP'\r\n";
        string key = args[1];
        if (!double.TryParse(args[2], NumberStyles.Float, CultureInfo.InvariantCulture, out double timeoutSeconds) || timeoutSeconds < 0)
            return "-ERR timeout is not a float or out of range\r\n";
        BlockedPopRequest request = new BlockedPopRequest();
        lock (listLock)
        {
            if (listStore.TryGetValue(key, out List<string>? list) && list.Count > 0)
            {
                string value = list[0]; list.RemoveAt(0);
                return EncodeKeyValueArray(key, value);
            }
            if (!blockedPopWaiters.ContainsKey(key)) blockedPopWaiters[key] = new Queue<BlockedPopRequest>();
            blockedPopWaiters[key].Enqueue(request);
        }
        bool signaled = timeoutSeconds == 0 ? request.Signal.WaitOne() : request.Signal.WaitOne(TimeSpan.FromSeconds(timeoutSeconds));
        if (signaled) return EncodeKeyValueArray(key, request.Value ?? string.Empty);
        lock (listLock)
        {
            if (request.IsCompleted) return EncodeKeyValueArray(key, request.Value ?? string.Empty);
            RemoveBlockedRequest(key, request);
        }
        return "*-1\r\n";
    }

    // ── TYPE ──────────────────────────────────────────────────────────────────
    static string HandleType(List<string> args)
    {
        if (args.Count != 2) return "-ERR wrong number of arguments for 'TYPE'\r\n";
        string key = args[1];
        if (store.ContainsKey(key))
        {
            var (_, expiry) = store[key];
            if (expiry.HasValue && DateTime.UtcNow > expiry.Value) { store.Remove(key); return "+none\r\n"; }
            return "+string\r\n";
        }
        lock (listLock)   { if (listStore.ContainsKey(key))   return "+list\r\n"; }
        lock (streamLock) { if (streamStore.ContainsKey(key)) return "+stream\r\n"; }
        return "+none\r\n";
    }

    // ── XADD ──────────────────────────────────────────────────────────────────
    static string HandleXAdd(List<string> args)
    {
        if (args.Count < 5 || ((args.Count - 3) % 2 != 0))
            return "-ERR wrong number of arguments for 'XADD'\r\n";
        string key = args[1], id = args[2], finalId = id;
        lock (listLock) { if (listStore.ContainsKey(key)) return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"; }
        if (store.ContainsKey(key)) return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        if (id == "*")
        {
            lock (streamLock)
            {
                if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries)) { entries = new List<StreamEntry>(); streamStore[key] = entries; }
                ulong ms = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), seq = 0;
                if (entries.Count > 0)
                {
                    string[] lp = entries[^1].Id.Split('-');
                    ulong lms = ulong.Parse(lp[0]), lseq = ulong.Parse(lp[1]);
                    if (ms <= lms) { ms = lms; seq = lseq + 1; }
                }
                finalId = $"{ms}-{seq}";
                StreamEntry e = new StreamEntry { Id = finalId };
                for (int i = 3; i < args.Count; i += 2) e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i + 1]));
                entries.Add(e);
                ServeBlockedXReadClients(key, entries);
            }
            return $"${finalId.Length}\r\n{finalId}\r\n";
        }

        if (id.EndsWith("-*", StringComparison.Ordinal))
        {
            string msPart = id.Substring(0, id.Length - 2);
            if (!ulong.TryParse(msPart, out ulong ms)) return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            lock (streamLock)
            {
                if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries)) { entries = new List<StreamEntry>(); streamStore[key] = entries; }
                ulong seq = 0;
                if (entries.Count > 0)
                {
                    string[] lp = entries[^1].Id.Split('-');
                    ulong lms = ulong.Parse(lp[0]), lseq = ulong.Parse(lp[1]);
                    if (ms < lms) return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    seq = ms == lms ? lseq + 1 : (ulong)(ms == 0 ? 1 : 0);
                }
                else seq = (ulong)(ms == 0 ? 1 : 0);
                finalId = $"{ms}-{seq}";
                StreamEntry e = new StreamEntry { Id = finalId };
                for (int i = 3; i < args.Count; i += 2) e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i + 1]));
                entries.Add(e);
                ServeBlockedXReadClients(key, entries);
            }
            return $"${finalId.Length}\r\n{finalId}\r\n";
        }

        if (!TryParseExplicitStreamId(id, out ulong ems, out ulong eseq)) return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        if (ems == 0 && eseq == 0) return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        lock (streamLock)
        {
            if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries)) { entries = new List<StreamEntry>(); streamStore[key] = entries; }
            if (entries.Count > 0 && CompareStreamIds(finalId, entries[^1].Id) <= 0)
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            StreamEntry e = new StreamEntry { Id = finalId };
            for (int i = 3; i < args.Count; i += 2) e.Fields.Add(new KeyValuePair<string, string>(args[i], args[i + 1]));
            entries.Add(e);
            ServeBlockedXReadClients(key, entries);
        }
        return $"${finalId.Length}\r\n{finalId}\r\n";
    }

    // ── XRANGE ────────────────────────────────────────────────────────────────
    static string HandleXRange(List<string> args)
    {
        if (args.Count != 4) return "-ERR wrong number of arguments for 'XRANGE'\r\n";
        string key = args[1];
        if (!TryParseStreamRangeBound(args[2], true, out string startId) || !TryParseStreamRangeBound(args[3], false, out string endId))
            return "-ERR Invalid stream ID specified as range\r\n";
        lock (streamLock)
        {
            if (!streamStore.TryGetValue(key, out List<StreamEntry>? entries) || entries.Count == 0) return "*0\r\n";
            List<StreamEntry> match = entries.FindAll(e => CompareStreamIds(e.Id, startId) >= 0 && CompareStreamIds(e.Id, endId) <= 0);
            if (match.Count == 0) return "*0\r\n";
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{match.Count}\r\n");
            foreach (StreamEntry e in match)
            {
                sb.Append("*2\r\n");
                sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n");
                sb.Append($"*{e.Fields.Count * 2}\r\n");
                foreach (var f in e.Fields) { sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n"); sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n"); }
            }
            return sb.ToString();
        }
    }

    // ── XREAD ─────────────────────────────────────────────────────────────────
    static string HandleXRead(List<string> args)
    {
        if (args.Count < 4) return "-ERR wrong number of arguments for 'XREAD'\r\n";
        bool isBlocking = false; int timeoutMs = 0; int streamsArgIndex = 1;
        if (args[1].Equals("BLOCK", StringComparison.OrdinalIgnoreCase))
        {
            if (args.Count < 5) return "-ERR wrong number of arguments for 'XREAD'\r\n";
            if (!int.TryParse(args[2], out timeoutMs) || timeoutMs < 0) return "-ERR timeout is not an integer or out of range\r\n";
            isBlocking = true; streamsArgIndex = 3;
        }
        if (!args[streamsArgIndex].Equals("STREAMS", StringComparison.OrdinalIgnoreCase)) return "-ERR syntax error\r\n";
        int trailingArgCount = args.Count - (streamsArgIndex + 1);
        if (trailingArgCount % 2 != 0) return "-ERR syntax error\r\n";
        int streamCount = trailingArgCount / 2;
        if (streamCount == 0) return "-ERR wrong number of arguments for 'XREAD'\r\n";
        List<string> keys = new List<string>(streamCount);
        List<string> startIdsExclusive = new List<string>(streamCount);
        for (int i = 0; i < streamCount; i++) keys.Add(args[streamsArgIndex + 1 + i]);
        List<string> rawIds = new List<string>(streamCount);
        for (int i = 0; i < streamCount; i++) rawIds.Add(args[streamsArgIndex + 1 + streamCount + i]);
        bool hasDollarId = rawIds.Exists(id => id == "$");
        if (!hasDollarId)
        {
            for (int i = 0; i < streamCount; i++)
            {
                if (!TryParseXReadId(rawIds[i], out string sid)) return "-ERR Invalid stream ID specified as stream command argument\r\n";
                startIdsExclusive.Add(sid);
            }
            string imm = BuildXReadResponse(keys, startIdsExclusive, streamCount);
            if (imm != "*0\r\n") return imm;
        }
        if (!isBlocking) return "*0\r\n";
        string blockKey = keys[0];
        BlockedXReadRequest request;
        lock (streamLock)
        {
            if (hasDollarId)
            {
                startIdsExclusive.Clear();
                for (int i = 0; i < streamCount; i++)
                {
                    if (rawIds[i] == "$")
                    {
                        string lastId = "0-0";
                        if (streamStore.TryGetValue(keys[i], out List<StreamEntry>? ex) && ex.Count > 0) lastId = ex[^1].Id;
                        startIdsExclusive.Add(lastId);
                    }
                    else
                    {
                        if (!TryParseXReadId(rawIds[i], out string parsed)) return "-ERR Invalid stream ID specified as stream command argument\r\n";
                        startIdsExclusive.Add(parsed);
                    }
                }
            }
            else
            {
                string recheck = BuildXReadResponse(keys, startIdsExclusive, streamCount);
                if (recheck != "*0\r\n") return recheck;
            }
            request = new BlockedXReadRequest(startIdsExclusive[0]);
            if (!blockedXReadWaiters.ContainsKey(blockKey)) blockedXReadWaiters[blockKey] = new List<BlockedXReadRequest>();
            blockedXReadWaiters[blockKey].Add(request);
        }
        bool signaled = timeoutMs == 0 ? request.Signal.WaitOne() : request.Signal.WaitOne(timeoutMs);
        if (!signaled)
        {
            lock (streamLock)
            {
                if (!request.IsCompleted)
                {
                    if (blockedXReadWaiters.TryGetValue(blockKey, out List<BlockedXReadRequest>? wl))
                    { wl.Remove(request); if (wl.Count == 0) blockedXReadWaiters.Remove(blockKey); }
                }
            }
            if (!request.IsCompleted) return "*-1\r\n";
        }
        if (request.NewEntries == null || request.NewEntries.Count == 0) return "*-1\r\n";
        return BuildXReadResponseFromEntries(blockKey, request.NewEntries);
    }

    static string BuildXReadResponse(List<string> keys, List<string> startIds, int streamCount)
    {
        lock (streamLock)
        {
            List<(string key, List<StreamEntry> entries)> results = new List<(string, List<StreamEntry>)>();
            for (int i = 0; i < streamCount; i++)
            {
                if (!streamStore.TryGetValue(keys[i], out List<StreamEntry>? se) || se.Count == 0) continue;
                List<StreamEntry> m = se.FindAll(e => CompareStreamIds(e.Id, startIds[i]) > 0);
                if (m.Count > 0) results.Add((keys[i], m));
            }
            if (results.Count == 0) return "*0\r\n";
            StringBuilder sb = new StringBuilder();
            sb.Append($"*{results.Count}\r\n");
            foreach ((string k, List<StreamEntry> ents) in results)
            {
                sb.Append("*2\r\n");
                sb.Append($"${k.Length}\r\n{k}\r\n");
                sb.Append($"*{ents.Count}\r\n");
                foreach (StreamEntry e in ents)
                {
                    sb.Append("*2\r\n");
                    sb.Append($"${e.Id.Length}\r\n{e.Id}\r\n");
                    sb.Append($"*{e.Fields.Count * 2}\r\n");
                    foreach (var f in e.Fields) { sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n"); sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n"); }
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
            foreach (var f in e.Fields) { sb.Append($"${f.Key.Length}\r\n{f.Key}\r\n"); sb.Append($"${f.Value.Length}\r\n{f.Value}\r\n"); }
        }
        return sb.ToString();
    }

    static void ServeBlockedXReadClients(string key, List<StreamEntry> allEntries)
    {
        if (!blockedXReadWaiters.TryGetValue(key, out List<BlockedXReadRequest>? waiters) || waiters.Count == 0) return;
        List<BlockedXReadRequest> done = new List<BlockedXReadRequest>();
        foreach (BlockedXReadRequest w in waiters)
        {
            List<StreamEntry> ne = allEntries.FindAll(e => CompareStreamIds(e.Id, w.StartIdExclusive) > 0);
            if (ne.Count > 0) { w.NewEntries = ne; w.IsCompleted = true; w.Signal.Set(); done.Add(w); }
        }
        foreach (BlockedXReadRequest w in done) waiters.Remove(w);
        if (waiters.Count == 0) blockedXReadWaiters.Remove(key);
    }

    static bool TryParseExplicitStreamId(string id, out ulong ms, out ulong seq)
    {
        ms = seq = 0;
        if (id == "*") return false;
        string[] p = id.Split('-');
        return p.Length == 2 && ulong.TryParse(p[0], out ms) && ulong.TryParse(p[1], out seq);
    }

    static bool TryParseStreamRangeBound(string input, bool isStart, out string normalizedId)
    {
        normalizedId = string.Empty;
        if (isStart && input == "-") { normalizedId = "0-0"; return true; }
        if (!isStart && input == "+") { normalizedId = $"{ulong.MaxValue}-{ulong.MaxValue}"; return true; }
        string[] p = input.Split('-');
        if (p.Length == 1 && ulong.TryParse(p[0], out ulong ms1)) { normalizedId = $"{ms1}-{(isStart ? 0 : ulong.MaxValue)}"; return true; }
        if (p.Length == 2 && ulong.TryParse(p[0], out ulong ms2) && ulong.TryParse(p[1], out ulong seq)) { normalizedId = $"{ms2}-{seq}"; return true; }
        return false;
    }

    static bool TryParseXReadId(string input, out string normalizedId)
    {
        normalizedId = string.Empty;
        string[] p = input.Split('-');
        if (p.Length == 1 && ulong.TryParse(p[0], out ulong ms))   { normalizedId = $"{ms}-0";  return true; }
        if (p.Length == 2 && ulong.TryParse(p[0], out ulong ms2) && ulong.TryParse(p[1], out ulong seq)) { normalizedId = $"{ms2}-{seq}"; return true; }
        return false;
    }

    static int CompareStreamIds(string left, string right)
    {
        string[] l = left.Split('-'), r = right.Split('-');
        ulong lms = ulong.Parse(l[0]), lseq = ulong.Parse(l[1]), rms = ulong.Parse(r[0]), rseq = ulong.Parse(r[1]);
        int c = lms.CompareTo(rms);
        return c != 0 ? c : lseq.CompareTo(rseq);
    }

    static void ServeBlockedClientsFromList(string key)
    {
        if (!listStore.TryGetValue(key, out List<string>? list) || list.Count == 0) return;
        if (!blockedPopWaiters.TryGetValue(key, out Queue<BlockedPopRequest>? waiters) || waiters.Count == 0) return;
        while (list.Count > 0 && waiters.Count > 0)
        {
            BlockedPopRequest w = waiters.Dequeue();
            string v = list[0]; list.RemoveAt(0);
            w.Value = v; w.IsCompleted = true; w.Signal.Set();
        }
        if (waiters.Count == 0) blockedPopWaiters.Remove(key);
    }

    static string EncodeKeyValueArray(string key, string value)
        => $"*2\r\n${key.Length}\r\n{key}\r\n${value.Length}\r\n{value}\r\n";

    static void RemoveBlockedRequest(string key, BlockedPopRequest request)
    {
        if (!blockedPopWaiters.TryGetValue(key, out Queue<BlockedPopRequest>? waiters)) return;
        Queue<BlockedPopRequest> filtered = new Queue<BlockedPopRequest>();
        while (waiters.Count > 0) { var c = waiters.Dequeue(); if (!ReferenceEquals(c, request)) filtered.Enqueue(c); }
        if (filtered.Count == 0) blockedPopWaiters.Remove(key);
        else blockedPopWaiters[key] = filtered;
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