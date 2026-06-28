using System.Net;
using System.Text.Json;

var config = ParseArgs(args);

var proxy = new ProxyEngine(config);
TcpProxyEngine? tcpProxy = config.TcpListenEnabled
    ? new TcpProxyEngine(config, proxy.Metrics, proxy.Filter)
    : null;

Console.CancelKeyPress += (_, _) =>
{
    Console.WriteLine("\n[Proxy] Shutting down...");
    proxy.Stop();
    tcpProxy?.Stop();
};

var udpTask = proxy.RunAsync();
var tcpTask = tcpProxy?.RunAsync() ?? Task.CompletedTask;

await Task.WhenAll(udpTask, tcpTask);

static ProxyConfig ParseArgs(string[] args)
{
    var config = new ProxyConfig();

    for (var i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--listen":
            case "-l":
                config = config with { ListenEndpoint = ParseEndpoint(GetArgValue(ref i, args), 12121) };
                break;
            case "--tcp-listen":
            case "--tl":
                config = config with { TcpListenEnabled = true};
                break;
            case "--target":
            case "-t":
                config = config with { TargetEndpoint = ParseEndpoint(GetArgValue(ref i, args), 1212) };
                break;
            case "--config":
            case "-c":
            {
                var path = GetArgValue(ref i, args);
                if (File.Exists(path))
                {
                    var fileConfig = JsonSerializer.Deserialize<ProxyConfig>(File.ReadAllText(path));
                    if (fileConfig != null)
                    config = config with
                    {
                        AllowedIPs = config.AllowedIPs.Count > 0 ? config.AllowedIPs : fileConfig.AllowedIPs,
                        DeniedIPs = config.DeniedIPs.Count > 0 ? config.DeniedIPs : fileConfig.DeniedIPs,
                        RateLimitPerClient = fileConfig.RateLimitPerClient,
                        MaxClients = fileConfig.MaxClients,
                        SessionTimeoutSeconds = fileConfig.SessionTimeoutSeconds,
                        MetricsPort = config.MetricsPort > 0 ? config.MetricsPort : fileConfig.MetricsPort,
                        Verbose = fileConfig.Verbose,
                        TcpListenEnabled = config.TcpListenEnabled || fileConfig.TcpListenEnabled,
                    };
                }
                break;
            }
            case "--allow-list":
            case "-a":
            {
                var path = GetArgValue(ref i, args);
                if (File.Exists(path))
                    config = config with
                    {
                        AllowedIPs = File.ReadLines(path).Where(IsNotComment).ToList()
                    };
                break;
            }
            case "--deny-list":
            case "-d":
            {
                var path = GetArgValue(ref i, args);
                if (File.Exists(path))
                    config = config with
                    {
                        DeniedIPs = File.ReadLines(path).Where(IsNotComment).ToList()
                    };
                break;
            }
            case "--rate-limit":
            case "-r":
                config = config with { RateLimitPerClient = int.Parse(GetArgValue(ref i, args)) };
                break;
            case "--max-clients":
                config = config with { MaxClients = int.Parse(GetArgValue(ref i, args)) };
                break;
            case "--session-timeout":
                config = config with { SessionTimeoutSeconds = int.Parse(GetArgValue(ref i, args)) };
                break;
            case "--metrics-port":
                config = config with { MetricsPort = int.Parse(GetArgValue(ref i, args)) };
                break;
            case "--verbose":
            case "-v":
                config = config with { Verbose = true };
                break;
            case "--help":
            case "-h":
                PrintHelp();
                Environment.Exit(0);
                break;
            default:
                Console.Error.WriteLine($"Unknown option: {args[i]}");
                PrintHelp();
                Environment.Exit(1);
                break;
        }
    }

    return config;
}

static string GetArgValue(ref int i, string[] args)
{
    if (++i >= args.Length)
    {
        Console.Error.WriteLine($"Expected value after {args[i - 1]}");
        Environment.Exit(1);
    }
    return args[i];
}

static IPEndPoint ParseEndpoint(string value, int defaultPort)
{
    if (value.Contains('.'))
    {
        var parts = value.Split(':');
        var address = IPAddress.Parse(parts[0]);
        var port = parts.Length > 1 ? int.Parse(parts[1]) : defaultPort;
        return new IPEndPoint(address, port);
    }

    return new IPEndPoint(IPAddress.Any, int.Parse(value));
}

static bool IsNotComment(string line)
{
    var trimmed = line.Trim();
    return trimmed.Length > 0 && !trimmed.StartsWith('#');
}

static void PrintHelp()
{
    Console.WriteLine("""
Game Traffic Proxy for RobustToolbox/Lidgren

Usage: GameTrafficProxy [options]

Options:
  -l, --listen <endpoint>     UDP listen address:port (default: 0.0.0.0:12121)
  -t, --target <endpoint>     Target server address:port (default: 127.0.0.1:1212)
      --tcp-listen <endpoint> Enable TCP relay on address:port (default: 0.0.0.0:12121)
  -c, --config <path>         JSON config file
  -a, --allow-list <path>     File with allowed IPs/CIDRs (one per line)
  -d, --deny-list <path>      File with denied IPs/CIDRs (one per line)
  -r, --rate-limit <n>        Max packets/sec per client (0 = unlimited)
      --max-clients <n>       Maximum concurrent clients (default: 256)
      --session-timeout <s>   Seconds idle before dropping session (default: 60)
      --metrics-port <port>  Prometheus metrics HTTP port (0 = disabled, e.g. 1234)
  -v, --verbose               Log all forwarded packets
  -h, --help                  Show this help

Examples:
  GameTrafficProxy -l 0.0.0.0:7777 -t 192.168.1.10:1212
  GameTrafficProxy -c config.json --metrics-port 9090 -v
  GameTrafficProxy -a allowlist.txt -d denylist.txt -r 100
""");
}

public record ProxyConfig
{
    public IPEndPoint ListenEndpoint { get; init; } = new(IPAddress.Any, 12121);
    public IPEndPoint TargetEndpoint { get; init; } = new(IPAddress.Loopback, 1212);
    public List<string> AllowedIPs { get; init; } = [];
    public List<string> DeniedIPs { get; init; } = [];
    public int RateLimitPerClient { get; init; }
    public int MaxClients { get; init; } = 256;
    public int SessionTimeoutSeconds { get; init; } = 60;
    public int MetricsPort { get; init; }
    public bool Verbose { get; init; }
    public bool TcpListenEnabled { get; init; }
}
