using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaBroker.Handlers;


// ------------------ Program entry ------------------
public static class Program
{
    public static async Task Main(string[] args)
    {
        var ip = IPAddress.Any;
        int port = 9092;
        var logs = new LogManager();
        // seed topics if muốn:
        logs.EnsureTopic("demo", partitions: 1);

        var server = new KafkaServer(new IPEndPoint(ip, port), logs, advertisedHost: "localhost", advertisedPort: port);
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        await server.StartAsync(cts.Token);
    }
}