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


// Ghi response: buffer vào MemoryStream để tính size, sau đó ghi size + correlationId + body
static class ResponseWriter
{
    public static void WriteResponse(Stream networkStream, int correlationId, Action<KafkaBinaryWriter> writeBody)
    {
        using var ms = new MemoryStream();
        var w = new KafkaBinaryWriter(ms);
        // Response header: CorrelationId
        w.WriteInt32BE(correlationId);
        // Body
        writeBody(w);
        // Prepend Size
        var payload = ms.ToArray();
        var writer = new KafkaBinaryWriter(networkStream);
        writer.WriteInt32BE(payload.Length); // size excludes this field itself
        writer.WriteBytes(payload);
    }
}




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