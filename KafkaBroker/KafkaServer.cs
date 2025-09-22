using System.Net;
using System.Net.Sockets;
using KafkaBroker.Handlers;
using KafkaBroker.LogStorage;
using KafkaBroker.Utils;
using Serilog;

namespace KafkaBroker;

public class KafkaServer
{
    private readonly TcpListener _listener;
    private readonly Dispatcher _dispatcher;
    private readonly ILogger _logger;
    private readonly ILogManager _logManager;

    public KafkaServer(
        IPEndPoint endpoint, 
        ILogger logger, 
        ILogManager logManager, 
        ITopicMetadataManager topicMetadataManager,
        IGroupCoordinatorManager groupCoordinatorManager)
    {
        _logger = logger;
        _listener = new TcpListener(endpoint);
        _logManager = logManager;

        var handlers = new Dictionary<short, IRequestHandler>
        {
            [(short)ApiKeys.Produce] = new ProduceHandler(logManager, logger),
            [(short)ApiKeys.Fetch] = new FetchHandler(logManager, logger),
            [(short)ApiKeys.ListOffsets] = new ListOffsetsHandler(logManager, logger),
            [(short)ApiKeys.Metadata] = new TopicMetadataHandler(logger, topicMetadataManager),

            [(short)ApiKeys.FindCoordinator] = new GroupCoordinatorHandler(logger, groupCoordinatorManager),
            [(short)ApiKeys.JoinGroup] = new JoinGroupHandler(broker, logger),
            [(short)ApiKeys.Heartbeat] = new HeartbeatHandler(broker, logger),
            [(short)ApiKeys.LeaveGroup] = new LeaveGroupHandler(broker, logger),
            [(short)ApiKeys.SyncGroup] = new SyncGroupHandler(broker, logger),

            [(short)ApiKeys.OffsetCommit] = new OffsetCommitHandler(broker, logger),
            [(short)ApiKeys.OffsetFetch] = new OffsetFetchHandler(broker, logger)
        };

        _dispatcher = new Dispatcher(handlers, logger);
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _listener.Start();
        _logger.Information("KafkaServer listening on {Endpoint}", _listener.LocalEndpoint);

        while (!ct.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(ct);
            }
            catch when (ct.IsCancellationRequested)
            {
                break;
            }

            _ = Task.Run(() => HandleClient(client), ct);
        }
    }

    private void HandleClient(TcpClient client)
    {
        using (client)
        using (var stream = client.GetStream())
        {
            try
            {
                _dispatcher.Process(stream);
            }
            catch (Exception ex)
            {
                _logger.Warning("Error handling client {Client}: {Error}", client.Client.RemoteEndPoint, ex.Message);
            }
        }
    }
}