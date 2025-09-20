using KafkaBroker.Handlers;
using KafkaBroker.Requests;
using Serilog;

namespace KafkaBroker;

public class Dispatcher(Dictionary<short, IRequestHandler> handlers, ILogger logger)
{
    private readonly Dictionary<short, IRequestHandler> _handlers = handlers;
    private readonly ILogger _logger = logger;

    public void Process(Stream stream)
    {
        var reader = new KafkaBinaryReader(stream);

        while (true)
        {
            try
            {
                reader.ReadInt32Be();
            }
            catch (EndOfStreamException)
            {
                _logger.Warning("No more events available");
                break;
            }

            var apiKey = reader.ReadInt16Be();
            var apiVer = reader.ReadInt16Be();
            var corrId = reader.ReadInt32Be();
            var clientId = reader.ReadKafkaString();

            var header = new RequestHeader(apiKey, apiVer, corrId, clientId);

            if (!_handlers.TryGetValue(apiKey, out var handler))
            {
                // Unknown API: consume payload (best-effort) & ignore
                break;
            }

            handler.Handle(header, reader, stream);
        }
    }
}