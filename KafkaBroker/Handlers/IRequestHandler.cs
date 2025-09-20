using KafkaBroker.Requests;

namespace KafkaBroker.Handlers;

public interface IRequestHandler
{
    void Handle(RequestHeader header, KafkaBinaryReader reader, Stream output);
}