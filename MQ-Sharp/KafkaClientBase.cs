namespace MQ_Sharp;

public abstract class KafkaClientBase: IDisposable
{
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    protected abstract void Dispose(bool disposing);
}