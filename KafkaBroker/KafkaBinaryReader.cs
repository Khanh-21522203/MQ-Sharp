using System.Buffers.Binary;
using System.Text;

namespace KafkaBroker;

public class KafkaBinaryReader(Stream stream) : IDisposable
{
    private readonly Stream _stream = stream;
    private readonly byte[] _b8 = new byte[8];
    private readonly byte[] _b4 = new byte[4];
    private readonly byte[] _b2 = new byte[2];

    public int ReadInt32Be()
    {
        Fill(_b4);
        return BinaryPrimitives.ReadInt32BigEndian(_b4);
    }

    public short ReadInt16Be()
    {
        Fill(_b2);
        return BinaryPrimitives.ReadInt16BigEndian(_b2);
    }

    public long ReadInt64Be()
    {
        Fill(_b8);
        return BinaryPrimitives.ReadInt64BigEndian(_b8);
    }

    public string ReadKafkaString()
    {
        short len = ReadInt16Be();
        if (len < 0) return string.Empty;
        if (len == 0) return string.Empty;
        var buf = new byte[len];
        Fill(buf);
        return Encoding.UTF8.GetString(buf);
    }

    public byte[] ReadBytesExact(int n)
    {
        var buf = new byte[n];
        Fill(buf);
        return buf;
    }

    private void Fill(byte[] buf)
    {
        var read = 0;
        while (read < buf.Length)
        {
            var n = _stream.Read(buf, read, buf.Length - read);
            if (n <= 0)
                throw new EndOfStreamException();
            read += n;
        }
    }

    public void Dispose() => _stream.Dispose();
}