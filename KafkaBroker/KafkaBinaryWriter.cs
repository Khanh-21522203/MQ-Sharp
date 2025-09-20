using System.Buffers.Binary;
using System.Text;

namespace KafkaBroker;

public class KafkaBinaryWriter(Stream stream)
{
    private readonly Stream _stream = stream;

    public void WriteInt16Be(short v)
    {
        Span<byte> b = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(b, v);
        _stream.Write(b);
    }

    public void WriteInt32Be(int v)
    {
        Span<byte> b = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(b, v);
        _stream.Write(b);
    }

    public void WriteInt64Be(long v)
    {
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(b, v);
        _stream.Write(b);
    }

    public void WriteKafkaString(string? s)
    {
        if (s == null)
        {
            WriteInt16Be(-1);
            return;
        }

        var bytes = Encoding.UTF8.GetBytes(s);
        WriteInt16Be((short)bytes.Length);
        _stream.Write(bytes, 0, bytes.Length);
    }

    public void WriteBytes(byte[] b) => _stream.Write(b, 0, b.Length);
}