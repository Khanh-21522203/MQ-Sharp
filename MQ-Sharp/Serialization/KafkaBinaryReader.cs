using System.Net;
using System.Net.Sockets;
using System.Text;
using MQ_Sharp.Request;

namespace MQ_Sharp.Serialization;

public class KafkaBinaryReader(Stream input) : BinaryReader(input)
{

    protected override void Dispose(bool disposing)
    {
        if (BaseStream.CanSeek)
        {
            BaseStream.Position = 0;
        }
        base.Dispose(disposing);
    }
    public override short ReadInt16()
    {
        var value = base.ReadInt16();
        var currentOrdered = IPAddress.NetworkToHostOrder(value);
        return currentOrdered;
    }
    public override int ReadInt32()
    {
        var value = base.ReadInt32();
        var currentOrdered = IPAddress.NetworkToHostOrder(value);
        return currentOrdered;
    }
    [CLSCompliant(false)]
    public override uint ReadUInt32()
    {
        var value = base.ReadBytes(4);
        return BitConverter.ToUInt32(value.Reverse().ToArray(), 0);
    }
    
    public override long ReadInt64()
    {
        var value = base.ReadInt64();
        var currentOrdered = IPAddress.NetworkToHostOrder(value);
        return currentOrdered;
    }
    

    public string? ReadShortString(string encoding = AbstractRequest.DefaultEncoding)
    {
        var length = this.ReadInt16();
        if (length == -1)
        {
            return null;
        }

        var bytes = this.ReadBytes(length);
        var encoder = Encoding.GetEncoding(encoding);
        return encoder.GetString(bytes);
    }

    public bool DataAvailable
    {
        get
        {
            if (BaseStream is NetworkStream)
            {
                return ((NetworkStream)BaseStream).DataAvailable;
            }

            return BaseStream.Length != BaseStream.Position;
        }
    }
}