using System.Text;
using MQ_Sharp.Serialization;

namespace MQ_Sharp.Utils;

public class BitWorks
{
    /// <summary>
    /// Converts the value to bytes and reverses them.
    /// </summary>
    /// <param name="value">The value to convert to bytes.</param>
    /// <returns>Bytes representing the value.</returns>
    public static byte[] GetBytesReversed(short value)
    {
        return ReverseBytes(BitConverter.GetBytes(value));
    }

    /// <summary>
    /// Converts the value to bytes and reverses them.
    /// </summary>
    /// <param name="value">The value to convert to bytes.</param>
    /// <returns>Bytes representing the value.</returns>
    public static byte[] GetBytesReversed(int value)
    {
        return ReverseBytes(BitConverter.GetBytes(value));
    }

    /// <summary>
    /// Converts the value to bytes and reverses them.
    /// </summary>
    /// <param name="value">The value to convert to bytes.</param>
    /// <returns>Bytes representing the value.</returns>
    public static byte[] GetBytesReversed(long value)
    {
        return ReverseBytes(BitConverter.GetBytes(value));
    }

    /// <summary>
    /// Reverse the position of an array of bytes.
    /// </summary>
    /// <param name="inArray">
    /// The array to reverse.  If null or zero-length then the returned array will be null.
    /// </param>
    /// <returns>The reversed array.</returns>
    public static byte[] ReverseBytes(byte[] inArray)
    {
        if (inArray != null && inArray.Length > 0)
        {
            int highCtr = inArray.Length - 1;
            byte temp;

            for (int ctr = 0; ctr < inArray.Length / 2; ctr++)
            {
                temp = inArray[ctr];
                inArray[ctr] = inArray[highCtr];
                inArray[highCtr] = temp;
                highCtr -= 1;
            }
        }

        return inArray;
    }

    /// <summary>
    /// Return size of a size prefixed string where the size is stored as a 2 byte short
    /// </summary>
    /// <param name="text">The string to write</param>
    /// <param name="encoding">The encoding in which to write the string</param>
    /// <returns></returns>
    public static short GetShortStringLength(string text, string encoding)
    {
        if (string.IsNullOrEmpty(text))
        {
            return (short)2;
        }
        else
        {
            Encoding encoder = Encoding.GetEncoding(encoding);
            var result = (short)2 + (short)encoder.GetByteCount(text);
            return (short)result;
        }
    }

    public static string ReadShortString(KafkaBinaryReader reader, string encoding)
    {
        var size = reader.ReadInt16();
        if (size < 0)
        {
            return null;
        }
        var bytes = reader.ReadBytes(size);
        Encoding encoder = Encoding.GetEncoding(encoding);
        return encoder.GetString(bytes);
    }
}