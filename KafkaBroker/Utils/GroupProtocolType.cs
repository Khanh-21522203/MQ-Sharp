namespace KafkaBroker.Utils;

public enum GroupProtocolType : byte
{
    Consumer = 0
}

public static class GroupProtocolTypeExtensions
{
    public static string ToKafkaString(this GroupProtocolType type) => type switch
    {
        GroupProtocolType.Consumer => "consumer",
        _ => throw new ArgumentOutOfRangeException(nameof(type))
    };

    public static GroupProtocolType Parse(string s) => s switch
    {
        "consumer" => GroupProtocolType.Consumer,
        _ => throw new ArgumentOutOfRangeException(nameof(s), $"Unsupported GroupProtocolType: {s}")
    };
}