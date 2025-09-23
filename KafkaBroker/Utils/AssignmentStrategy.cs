namespace KafkaBroker.Utils;

public enum AssignmentStrategy : byte
{
    Range = 0
}

public static class AssignmentStrategyExtensions
{
    public static string ToKafkaString(this AssignmentStrategy strategy) => strategy switch
    {
        AssignmentStrategy.Range => "range",
        _ => throw new ArgumentOutOfRangeException(nameof(strategy))
    };

    public static AssignmentStrategy Parse(string s) => s switch
    {
        "range" => AssignmentStrategy.Range,
        _ => throw new ArgumentOutOfRangeException(nameof(s), $"Unsupported AssignmentStrategy: {s}")
    };
}