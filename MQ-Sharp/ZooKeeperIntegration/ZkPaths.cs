using System.Globalization;

namespace MQ_Sharp.ZooKeeperIntegration;

public static class ZkPaths
{
     // ===== Roots
    public const string BrokersRoot   = "/brokers";
    public const string BrokerIds     = BrokersRoot + "/ids";
    public const string BrokerTopics  = BrokersRoot + "/topics";

    public const string ConfigRoot        = "/config";
    public const string ConfigTopicsRoot  = ConfigRoot + "/topics";
    public const string ConfigBrokersRoot = ConfigRoot + "/brokers";

    public const string Controller        = "/controller";
    public const string ControllerEpoch   = "/controller_epoch";

    public const string ConsumersRoot     = "/consumers";
    
}