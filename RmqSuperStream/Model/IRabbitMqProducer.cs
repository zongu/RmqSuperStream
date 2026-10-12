
namespace RmqSuperStream.Model
{
    using System.Threading.Tasks;

    /// <summary>
    /// 生產者
    /// </summary>
    public interface IRabbitMqProducer
    {
        /// <summary>
        /// 發佈事件
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topicName">topic</param>
        /// <param name="data">事件</param>
        /// <param name="suffix">route partition用</param>
        /// <param name="rmqExpiration">事件逾期時限 stream目前沒用</param>
        /// <returns></returns>
        Task PublishAsync<T>(string topicName, T data, string suffix = null, string rmqExpiration = "86400000");
    }
}
