
namespace RmqSuperStream.Applibs
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Text.Json;
    using System.Threading.Tasks;
    using RabbitMQ.Client;
    using RmqSuperStream.Model;

    /// <summary>
    /// 生產者
    /// </summary>
    public class RabbitMqProducer : IRabbitMqProducer
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
        public async Task PublishAsync<T>(string topicName, T data, string suffix = null, string rmqExpiration = "86400000")
        {
            var channel = await RabbitMqFactory.GetChannelAsync(topicName);
            var es = new RabbitMqEventStream(
                data.GetType().Name,
                JsonSerializer.Serialize(data),
                TimeStampHelper.ToUtcTimeStamp(DateTime.Now));

            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(es));
            var props = new BasicProperties();
            props.ContentType = "text/plain";
            props.DeliveryMode = DeliveryModes.Persistent;
            props.Expiration = rmqExpiration;
            props.Type = string.IsNullOrEmpty(suffix) ? Guid.NewGuid().ToString() : suffix;
            props.Headers = new Dictionary<string, object>()
            {
                { "Subject", string.IsNullOrEmpty(suffix) ? Guid.NewGuid().ToString() : suffix },
                { "PartitionKey", string.IsNullOrEmpty(suffix) ? Guid.NewGuid().ToString() : suffix }
            };

            await channel.BasicPublishAsync(
                topicName,
                string.IsNullOrEmpty(suffix) ? Guid.NewGuid().ToString() : suffix,
                true,
                props,
                body);
        }
    }
}
