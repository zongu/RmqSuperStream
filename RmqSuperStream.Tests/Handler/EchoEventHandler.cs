
namespace RmqSuperStream.Tests.Handler
{
    using System;
    using System.Text.Json;
    using System.Threading.Tasks;
    using RmqSuperStream.Model;
    using RmqSuperStream.Tests.Event;

    /// <summary>
    /// 回呼事件處理
    /// </summary>
    public class EchoEventHandler : IRabbitMqPubSubHandler
    {
        /// <summary>
        /// 處理事件
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public async Task<bool> HandleAsync(RabbitMqEventStream stream)
        {
            try
            {
                var @event = JsonSerializer.Deserialize<EchoEvent>(stream.Data);

                Console.WriteLine($"{this.GetType().Name} Received: {stream.Data}");

                return await Task.FromResult<bool>(true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{this.GetType().Name} Handler Exception:{ex.Message}");

                return false;
            }
        }
    }
}
