/*
## stream下的Exchange分級制度

                    ┌── [RoutingKey: "Topic1-SubscribeTopic"]  ─► [Topic1-SubscribeTopic (Super Stream)]
                    │                                                      ├── Topic1-SubscribeTopic-0 (partition 0)
                    │                                                      ├── Topic1-SubscribeTopic-1 (partition 1)
[SubscribeTopic]──┤                                                      └── Topic1-SubscribeTopic-2 (partition 2)
 (訂閱對象Topic)    │
                    │
                    └── [RoutingKey: "Topic2-SubscribeTopic"]  ─► [Topic2-SubscribeTopic (Super Stream)]
                                                                            ├── Topic2-SubscribeTopic-0 (partition 0)
                                                                            ├── Topic2-SubscribeTopic-1 (partition 1)
                                                                            └── Topic2-SubscribeTopic-2 (partition 2)
*/

namespace RmqSuperStream.Applibs
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Text;
    using System.Text.Json;
    using System.Threading.Tasks;
    using RabbitMQ.Stream.Client;
    using RabbitMQ.Stream.Client.Reliable;
    using RmqSuperStream.Model;

    /// <summary>
    /// RMQ消費者
    /// </summary>
    public class RabbitMqConsumer
    {
        private string reference;
        private IEnumerable<string> queueNames;
        private IPubSubDispatcher<RabbitMqEventStream> dispatcher;
        private string topic;
        private int? rmqClusterNum;
        private int? partitionCount;
        private ConcurrentDictionary<string, Consumer> consumers = new ConcurrentDictionary<string, Consumer>();

        /// <summary>
        /// 建構子
        /// </summary>
        /// <param name="reference">scale-out 參考唯一值</param>
        /// <param name="queueNames">訂閱對象的Topic</param>
        /// <param name="dispatcher">事件處理調度員</param>
        /// <param name="topic">本身topic</param>
        /// <param name="rmqClusterNum">叢集實作HA數量</param>
        /// <param name="partitionCount">partition切割數量</param>
        public RabbitMqConsumer(
            string reference,
            IEnumerable<string> queueNames,
            IPubSubDispatcher<RabbitMqEventStream> dispatcher,
            string topic,
            int? rmqClusterNum = null,
            int partitionCount = 16)
        {
            this.reference = reference;
            this.queueNames = queueNames;
            this.dispatcher = dispatcher;
            this.topic = topic;
            this.rmqClusterNum = rmqClusterNum;
            this.partitionCount = partitionCount;
        }

        /// <summary>
        /// 註冊消費者
        /// </summary>
        /// <returns></returns>
        public async Task RegisterAsync()
        {
            foreach (var topicName in this.queueNames)
            {
                // 先建立最外層的1級Exchange
                var channel = await RabbitMqFactory.GetChannelAsync(topicName);

                var exchangeName = $"{this.topic}-{topicName}";

                // 建立stream
                {
                    var exchangeArgs = new Dictionary<string, object>()
                    {
                        { "x-super-stream", true },
                        { "hash-header", "PartitionKey" }
                    };

                    // 綁定2級Exchange
                    await channel.ExchangeDeclareAsync(
                        exchange: exchangeName,
                        type: "x-consistent-hash",
                        durable: true,
                        autoDelete: false,
                        arguments: exchangeArgs);

                    var queueArgs = new Dictionary<string, object>()
                    {
                        // 表示Stream
                        { "x-queue-type", "stream" },
                        // 保留 1 天 (單位支援時間字串，如 "1D" 或以毫秒/秒為單位)
                        { "x-max-age", "1D" },
                        // 每個 Segment 最大 500MB (Bytes)
                        { "x-stream-max-segment-size-bytes", 524_288_000 }
                    };

                    if (this.rmqClusterNum.HasValue)
                    {
                        // stream ha
                        queueArgs.Add("x-initial-cluster-size", this.rmqClusterNum.Value);
                    }

                    // 建立partition
                    for (var partition = 0; partition < this.partitionCount; partition++)
                    {
                        var queueName = $"{exchangeName}-{partition}";

                        // 建立Q
                        await channel.QueueDeclareAsync(
                            queue: queueName,
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: queueArgs);

                        // 建立Q跟Exchange關係
                        await channel.QueueBindAsync(
                            queue: queueName,
                            exchange: exchangeName,
                            routingKey: "100");
                    }

                    // 將1 2 級Exchange綁定關係
                    await channel.ExchangeBindAsync(exchangeName, topicName, routingKey: string.Empty);
                }

                // 建立consumer
                {
                    var consumer = await Consumer.Create(new ConsumerConfig(RabbitMqFactory.RmqStreamSystem, exchangeName)
                    {
                        // 很重要 有這個才能做partition拓樸
                        IsSuperStream = true,
                        Reference = this.reference,
                        OffsetSpec = new OffsetTypeNext(),
                        IsSingleActiveConsumer = true,
                        ConsumerUpdateListener = async (reference, stream, isActive) =>
                        {
                            if (!isActive)
                            {
                                return new OffsetTypeNext();
                            }

                            var storedOffset = await RabbitMqFactory.RmqStreamSystem.TryQueryOffset(reference, stream);

                            if (storedOffset == null)
                            {
                                return new OffsetTypeNext();
                            }

                            return new OffsetTypeOffset(storedOffset.Value + 1);
                        },
                        MessageHandler = async (stream, rawConsumer, context, message) =>
                        {
                            var @event = JsonSerializer.Deserialize<RabbitMqEventStream>(Encoding.UTF8.GetString(message.Data.Contents));

                            var errorCount = 0;

                            while (++errorCount <= 3 && !await this.dispatcher.DispatchMessageAsync(@event))
                            {
                                await Task.Delay(1000);
                            }

                            if (errorCount > 3)
                            {
                                // dispatch fail do something
                            }

                            await rawConsumer.StoreOffset(context.Offset);
                        },
                        ClientProvidedName = Environment.MachineName
                    });

                    this.consumers.TryAdd(new Guid().ToString(), consumer);
                }
            }
        }

        /// <summary>
        /// 釋放消費者
        /// </summary>
        /// <returns></returns>
        public async Task DisposeAsync()
        {
            foreach (var consumer in this.consumers)
            {
                try
                {
                    await consumer.Value.Close();
                }
                catch
                {
                }
            }
        }
    }
}
