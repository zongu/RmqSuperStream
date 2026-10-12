
namespace RmqSuperStream.Applibs
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using RabbitMQ.Client;
    using RabbitMQ.Stream.Client;

    public static class RabbitMqFactory
    {
        private static IConnection _connection;
        private static ConnectionFactory _factory;
        private static ConcurrentDictionary<string, IChannel> _models = new ConcurrentDictionary<string, IChannel>();
        private static StreamSystem _streamSystem;
        public static StreamSystem RmqStreamSystem
            => _streamSystem;

        /// <summary>
        /// 取得channel
        /// </summary>
        /// <param name="topic">Topic</param>
        /// <returns></returns>
        public static async Task<IChannel> GetChannelAsync(string topicName)
        {
            await TryAddModelAsync(topicName);
            return _models[topicName];
        }

        /// <summary>
        /// RMQ工廠上班
        /// </summary>
        /// <param name="userName">帳號</param>
        /// <param name="password">密碼</param>
        /// <param name="rabbitMqUri">RMQ位址</param>
        /// <param name="rabbitMqStreamUri">RMQ Stream位址</param>
        /// <returns></returns>
        public static async Task StartAsync(
            string userName,
            string password,
            string rabbitMqUri,
            string rabbitMqStreamUri)
        {
            // 創建factorry
            if (_factory == null)
            {
                _factory = new ConnectionFactory()
                {
                    UserName = userName,
                    Password = password,
                    AutomaticRecoveryEnabled = true,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(5)
                };

                _connection = await _factory.CreateConnectionAsync(AmqpTcpEndpoint.ParseMultiple(rabbitMqUri));
            }

            // streamSystem本體
            if (_streamSystem == null)
            {
                var config = new StreamSystemConfig()
                {
                    UserName = userName,
                    Password = password,
                    Endpoints = rabbitMqStreamUri.Split(',').Select(uri => (EndPoint)(new IPEndPoint(IPAddress.Parse(uri.Split(':')[0]), int.Parse(uri.Split(':')[1])))).ToList()
                };

                _streamSystem = await StreamSystem.Create(config);
            }
        }

        /// <summary>
        /// RMQ工廠下班
        /// </summary>
        /// <returns></returns>
        public static async Task StopAsync()
        {
            if (_streamSystem == null)
            {
                return;
            }

            await _streamSystem.Close();
            await _streamSystem.DisposeAsync();
            _streamSystem = null;
        }

        /// <summary>
        /// 添加RMQ Channel
        /// </summary>
        /// <param name="topicName">topic</param>
        /// <returns></returns>
        private static async Task<bool> TryAddModelAsync(string topicName)
        {
            if (!_models.ContainsKey(topicName))
            {
                var channel = await _connection.CreateChannelAsync();

                await channel.ExchangeDeclareAsync(
                    exchange: topicName,
                    type: ExchangeType.Fanout,
                    durable: true,
                    autoDelete: false);

                _models.TryAdd(topicName, channel);

                return true;
            }

            return false;
        }
    }
}
