using Autofac;
using Autofac.Features.Indexed;
using RmqSuperStream.Applibs;
using RmqSuperStream.Model;
using RmqSuperStream.Tests.Applibs;
using RmqSuperStream.Tests.Event;

const string rabbitMqUri = @"localhost:5672";
const string rabbitMqStreamUri = @"localhost:5552";
const string rabbitUserName = "guest";
const string rabbitPassword = "guest";

try
{
    await RabbitMqFactory.StartAsync(
        rabbitUserName,
        rabbitPassword,
        rabbitMqUri,
        rabbitMqStreamUri);

    var category = string.Empty;

    while (!(new[] { "1", "2" }.Any(p => p == category)))
    {
        Console.WriteLine("1.Producer");
        Console.WriteLine("2.Consumer");
        Console.Write("Category:");
        category = Console.ReadLine();
    }

    switch (category)
    {
        case "1":

            {
                using (var scope = AutofacConfig.Container.BeginLifetimeScope())
                {
                    var producer = scope.Resolve<IRabbitMqProducer>();

                    var topic = string.Empty;

                    while (string.IsNullOrEmpty(topic))
                    {
                        Console.Write("Topic:");
                        topic = Console.ReadLine();
                    }

                    int count = 0;

                    while (++count < 100)
                    {
                        var @event = new EchoEvent()
                        {
                            Counter = count,
                            Content = $"{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}"
                        };

                        await producer.PublishAsync(
                            topic,
                            @event,
                            $"{count}");

                        Console.WriteLine($"Counter: {count}");

                        await Task.Delay(5000);
                    }
                }
            }

            break;

        case "2":

            {
                var reference = string.Empty;
                var topic = string.Empty;
                var subscribeTopicsStr = string.Empty;

                while (
                    string.IsNullOrEmpty(reference) ||
                    string.IsNullOrEmpty(topic) ||
                    string.IsNullOrEmpty(subscribeTopicsStr))
                {
                    Console.Write("Reference:");
                    reference = Console.ReadLine();
                    Console.Write("Topic:");
                    topic = Console.ReadLine();
                    Console.Write("SubscribeTopics(',' Split):");
                    subscribeTopicsStr = Console.ReadLine();
                }

                using (var scope = AutofacConfig.Container.BeginLifetimeScope())
                {
                    var handlerSets = scope.Resolve<IIndex<string, IPubSubHandler<RabbitMqEventStream>>>();

                    var consumer = new RabbitMqConsumer(
                        reference,
                        subscribeTopicsStr.Split(','),
                        new PubSubDispatcher<RabbitMqEventStream>(
                            handlerSets,
                            (errorMsg) =>
                            {
                                Console.WriteLine($"RMQ Handler Exception: {errorMsg}");
                            }),
                        topic,
                        partitionCount: 3,
                        rmqClusterNum: 3);

                    await consumer.RegisterAsync();

                    while (Console.ReadLine().ToLower() != "exit")
                    {
                        await Task.Delay(1000);
                    }
                }
            }

            break;

        default:
            throw new Exception("illegal category");
    }
}
catch (Exception ex)
{
    Console.WriteLine(ex.Message);
}

Console.Write("finished");
Console.Read();