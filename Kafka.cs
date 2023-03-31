using Confluent.Kafka;

namespace TrainingApp
{
    public static class Kafka
    {
        const string address = "localhost:29092";
        const string topic = "new-topic";
        public static async Task AddMess(string mes)
        {
            var config = new ProducerConfig { BootstrapServers = address };
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var topicPartition = new TopicPartition(topic, new Partition(0));
                    var dr = await p.ProduceAsync(topicPartition, new Message<Null, string> { Value = mes });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

        public static void Reading(string groupId)
        {
            var conf = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = address,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed '{cr.Value}' at: '{cr.TopicPartitionOffset}' group - {conf.GroupId} thread - {Thread.CurrentThread.ManagedThreadId}.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}
