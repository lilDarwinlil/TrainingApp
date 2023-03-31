using TrainingApp;

class Program
{
    public static async Task Main(string[] args)
    {
        Task.Run(() => Kafka.Reading("g1"));
        Task.Run(() => Kafka.Reading("g1"));
        Task.Run(() => Kafka.Reading("g2"));

        for (int i = 0; i < 500; i++)
        {
            await Kafka.AddMess($"mes #{i}");
            Thread.Sleep(1000);
        }

        Console.ReadLine();
    }
}