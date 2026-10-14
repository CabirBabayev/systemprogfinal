using System.Collections.Concurrent;
var orders = new ConcurrentQueue<Order>();
var random = new Random();
for (int i = 1; i <= 100; i++)
{
    orders.Enqueue(new Order
    {
        Id = i,
        Price = random.Next(100, 1001),
        IsProcessed = false
    });
}
volatileClass vc = new volatileClass();
int processedCount = 0;
decimal totalPrice = 0;
object lockObject = new object();
object fileLock = new object();
SemaphoreSlim semaphore = new SemaphoreSlim(2, 2);
File.WriteAllText("results.txt", "Результаты обработки заказов\n");
Task[] workers = new Task[5];
for (int i = 0; i < workers.Length; i++)
{
    int workerId = i + 1;
    workers[i] = Task.Run(() =>
    {
        while (vc.IsRunning && orders.TryDequeue(out Order order))
        {
            semaphore.Wait();
            try
            {
                Thread.Sleep(100);
                Monitor.Enter(lockObject);
                try
                {
                    order.IsProcessed = true;
                    processedCount++;
                    totalPrice += order.Price;
                }
                finally
                {
                    Monitor.Exit(lockObject);
                }
                lock (fileLock)
                {
                    File.AppendAllText("results.txt", $"Поток {workerId}: Заказ {order.Id}, " + $"Цена: {order.Price}, Обработан: {order.IsProcessed}\n");
                }
                Console.WriteLine($"Поток {workerId} обработал заказ {order.Id}");
            }
            finally
            {
                semaphore.Release();
            }
        }
    });
}
Task.WaitAll(workers);
vc.IsRunning = false;
Console.WriteLine($"Обработано заказов: {processedCount}");
Console.WriteLine($"Общая стоимость: {totalPrice}");
Console.WriteLine($"Система работает: {vc.IsRunning}");
class Order
{
    public int Id { get; set; }
    public decimal Price { get; set; }
    public bool IsProcessed { get; set; }
}
class volatileClass
{
    public volatile bool IsRunning = true;
}