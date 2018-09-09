using System;
using System.Reactive;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Reactive.AzureStorage.Queue
{
    public interface ICommunicator
    {
        string StorageAccountKey { get; }
        string StorageAccountName { get; }

        IObservable<CloudQueueMessage> GetQueueMessageAsync(string queueName);
        IObservable<Unit> InsertQueueMessageAsync(string queueName, CloudQueueMessage cloudQueueMessage);
    }
}