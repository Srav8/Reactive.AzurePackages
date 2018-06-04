using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;

namespace Reactive.AzureStorage.Queue
{
    public class Communicator
    {
        private readonly Lazy<CloudQueueClient> _cloudQueueClient;

        public Communicator(string storageAccountName, string storageAccountKey)
        {
            StorageAccountName = string.IsNullOrEmpty(storageAccountName) ?
                                    throw new ArgumentNullException(nameof(storageAccountName)) :
                                    storageAccountName;

            StorageAccountKey = string.IsNullOrEmpty(storageAccountKey) ?
                throw new ArgumentNullException(nameof(storageAccountKey)) :
                storageAccountKey;

            var storageCredentials = new StorageCredentials(StorageAccountName, StorageAccountKey);

            _cloudQueueClient = new Lazy<CloudQueueClient>(() =>
                    new CloudStorageAccount(storageCredentials, true).CreateCloudQueueClient(), true);
        }

        public string StorageAccountName { get; }

        public string StorageAccountKey { get; }

        public IObservable<Unit> InsertQueueMessageAsync(string queueName, CloudQueueMessage cloudQueueMessage)
        {
            return GetQueueReferenceAsysnc(queueName)
                    .SelectMany(cq => Observable.FromAsync(() => cq.AddMessageAsync(cloudQueueMessage)));
        }

        public IObservable<CloudQueueMessage> GetQueueMessageAsync(string queueName)
        {
            var queueReference = GetQueueReferenceAsysnc(queueName).Repeat();
            return Observable.Create<CloudQueueMessage>(async observer =>
            {
                while(true)
                {
                    var cloudQueueMessage = await queueReference
                                                    //.TakeWhile(cq => cq.PeekMessageAsync().Result != null)
                                                    .SelectMany(qRef => qRef.GetMessageAsync())
                                                    .FirstOrDefaultAsync();
                    if (cloudQueueMessage != null)
                        observer.OnNext(cloudQueueMessage);
                    else
                    { 
                        observer.OnCompleted();
                        break;
                    }
                }
                return Disposable.Empty;
            });
        }

        private IObservable<CloudQueue> GetQueueReferenceAsysnc(string queueName)
        {
            return Observable
                        .Return(_cloudQueueClient.Value.GetQueueReference(queueName))
                        .SelectMany(async qRef => (qRef, await qRef.CreateIfNotExistsAsync()))
                        .Select(tple => tple.qRef)
                        .SingleAsync();
        }
    }
}
