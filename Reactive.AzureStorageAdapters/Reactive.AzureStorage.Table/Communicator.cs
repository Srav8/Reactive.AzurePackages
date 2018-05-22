using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq.Expressions;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Reactive.AzureStorage.Table
{
    public class Communicator
    {
        private readonly Lazy<CloudTableClient> _tableClient;

        public Communicator(string storageAccountName, string storageAccountKey)
        {
            StorageAccountName = string.IsNullOrEmpty(storageAccountName) ?
                                    throw new ArgumentNullException(nameof(storageAccountName)) :
                                    storageAccountName;

            StorageAccountKey = string.IsNullOrEmpty(storageAccountKey) ?
                throw new ArgumentNullException(nameof(storageAccountKey)) :
                storageAccountKey;

            var storageCredentials = new StorageCredentials(StorageAccountName, StorageAccountKey);

            _tableClient = new Lazy<CloudTableClient>(() =>
                 new CloudStorageAccount(storageCredentials, true).CreateCloudTableClient(),true);
        }

        public string StorageAccountName { get; }

        public string StorageAccountKey { get; }

        public IObservable<T> Read<T>(string tableName, string partitionKey, string rowKey) where T: ITableEntity =>
            GetCloudTable(tableName)
                        .Select(tble => (tble, TableOperation.Retrieve<ITableEntity>(partitionKey, rowKey)))
                        .SelectMany(tple => tple.tble.ExecuteAsync(tple.Item2))
                        .Select(result => (T)result.Result);

        public IObservable<T> ReadAsync<T>(string tableName, TableQuery tableQuery) where T: ITableEntity =>
            Observable.Create<T>(async observer =>
            {
                await PopulateObserverWithTableDataAsync(GetCloudTable(tableName), tableQuery, null, observer);
                return Disposable.Empty;
            });

        private async Task PopulateObserverWithTableDataAsync<T>(IObservable<CloudTable> cloudTable, TableQuery tableQuery, TableContinuationToken tableContineousToken,
            IObserver<T> observer) where T : ITableEntity
        {
            try
            {
                do
                {
                    await cloudTable
                            .SelectMany(ct => ct.ExecuteQuerySegmentedAsync(tableQuery, tableContineousToken))
                            .Where(seg => seg != null)
                            .Do(seg => tableContineousToken = seg.ContinuationToken)
                            .Select(seg => seg.Results)
                            .Where(res => res != null)
                            .SelectMany(res => res)
                            .Select(e => (ITableEntity)e)
                            .Select(e => (T)e)
                            .ForEachAsync(e => observer.OnNext(e));

                } while (tableContineousToken != null);

                observer.OnCompleted();
            }
            catch (Exception exception)
            {
                observer.OnError(exception);
            }
        }


        private IObservable<CloudTable> GetCloudTable(string tableName) =>
            Observable
                    .Return(_tableClient.Value.GetTableReference(tableName))
                    .SelectMany(async tbRef => (tbRef, await tbRef.CreateIfNotExistsAsync()))
                    .Select(tple => tple.tbRef);

    }
}
