
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Reactive.AzureStorage.Table
{
    public class Communicator : ICommunicator
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
                 new CloudStorageAccount(storageCredentials, true).CreateCloudTableClient(), true);
        }

        public string StorageAccountName { get; }

        public string StorageAccountKey { get; }

        public int BatchSize { get; } = 100;

        public IObservable<bool> CreateCloudTableAsync(string tableName)
        { 
            return Observable
                    .Return(_tableClient.Value.GetTableReference(tableName))
                    .SelectMany(tbRef => tbRef.CreateIfNotExistsAsync())
                    .FirstAsync();
        }

        public IObservable<TableResult> InsertOrReplaceAsync<T>(string tableName, T entity) where T : TableEntity
        {
            return GetCloudTableAsync(tableName)
                    .Select(ct => (ct, op: TableOperation.InsertOrReplace(entity)))
                    .SelectMany(tple => tple.ct.ExecuteAsync(tple.op))
                    .FirstAsync();
        }

        public IObservable<TableResult> InsertOrMergeAsync<T>(string tableName, T entity) where T : TableEntity
        {
            return GetCloudTableAsync(tableName)
                    .Select(ct => (ct, op: TableOperation.InsertOrMerge(entity)))
                    .SelectMany(tple => tple.ct.ExecuteAsync(tple.op))
                    .FirstAsync();
        }

        public IObservable<TableResult> BatchInsertOrReplaceAsync<T>(string tableName, string partitionKey, T[] entities) where T : TableEntity =>
            BatchOperationAsync(tableName, partitionKey, entities, (e, op) => op.InsertOrReplace(e));

        public IObservable<TableResult> BatchInsertOrMergeAsync<T>(string tableName, string partitionKey, T[] entities) where T : TableEntity =>
            BatchOperationAsync(tableName, partitionKey, entities, (e, op) => op.InsertOrMerge(e));

        private IObservable<TableResult> BatchOperationAsync<T>(string tableName, string partitionKey, T[] entities, Action<T, TableBatchOperation> action)
            where T : TableEntity
        {
            var cloudTable = GetCloudTableAsync(tableName).SingleAsync().Repeat();

            return entities.ToObservable()
                                    .Buffer(BatchSize)
                                    .Select(batch => (batch, batchOp: new TableBatchOperation()))
                                    .Do(tple => Array.ForEach(tple.batch.ToArray(), e => action(e, tple.batchOp)))
                                    .Select(tple => tple.batchOp)
                                    .Zip(cloudTable, (op, ct) => ct.ExecuteBatchAsync(op))
                                    .SelectMany(tsk => tsk)
                                    .SelectMany(results => results);
        }

        public IObservable<T> ReadAsync<T>(string tableName, string partitionKey, string rowKey) where T : TableEntity
        { 
            return GetCloudTableAsync(tableName)
                        .Select(tble => (tble, TableOperation.Retrieve<TableEntity>(partitionKey, rowKey)))
                        .SelectMany(tple => tple.tble.ExecuteAsync(tple.Item2))
                        .Select(result => (T)result.Result);
        }

        public IObservable<T> ReadAsync<T>(string tableName, TableQuery<T> tableQuery) where T : TableEntity, new()
        {
            return Observable.Create<T>(async observer =>
                    {
                        await PopulateObserverWithTableDataAsync(GetCloudTableAsync(tableName), tableQuery, null, observer);
                        return Disposable.Empty;
                    });
        }

        public IObservable<TableResult> DeleteAsync<T>(string tableName, T entity) where T: TableEntity
        {
            return GetCloudTableAsync(tableName)
                    .Select(tble => (tble, op: TableOperation.Delete(entity)))
                    .SelectMany(tple => tple.tble.ExecuteAsync(tple.op));
        }

        private async Task PopulateObserverWithTableDataAsync<T>(IObservable<CloudTable> cloudTable, TableQuery<T> tableQuery, TableContinuationToken tableContineousToken,
            IObserver<T> observer) where T : TableEntity, new()
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
                            .Select(e => (TableEntity)e)
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

        private IObservable<CloudTable> GetCloudTableAsync(string tableName)
        {
            return Observable
                    .Return(_tableClient.Value.GetTableReference(tableName))
                    .SelectMany(async tbRef => (tbRef, await tbRef.CreateIfNotExistsAsync()))
                    .Select(tple => tple.tbRef)
                    .SingleAsync();
        }
    }
}
