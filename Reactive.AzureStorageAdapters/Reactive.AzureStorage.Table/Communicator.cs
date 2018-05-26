
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Reactive.AzureStorage.Table
{
    /// <summary>
    /// An observable API to "Microsoft.WindowsAzure.Storage.Table"
    /// </summary>
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

        public IObservable<bool> CreateCloudTableAsync(string tableName)
        { 
            return Observable
                    .Return(_tableClient.Value.GetTableReference(tableName))
                    .SelectMany(tbRef => tbRef.CreateIfNotExistsAsync())
                    .FirstAsync();
        }

        public IObservable<TableResult> InsertOrReplaceAsync<T>(string tableName, T entity) where T : ITableEntity
        {
            return GetCloudTableAsync(tableName)
                    .Select(ct => (ct, op: TableOperation.InsertOrReplace(entity)))
                    .SelectMany(tple => tple.ct.ExecuteAsync(tple.op))
                    .FirstAsync();
        }

        public IObservable<TableResult> InsertOrReplaceAsync<T>(IEnumerable<T> entities, string tableName) where T : ITableEntity =>
            InsertOrReplaceAsync(entities.ToObservable(), tableName);

        public IObservable<TableResult> InsertOrReplaceAsync<T>(IObservable<T> entities, string tableName) where T : ITableEntity
        {
            return entities
                    .Zip(GetCloudTableAsync(tableName).Repeat(), (e, ct) => (ct, op: TableOperation.InsertOrReplace(e)))
                    .SelectMany(tple => tple.ct.ExecuteAsync(tple.op));
        }

        public IObservable<TableResult> InsertOrMergeAsync<T>(IEnumerable<T> entities, string tableName) where T : ITableEntity =>
            InsertOrMergeAsync(entities.ToObservable(), tableName);

        public IObservable<TableResult> InsertOrMergeAsync<T>(IObservable<T> entities, string tableName) where T : ITableEntity
        {
            return entities
                    .Zip(GetCloudTableAsync(tableName).Repeat(), (e, ct) => (ct, op: TableOperation.InsertOrMerge(e)))
                    .SelectMany(tple => tple.ct.ExecuteAsync(tple.op));
        }

        public IObservable<TableResult> InsertOrMergeAsync<T>(string tableName, T entity) where T : ITableEntity
        {
            return GetCloudTableAsync(tableName)
                    .Select(ct => (ct, op: TableOperation.InsertOrMerge(entity)))
                    .SelectMany(tple => tple.ct.ExecuteAsync(tple.op))
                    .FirstAsync();
        }

        public IObservable<TableResult> DeleteAsync<T>(string tableName, T entity) where T : ITableEntity
        {
            return GetCloudTableAsync(tableName)
                    .Select(tble => (tble, op: TableOperation.Delete(entity)))
                    .SelectMany(tple => tple.tble.ExecuteAsync(tple.op));
        }

        public IObservable<TableResult> TableBatchOperationAsync<T>(string tableName, T[] entities, Action<T, TableBatchOperation> action)
            where T : ITableEntity => BatchOperationAsync(tableName, entities, action);

        public IObservable<TableResult> BatchInsertOrReplaceAsync<T>(string tableName,  T[] entities) where T : ITableEntity =>
            BatchOperationAsync(tableName,  entities, (e, op) => op.InsertOrReplace(e));

        public IObservable<TableResult> BatchInsertOrMergeAsync<T>(string tableName,  T[] entities) where T : ITableEntity =>
            BatchOperationAsync(tableName,  entities, (e, op) => op.InsertOrMerge(e));

        public IObservable<TableResult> BatchDeleteAsync<T>(string tableName,  T[] entities) where T : ITableEntity =>
            BatchOperationAsync(tableName,  entities, (e, op) => op.Delete(e));


        private IObservable<TableResult> BatchOperationAsync<T>(string tableName, T[] entities, Action<T, TableBatchOperation> action)
            where T : ITableEntity
        {
            return Observable
                            .Return(new TableBatchOperation())
                            .Do(op => Array.ForEach(entities, e => action(e,op)))
                            .Zip(GetCloudTableAsync(tableName), (op, ct) => ct.ExecuteBatchAsync(op))
                            .SelectMany(tsk => tsk)
                            .SelectMany(results => results);
        }

        public IObservable<T> ReadAsync<T>(string tableName, string partitionKey, string rowKey) where T : ITableEntity
        { 
            return GetCloudTableAsync(tableName)
                        .Select(tble => (tble, TableOperation.Retrieve<T>(partitionKey, rowKey)))
                        .SelectMany(tple => tple.tble.ExecuteAsync(tple.Item2))
                        .Select(result => (T)result.Result);
        }

        public IObservable<T> ReadAsync<T>(string tableName, TableQuery<T> tableQuery) where T : ITableEntity, new()
        {
            return Observable.Create<T>(async observer =>
                    {
                        await PopulateObserverWithTableDataAsync(GetCloudTableAsync(tableName), tableQuery, null, observer);
                        return Disposable.Empty;
                    });
        }

        private async Task PopulateObserverWithTableDataAsync<T>(IObservable<CloudTable> cloudTable, TableQuery<T> tableQuery, TableContinuationToken tableContineousToken,
            IObserver<T> observer) where T : ITableEntity, new()
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
