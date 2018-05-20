using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq.Expressions;
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

            _tableClient = new Lazy<CloudTableClient>(
                () => new CloudStorageAccount(storageCredentials, true).CreateCloudTableClient(),true);
        }

        public string StorageAccountName { get; }

        public string StorageAccountKey { get; }

        public IObservable<T> Read<T>(string tableName, string partitionKey, string rowKey) where T: ITableEntity =>
            GetCloudTable(tableName)
                        .Select(tble => (tble, TableOperation.Retrieve<ITableEntity>(partitionKey, rowKey)))
                        .SelectMany(tple => tple.tble.ExecuteAsync(tple.Item2))
                        .Select(result => (T)result.Result);

        public IObservable<T> Read<T>(string tableName, Expression<Func<ITableEntity, bool>> filter) where T: ITableEntity
        {
            
            return null;
        }


        private IObservable<CloudTable> GetCloudTable(string tableName) =>
            Observable
                    .Return(_tableClient.Value.GetTableReference(tableName))
                    .SelectMany(async tbRef => (tbRef, await tbRef.CreateIfNotExistsAsync()))
                    .Select(tple => tple.tbRef);

    }
}
