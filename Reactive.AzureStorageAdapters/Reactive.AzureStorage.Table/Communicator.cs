using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Reactive.AzureStorage.Table
{
    public class Communicator
    {
        private readonly string _storageAccountName;
        private readonly string _storageAccountKey;

        private readonly Lazy<CloudTableClient> _tableClient;

        public Communicator(string storageAccountName, string storageAccountKey)
        {
            _storageAccountName = string.IsNullOrEmpty(storageAccountName) ?
                                    throw new ArgumentNullException(nameof(storageAccountName)) :
                                    storageAccountName;

            _storageAccountKey = string.IsNullOrEmpty(storageAccountKey) ?
                throw new ArgumentNullException(nameof(storageAccountKey)) :
                storageAccountKey;

            var storageCredentials = new StorageCredentials(_storageAccountName, _storageAccountKey);

            _tableClient = new Lazy<CloudTableClient>(
                () => new CloudStorageAccount(storageCredentials, true).CreateCloudTableClient(),true);
        }

        public string StorageAccountName { get => _storageAccountName; }

        public string StorageAccountKey { get => _storageAccountKey; }

        public IObservable<T> Read<T>(string tableName, string partitionKey, string rowKey) where T: ITableEntity =>
            GetCloudTableAsync(tableName)
                        .Select(tble => (tble, TableOperation.Retrieve<DynamicTableEntity>(partitionKey, rowKey)))
                        .SelectMany(tple => tple.tble.ExecuteAsync(tple.Item2))
                        .Select(result => (T)result.Result);


        private IObservable<CloudTable> GetCloudTableAsync(string tableName) =>
            Observable
                    .Return(_tableClient.Value.GetTableReference(tableName))
                    .SelectMany(async tbRef => (tbRef, await tbRef.CreateIfNotExistsAsync()))
                    .Select(tple => tple.tbRef);

    }
}
