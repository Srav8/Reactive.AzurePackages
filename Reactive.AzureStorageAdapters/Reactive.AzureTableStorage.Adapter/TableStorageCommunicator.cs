using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Reactive.AzureTableStorage.Adapter
{
    public class TableStorageCommunicator : ITableStorageCommunicator
    {
        private readonly string _storageAccountName;
        private readonly string _storageAccountKey;

        private readonly Lazy<CloudTableClient> _tableClient;

        public TableStorageCommunicator(string storageAccountName, string storageAccountKey)
        {
            _storageAccountName = string.IsNullOrEmpty(storageAccountName) ?
                                    throw new ArgumentNullException(nameof(storageAccountName)) :
                                    storageAccountName;

            _storageAccountKey = string.IsNullOrEmpty(storageAccountKey) ?
                throw new ArgumentNullException(nameof(storageAccountKey)) :
                storageAccountKey;

            var storageCredentials = new StorageCredentials(_storageAccountName, _storageAccountKey);

            _tableClient = new Lazy<CloudTableClient>(
                () => new CloudStorageAccount(storageCredentials, true).CreateCloudTableClient(), false);
        }

        public string GetTableData() => "someData";
    }
}
