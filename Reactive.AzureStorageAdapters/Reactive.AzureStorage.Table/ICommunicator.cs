using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace Reactive.AzureStorage.Table
{
    public interface ICommunicator
    {
        IObservable<T> Read<T>(string tableName, string partitionKey, string rowKey) where T : ITableEntity
    }
}
