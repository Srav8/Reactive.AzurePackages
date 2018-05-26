using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Reactive.AzureStorage.Table
{
    public interface ICommunicator
    {
        int BatchSize { get; }
        string StorageAccountKey { get; }
        string StorageAccountName { get; }

        IObservable<TableResult> BatchInsertOrMergeAsync<T>(string tableName, string partitionKey, T[] entities) where T : ITableEntity;
        IObservable<TableResult> BatchInsertOrReplaceAsync<T>(string tableName, string partitionKey, T[] entities) where T : ITableEntity;
        IObservable<bool> CreateCloudTableAsync(string tableName);
        IObservable<TableResult> DeleteAsync<T>(string tableName, T entity) where T : ITableEntity;
        IObservable<TableResult> InsertOrMergeAsync<T>(string tableName, T entity) where T : ITableEntity;
        IObservable<TableResult> InsertOrReplaceAsync<T>(string tableName, T entity) where T : ITableEntity;
        IObservable<T> ReadAsync<T>(string tableName, string partitionKey, string rowKey) where T : ITableEntity;
        IObservable<T> ReadAsync<T>(string tableName, TableQuery<T> tableQuery) where T : ITableEntity, new();
    }
}