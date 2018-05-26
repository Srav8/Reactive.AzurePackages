using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace Reactive.AzureStorage.Table
{
    public interface ICommunicator
    {
        string StorageAccountKey { get; }
        string StorageAccountName { get; }
        
        IObservable<bool> CreateCloudTableAsync(string tableName);

        IObservable<TableResult> InsertOrMergeAsync<T>(string tableName, T entity) where T : ITableEntity;
        IObservable<TableResult> InsertOrMergeAsync<T>(IEnumerable<T> entities, string tableName) where T : ITableEntity;
        IObservable<TableResult> InsertOrMergeAsync<T>(IObservable<T> entities, string tableName) where T : ITableEntity;

        IObservable<TableResult> InsertOrReplaceAsync<T>(string tableName, T entity) where T : ITableEntity;
        IObservable<TableResult> InsertOrReplaceAsync<T>(IEnumerable<T> entities, string tableName) where T : ITableEntity;
        IObservable<TableResult> InsertOrReplaceAsync<T>(IObservable<T> entities, string tableName) where T : ITableEntity;

        IObservable<T> ReadAsync<T>(string tableName, string partitionKey, string rowKey) where T : ITableEntity;
        IObservable<T> ReadAsync<T>(string tableName, TableQuery<T> tableQuery) where T : ITableEntity, new();

        IObservable<TableResult> DeleteAsync<T>(string tableName, T entity) where T : ITableEntity;

        IObservable<TableResult> BatchInsertOrMergeAsync<T>(string tableName, T[] entities) where T : ITableEntity;
        IObservable<TableResult> BatchInsertOrReplaceAsync<T>(string tableName, T[] entities) where T : ITableEntity;

        IObservable<TableResult> BatchDeleteAsync<T>(string tableName, T[] entities) where T : ITableEntity;
    }
}