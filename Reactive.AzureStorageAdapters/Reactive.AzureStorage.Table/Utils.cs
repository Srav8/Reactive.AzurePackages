
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Reactive.AzureStorage.Table
{
    public static class Utils
    {
        public static DynamicTableEntity ToDynamicTableEntity(this object entity, string partitionKey, string rowKey)
        {
            var props = TableEntity.Flatten(entity, new OperationContext());
            return new DynamicTableEntity(partitionKey, rowKey, null, props);            
        }

        public static T ToBusinessEntity<T>(this DynamicTableEntity dynamicTableEntity) =>
            TableEntity.ConvertBack<T>(dynamicTableEntity.Properties, new OperationContext());
    }
}
