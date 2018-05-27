
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;

namespace Reactive.AzureStorage.Table.FunctionalTest
{
    public class Customer
    {
        public int CustomerId { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }
    }

    public class CustomerEntity : TableEntity
    {
        public CustomerEntity()
        {

        }
        public CustomerEntity(string partitionKey, string rowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
        }

        public int CustomerId { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }
    }


    [TestClass]
    public class CommunicatorTest
    {
        private readonly string _accountName = "";
        private readonly string _accountKey = "";

        [TestMethod]
        public async Task InsertOrReplaceTest()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var entity = new CustomerEntity("Washington", "Redmond") { CustomerId = 100, FirstName = "Sam", LastName = "mas" };

            var result = await communicator.InsertOrReplaceAsync("USA", entity).FirstOrDefaultAsync();
        }

        [TestMethod]
        public async Task InsertOrReplaceObservableTest()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var entity = new CustomerEntity("Washington", "Redmond") { CustomerId = 100, FirstName = "Sam", LastName = "mas" };

            var result = await communicator.InsertOrReplaceAsync(Observable.Return(entity), "USA").FirstOrDefaultAsync();
        }

        [TestMethod]
        public async Task BatchInsertOrReplaceTest()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var entities = GetEntities("Partition_1");

            var results = await entities
                                    .ToObservable()
                                    .Buffer(100)
                                    .SelectMany(ets => communicator.BatchInsertOrReplaceAsync("USA", ets.ToArray()))
                                    .ToArray();
        }

        [TestMethod]
        public async Task BatchInsertOrMergeTest()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var entities = GetEntities("Partition_2");

            var results = await entities
                                    .ToObservable()
                                    .Buffer(100)
                                    .SelectMany(ets => 
                                        communicator.TableBatchOperationAsync("USA", ets.ToArray(),(e,op) => op.InsertOrMerge(e)))
                                    .ToArray();
        }

        [TestMethod]
        public async Task ReadIndividualEntityTest()
        {
            var entity = new Customer() { CustomerId = 100, FirstName = "Sam", LastName = "mas" };
            var communicator = new Communicator(_accountName, _accountKey);

            var tableEntity = entity.ToDynamicTableEntity("Washington", "Redmond");
            await communicator.InsertOrReplaceAsync("USA", tableEntity).FirstOrDefaultAsync();

            var result = await communicator.ReadAsync<DynamicTableEntity>("USA", "Washington", "Redmond");

            var customer = result.ToBusinessEntity<Customer>();
        }

        [TestMethod]
        public async Task BulkReadTest()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var tableQuery = new TableQuery<CustomerEntity>()
                                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, "Partition"));
            var timer = new Stopwatch();
            timer.Start();
            var results = await communicator.ReadAsync("USA", tableQuery) // Process the table operation continuously on a separate thread
                                .SelectMany(e => GetCustomer(e)) // Process the intermediate results on a separate thread soon after it becomes available
                                .ToArray();
            timer.Stop();

            var elapsedTime = timer.ElapsedMilliseconds;
        }

        public async Task Delete()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var tableQuery = new TableQuery<CustomerEntity>()
                                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, "DateRange"));

            var results = await communicator.ReadAsync("USA", tableQuery)
                                            .Buffer(100)
                                            .SelectMany(ets => communicator.BatchDeleteAsync("Table", ets.ToArray()))
                                            .ToArray();
        }


        private async Task<Customer> GetCustomer(CustomerEntity e)
        {
            await Task.Delay(5); // simulated delay
            return new Customer { CustomerId = e.CustomerId, FirstName = e.FirstName, LastName = e.LastName };
        }

        private CustomerEntity[] GetEntities(string partitionKey)
        {
            var entities = new List<CustomerEntity>();

            for (int i = 1; i <= 2500; i++)
            {
                entities.Add(new CustomerEntity(partitionKey, "RowKey_" + i.ToString())
                {
                    CustomerId = i,
                    FirstName = "FName_" + i.ToString(),
                    LastName = "LName_" + i.ToString()
                });
            }

            return entities.ToArray();
        }
    }
}
