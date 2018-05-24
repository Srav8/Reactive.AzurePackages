using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
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

        public CustomerEntity[] GetEntities()
        {
            var entities = new List<CustomerEntity>();

            for(int i = 1; i<=2500; i++)
            {
                entities.Add(new CustomerEntity("Partition_2", "RowKey_" + i.ToString())
                {
                    CustomerId = i,
                    FirstName = "FName_" + i.ToString(),
                    LastName = "LName_" + i.ToString()
                });
            }

            return entities.ToArray();
        }

        [TestMethod]
        public async Task InsertOrReplaceTest()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var entity = new CustomerEntity("Washington", "Redmond") { CustomerId = 100, FirstName = "Sam", LastName = "mas" };

            var result = await communicator.InsertOrReplace("USA", entity).FirstOrDefaultAsync();
        }

        [TestMethod]
        public async Task BatchInsertOrReplace()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var entities = GetEntities();

            var results = await communicator.BatchInsertOrReplace("USA", "Partition_1", entities).ToArray();
        }

        [TestMethod]
        public async Task BatchInsertOrMerge()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var entities = GetEntities();

            var results = await communicator.BatchInsertOrMerge("USA", "Partition_2", entities).ToArray();
        }

        [TestMethod]
        public async Task Read()
        {
            var communicator = new Communicator(_accountName, _accountKey);
            var tableQuery = new TableQuery<CustomerEntity>()
                                    .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Partition_1"));
            var results = await communicator.ReadAsync("USA", tableQuery)
                                        .Select(e => new Customer { CustomerId = e.CustomerId, FirstName = e.FirstName, LastName = e.LastName })
                                        .ToArray();
        }
    }
}
