# Reactive.AzurePackages
        A reactive interface for Azure storage SDK - a natural programming model for asynchronous data streams.
## Features
### Azure Storage
#### Table
While working with azure storage, its throughput has to be taken into consideration and APIs need to be designed 
to enable and encourage better utilization of threads. The current throughput limitation on azure table storage 
is upto 2000 entities per second for single table partition with 1KiB entities 
[AzureStorageScalabilityandPerformanceTargets][1]. 

This will further be affected by the partitioning node server distribution, network bandwidth, message size and 
the table query. All these factors results into non deterministic nature of the table operation and the timeline.
    
Azure provides an indicator/tracking pointer on the table operation as continuous-token along with intermediate 
results. As long as it is having a  valid pointer (from where next operation - say a 'read' begins), the operation 
is not yet completed. With this approach, the operation results will be provided as packets/chunks over a period 
of time. As the each chunk requires a network round trip and should move linearly on the line connected by the 
token pointers, processing a chunk soon after it arrives improves performance and uses CPU cores optimally. 
Providing table-operation-result chunks as reactive stream will build the scaffolding structure for smooth 
construction of the processing pipeline. 

[1]: https://docs.microsoft.com/en-us/azure/storage/common/storage-scalability-targets

Example
-------
```csharp
public async Task BulkReadTest()
{
    var communicator = new Communicator(_accountName, _accountKey);
    var tableQuery = new TableQuery<CustomerEntity>()
                          .Where(TableQuery
                           .GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, "Partition"));

    var results = await communicator
                            // Processes the table operation continuously on a separate thread
                            .ReadAsync("USA", tableQuery) 
                            // Processes the intermediate results on a separate thread soon 
                            // after it becomes available
                            .SelectMany(e => GetCustomer(e)) 
                            .ToArray();
}

private async Task<Customer> GetCustomer(CustomerEntity e)
{
    await Task.Delay(5); // simulated delay
    return new Customer { CustomerId = e.CustomerId, FirstName = e.FirstName, LastName = e.LastName };
}
```
The reactive stream can further be extended all the way up, like 
```csharp
public IObservable<Customer> GetGoldCustomers()
{
    var communicator = new Communicator(_accountName, _accountKey);
    var tableQuery = new TableQuery<CustomerEntity>()
                          .Where(TableQuery
                           .GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Partition"));

    return communicator.ReadAsync("USA", tableQuery)
                        .SelectMany(e => GetCustomer(e))
                        .SelectMany(c => IsValidCustomer(c))
                        .Where(tple => tple.Item2 == true)
                        .SelectMany(tple => IsGoldCustomer(tple.Item1))
                        .Where(tple => tple.Item2 == true)
                        .Select(tple => tple.Item1);
}

private async Task<(Customer,bool)> IsValidCustomer(Customer c) => 
    await Task.Delay(5).ContinueWith(_ => (c,true));

private async Task<(Customer, bool)> IsGoldCustomer(Customer c) => 
    await Task.Delay(5).ContinueWith(_ => (c, true));
```

This stream based programming model helps join query based deletion of entities like
```csharp
public async Task Delete()
{
    var communicator = new Communicator(_accountName, _accountKey);
    var tableQuery = new TableQuery<CustomerEntity>()
                            .Where(TableQuery
                             .GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, "DateRange"));

    var results = await communicator
                         .ReadAsync("USA", tableQuery)
                         .Buffer(100)
                         .SelectMany(ets => communicator.BatchDeleteAsync("Table", ets.ToArray()))
                         .ToArray();
}
```
Important Points
----------------
As this is considered as the tight skin on top of the `WindowsAzure.Storage` sdk, 
it doesn't take additional responsibilities on input data. It is a pass through 
layer and provides reactive api to the caller. Additional customized layers can be 
added as decorators to this.

* Validation of parameters like null or empty table name are delegated to sdk
* Validation of common partition key and batch size of 100 is delegated to sdk. 
* Utilities are provided to serialize and deserialzie business entities to/from table 
    entities using `TableEntity` class. Test project contains examples of its usage
* Using `DataFlow` blocks with reactive streams results in more modular models

License
-------
    This software is open source
