using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Reactive.AzureTableStorage.Adapter
{
    public interface ITableStorageCommunicator
    {
        string GetTableData();
    }
}
