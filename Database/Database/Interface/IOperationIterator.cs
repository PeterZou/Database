using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.Interface
{
    public interface IOperationIterator
    {
        void Open();

        object GetNext();

        void Close();

    }
}
