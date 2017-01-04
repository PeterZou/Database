using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQL.Iterator
{
    public interface Iterator
    {
        void Open();
        void Close();
        object GetNext();
    }
}
