using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQL.Iteration
{
    public class SelectIterator<T> : Iterator<T>
    {
        int index = -1;
        T[] Array { get; set; }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public object GetNext()
        {
            index++;
            return Array[index];
        }

        public void Open(IEnumerable<T> array)
        {
            this.Array = array.ToArray();
        }
    }
}
