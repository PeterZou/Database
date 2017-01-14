using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQL.Iteration
{
    public class ProjectIterator<T> : Iterator<T>
    {
        public void Close()
        {
            throw new NotImplementedException();
        }

        public object GetNext()
        {
            
        }

        public void Open(IEnumerable<T> array)
        {
            
        }
    }
}
