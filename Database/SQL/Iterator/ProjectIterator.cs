using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQL.Iterator
{
    public class ProjectIterator : Iterator
    {
        Iterator Array { get; set; }

        Func<object, object> func;

        public void Close()
        {
            throw new NotImplementedException();
        }

        public object GetNext()
        {
            object obj = Array.GetNext();

            return func(obj);
        }

        public void Open()
        {
            Array.Open();
        }
    }
}
