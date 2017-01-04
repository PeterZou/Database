using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQL.Iterator
{
    public class UnionIterator : Iterator
    {
        Iterator Array1 { get; set; }

        Iterator Array2 { get; set; }

        Iterator Array3 { get; set; }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public object GetNext()
        {
            var temp = Array1.GetNext();

            while (temp != null)
            {
                temp = Array1.GetNext();
            }

            return Array3;
        }

        public void Open()
        {
            Array1.Open();
        }
    }
}
