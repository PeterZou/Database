using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation.data
{
    public class DataTupleT<T> : DataTuple
    {
        public DataTupleT(int count, int length) : base(count,length)
        {

        }

        public T GetData(string value,Func<string,T> ConvertStringToT)
        {
            return ConvertStringToT(value);
        }
    }
}
