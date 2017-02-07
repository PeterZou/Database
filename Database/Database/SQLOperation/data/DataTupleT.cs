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

        public T GetData(int attrOffset, int attrLength, Func<string, T> ConvertStringToT)
        {
            char[] value = new char[attrLength];

            for (int i = 0; i < attrLength; i++)
            {
                value[i] = data[i + attrOffset];
            }

            return ConvertStringToT(new string(value));
        }
    }
}
