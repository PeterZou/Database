using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database
{
    public static class ListExtendsion
    {
        public static void AddCus(this List<DataTuple> list, DataTuple item, TupleCmp com)
        {

            var last = list.Where(p => !com.Compare(p, item));
            if (last != null && last.ToList().Count != 0)
            {
                var lastItem = last.Last();
                int index = list.IndexOf(lastItem);
                list.Insert(index + 1, item);
            }
            else
            {
                list.Insert(0, item);
            }
        }
    }
}
