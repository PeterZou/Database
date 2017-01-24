using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation.data
{
    public static class SQLOperationUtil
    {
        public static void SetChars(char[] buf, char[] lbuf, List<DataAttrInfo> attrs, List<DataAttrInfo> lattrs)
        {
            for (int i = 0; i < attrs.Count; i++)
            {
                for (int j = attrs[i].offset; j < attrs[i].attrLength; j++)
                {
                    buf[j] = lbuf[j];
                }
            }
        }
    }
}
