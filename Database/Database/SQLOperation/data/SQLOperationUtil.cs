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

        public static int CompareTwoCharArray(char[] one, char[] two)
        {
            int flag = 0;
            if (one.Length != two.Length) flag = 1;
            for (int i = 0; i < one.Length; i++)
            {
                if (one[i] != two[i])
                    flag = 1;
            }
            return flag;
        }
    }
}
