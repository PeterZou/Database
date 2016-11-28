using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.RecordManage
{
    public class RM_Record
    {
        public int recordSize;
        public RID rid;
        public char[] data;
        
        public RM_Record()
        {
            rid = new RID(-1, -1);
            recordSize = -1;
        }

        public void Set(char[] pData, int size, RID ridTmp)
        {
            if (recordSize != -1 && (size != recordSize)) throw new Exception();
            recordSize = size;
            rid = ridTmp;
            if (data == null) data = new char[size];
            for (int i = 0; i < size; i++)
            {
                data[i] = pData[i];
            }
        }

        public char[] GetData()
        {
            if (data == null || recordSize == -1) throw new Exception();
            return data;
        }

        public RID GetRid()
        {
            if (data == null || recordSize == -1) throw new Exception();
            return rid;
        }
    }
}
