using Database.Const;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.IndexValue
{
    public class NotLeaf<TK>
    {
        public int capacity;

        public List<TK> keyList;

        public List<RID> childRidList;
    }
}
