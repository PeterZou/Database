using Database.Const;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.IndexValue
{
    public class NodeDisk<TK>
    {
        public int length;

        // leaf:0,branch:1
        public int isLeaf;

        public int capacity;

        public int height;

        public List<TK> keyList;

        public List<RID> childRidList;

        // leaf node
        // default(-1,-1)
        public RID leftRID;
        public RID rightRID;
    }
}
