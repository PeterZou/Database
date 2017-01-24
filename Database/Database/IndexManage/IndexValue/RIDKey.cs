using Database.IndexManage.BPlusTree;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.IndexValue
{
    public class RIDKey<TK> : INode<TK>
    {
        public RID Rid { get; set; }

        public RIDKey(RID rid,TK key) : base(key)
        {
            this.Rid = rid;
        }

        public RIDKey() : base()
        {
        }
    }
}
