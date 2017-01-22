using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.IndexManage.BPlusTree;
using Database.RecordManage;

namespace Database.IndexManage.IndexValue
{
    public class NodeRIDInt : INode<int>
    {
        private RID rid;

        public NodeRIDInt(int key, RID rid) : base(key)
        {
            this.rid = rid;
        }

        public override string ToString()
        {
            return "RID is: " + this.rid + "num is " + base.Key;
        }
    }
}
