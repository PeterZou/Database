using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public class NodeInt : INode<int>
    {
        private string nn = "TV";

        public NodeInt(int key) :base(key)
        {
        }

        public override string ToString()
        {
            return nn + "num is " + base.Key;
        }
    }
}
