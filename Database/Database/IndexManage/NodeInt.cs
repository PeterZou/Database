using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public class NodeInt : INode<int>
    {
        public NodeInt(int key) :base(key)
        {
        }

        public override string ToString()
        {
            return base.Key.ToString();
        }
    }
}
