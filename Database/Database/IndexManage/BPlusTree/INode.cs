using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public abstract class INode<TK>
    {
        public TK Key { set; get; }

        protected INode(TK key)
        {
            this.Key = key;
        }
    }
}
