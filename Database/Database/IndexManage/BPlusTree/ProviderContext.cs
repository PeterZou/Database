using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public class ProviderContext<TK, TV>
        where TV : INode<TK>
        where TK : IComparable<TK>
    {
        private Iprovider<TK, TV> ip;

        public Action<Node<TK, TV>> ActionToPerform { get; set; }

        public Node<TK, TV> GetERoot()
        {
            return ip.Root;
        }

        // false:not reset,true:reset
        public void Reset(TV value)
        {
            ip.Reset(value);
        }

        public ProviderContext(Iprovider<TK,TV> ip)
        {
            this.ip = ip;
        }

        public void Delete(TK key)
        {
            ip.Delete(key);
        }

        public void Insert(TV value,Action<Node<TK, TV>> func)
        {
            ip.Insert(value, func);
        }

        public void Search(TK key)
        {
            ip.Search(key);
        }

        public void Traverse(Node<TK, TV> node)
        {
            ip.Traverse(node, ActionToPerform);
        }
    }
}
