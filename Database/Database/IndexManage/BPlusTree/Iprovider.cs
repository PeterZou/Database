using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public interface Iprovider<TK, TV>
    {
        Node<TK, TV> Reset(TV value);

        void Search(TK key);

        void Insert(TV value);

        void Delete(TK key);

        void Traverse(Node<TK, TV> node, Action<Node<TK, TV>> action);
    }
}
