using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public interface Iprovider<TK, TV>
        where TV : INode<TK>
        where TK : IComparable<TK>
    {
        void Reset(TV value);

        Node<TK, TV> Root { get; set; }

        void Search(TK key);

        void Insert(TV value, Action<Node<TK, TV>> func);

        void Delete(TK key);

        void Traverse(Node<TK, TV> node, Action<Node<TK, TV>> action);
    }
}
