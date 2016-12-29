using System.Linq;
using System.Text;
using System;
using System.Collections.Generic;
using Database.IndexManage.BPlusTree;
using Database.IndexManage.IndexValue;

namespace Database
{
    class Program
    {
        static void Main(string[] args)
        {
            BPlusTree<int, NodeInt> bt = new BPlusTree<int, NodeInt>(4);

            var list = new List<NodeInt>();

            for (int i = 1; i <= 6; i++)
            {
                if (i != 9999)
                {
                    var node = new NodeInt(i);
                    bt.Insert(node);
                    bt.InsertRepair(true,null,null);
                }
            }
            bt.TraverseForword(bt.Root, bt.TraverseOutput);

            Console.ReadKey();
        }
    }
}
