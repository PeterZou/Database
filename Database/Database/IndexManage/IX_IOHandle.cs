using Database.IndexManage.BPlusTree;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.IndexManage.IndexValue;

namespace Database.IndexManage
{
    // wrapper the B+tree datastructure within the IO opeartion
    // TK:ConstProperty,TV:RID,ConstProperty
    public class IX_IOHandle<TK,TV>
        where TV : INode<TK>
        where TK : IComparable<TK>
    {
        private ProviderContext<TK, TV> providerContext;

        private RM_FileHandle rmp;

        public IX_IOHandle(RM_FileHandle rmp)
        {
            this.rmp = rmp;
        }

        public Node<TK, TV> ImportToMemory(RID rid)
        {
            RM_Record record = rmp.GetRec(rid);

            char[] data = record.GetData();

            // leaf or not


            return null;
        }

        public RID InsertNodeToDisk(Node<TK, TV> node)
        {
            if (node.IsLeaf)
            {
                //NotLeaf<TK> nl = new NotLeaf<TK>();
                //nl.capacity = node.Values.Count;
                //nl.keyList = node.Values.ToList();
                //foreach (var v in node.ChildrenNodes)
                //{
                //    nl.childRidList.Add();
                //}
                
            }
            else
            {

            }

            return default(RID);
        }

        public RID DeleteNodeToDisk(Node<TK, TV> node)
        {
            return default(RID);
        }
    }
}
