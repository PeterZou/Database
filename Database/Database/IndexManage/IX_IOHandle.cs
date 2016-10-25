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
    public class IX_IOHandle<TK>
        where TK : IComparable<TK>
    {
        private RM_FileHandle rmp;

        public IX_IOHandle(RM_FileHandle rmp)
        {
            this.rmp = rmp;
        }

        public void InsertImportToMemory(RID rid)
        {
            InsertImportToMemory(rid, null);
        }

        // location of the orignal node may not set in right 
        public void InsertImportToMemory(RID rid, Node<TK, RIDKey<TK>> parent)
        {
            RM_Record record = rmp.GetRec(rid);

            char[] data = record.GetData();

            // leaf or not
            NodeDisk<TK> nodeDisk = ConvertToNodeDisk(data);

            // set the node link to the parent
            Node<TK, RIDKey<TK>> node = ConvertNodeDiskToNode(nodeDisk, parent);

            parent.ChildrenNodes.Add(node);
        }

        public RID InsertExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            var nodeDisk = ConvertNodeToNodeDisk(node);
            char[] data = ConvertToArray(nodeDisk);
            // set the nl to the disk
            return rmp.InsertRec(data);
        }

        public void DeleteExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            rmp.DeleteRec(node.CurrentRID.Rid);
        }

        private Node<TK, RIDKey<TK>> ConvertNodeDiskToNode(NodeDisk<TK> nodeDisk, Node<TK, RIDKey<TK>> parent)
        {
            Node<TK, RIDKey<TK>> node = new Node<TK, RIDKey<TK>>();
            // leaf:0,branch:1
            if (nodeDisk.isLeaf == 0)
            {
                node.IsLeaf = true;

                // put the property to the leaf
                for (int i = 0; i < nodeDisk.capacity; i++)
                {
                    node.Property.Add(new RIDKey<TK>(nodeDisk.childRidList[i],nodeDisk.keyList[i]));
                }
            }
            else
            {
                node.IsLeaf = false;
            }
            foreach (var v in nodeDisk.keyList)
                node.Values.Add(v);
            return null;
        }

        // TODO
        private NodeDisk<TK> ConvertToNodeDisk(char[] data)
        {
            return null;
        }

        private NodeDisk<TK> ConvertNodeToNodeDisk(Node<TK, RIDKey<TK>> node)
        {
            NodeDisk<TK> nl = new NodeDisk<TK>();
            // leaf:0,branch:1
            nl.isLeaf = node.IsLeaf == true ? 0 : 1;
            nl.capacity = node.Values.Count;
            nl.keyList = node.Values.ToList();
            foreach (var v in node.Property)
            {
                nl.childRidList.Add(v.Rid);
            }
            nl.length = GetLength(nl);

            return nl;
        }

        // TODO
        private char[] ConvertToArray(NodeDisk<TK> nl)
        {
            return null;
        }

        // TODO
        private int GetLength(NodeDisk<TK> nl)
        {
            return 0;
        }
    }
}
