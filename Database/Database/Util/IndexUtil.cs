using Database.Const;
using Database.FileManage;
using Database.IndexManage;
using Database.IndexManage.BPlusTree;
using Database.IndexManage.IndexValue;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.Util
{
    public static class IndexUtil<TK>
        where TK : IComparable<TK>
    {
        public static Tuple<Node<TK, RIDKey<TK>>, List<RID>> ConvertNodeDiskToNode(NodeDisk<TK> nodeDisk, RID rid, Func<TK> CreatNewTK)
        {
            List<RID> ridlist = null;
            Node<TK, RIDKey<TK>> node = new Node<TK, RIDKey<TK>>();
            node.CurrentRID = new RIDKey<TK>(rid, CreatNewTK());
            node.Height = nodeDisk.height;

            // leaf:0,branch:1
            if (nodeDisk.isLeaf == 0)
            {
                node.IsLeaf = true;

                // put the property to the leaf
                for (int i = 0; i < nodeDisk.capacity; i++)
                {
                    node.Property.Add(new RIDKey<TK>(nodeDisk.childRidList[i], nodeDisk.keyList[i]));
                }
            }
            else
            {
                node.IsLeaf = false;
                ridlist = new List<RID>();
                foreach (var v in nodeDisk.childRidList)
                {
                    ridlist.Add(v);
                }
            }
            foreach (var v in nodeDisk.keyList)
                node.Values.Add(v);
            return new Tuple<Node<TK, RIDKey<TK>>, List<RID>>(node, ridlist);
        }

        public static NodeDisk<TK> ConvertNodeToNodeDisk(Node<TK, RIDKey<TK>> node)
        {
            NodeDisk<TK> nl = new NodeDisk<TK>();
            // leaf:0,branch:1
            nl.isLeaf = node.IsLeaf == true ? 0 : 1;
            nl.capacity = node.Values.Count;
            nl.keyList = node.Values.ToList();
            nl.height = node.Height;

            foreach (var v in node.Property)
            {
                nl.childRidList.Add(v.Rid);
            }
            nl.length = GetLength(nl);

            return nl;
        }

        public static int GetLength(NodeDisk<TK> nl)
        {
            int length = 0;

            length += 3 * ConstProperty.Int_Size + 1;

            // TODO Set TK is int
            length += nl.capacity * ConstProperty.Int_Size;
            length += nl.capacity * ConstProperty.RM_Page_RID_SIZE + ConstProperty.RM_Page_RID_SIZE;
            return length;
        }

        public static NodeDisk<TK> ConvertToNodeDisk(char[] data, Func<string, TK> ConverStringToTK)
        {
            string dataStr = new string(data);
            NodeDisk<TK> node = new NodeDisk<TK>();
            node.length = Convert.ToInt32(dataStr.Substring(0, ConstProperty.Int_Size));
            node.isLeaf = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size, 1));
            node.capacity = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
            node.height = Convert.ToInt32(dataStr.Substring(2 * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));

            for (int i = 0; i < node.capacity; i++)
            {
                TK tmp = ConverStringToTK(dataStr.Substring((3 + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                // TODO
                node.keyList[i] = tmp;
            }
            for (int i = 0; i < node.capacity; i++)
            {
                // TODO
                int pageNum = Convert.ToInt32(dataStr.Substring(
                    (3 + node.capacity + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                int slotNum = Convert.ToInt32(dataStr.Substring(
                    (3 + node.capacity + i + 1) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                node.childRidList[i] = new RID(pageNum, slotNum);
            }
            return node;
        }

        public static char[] ConvertToArray(NodeDisk<TK> nl)
        {
            char[] data = new char[nl.length];
            // length
            FileUtil.ReplaceTheNextFree(data, nl.length, 0);
            // isLeaf
            data[ConstProperty.Int_Size] = Convert.ToChar(nl.isLeaf);
            // capacity
            FileUtil.ReplaceTheNextFree(data, nl.capacity, ConstProperty.Int_Size + 1);
            // height
            FileUtil.ReplaceTheNextFree(data, nl.capacity, 2 * ConstProperty.Int_Size + 1);

            // keyList
            // TODO Set TK is int
            for (int i = 0; i < nl.capacity; i++)
            {
                FileUtil.ReplaceTheNextFree(data, nl.capacity, (3 + i) * ConstProperty.Int_Size + 1);
            }
            // childRidList
            for (int i = 0; i < nl.capacity; i++)
            {
                FileUtil.ReplaceTheNextFree(data, nl.capacity,
                    (3 + nl.capacity + i) * ConstProperty.Int_Size + 1, ConstProperty.RM_Page_RID_SIZE);
            }
            return data;
        }

        // TODO
        public static IX_FileHdr GetIndexHeader(PF_PageHandle ph)
        {
            return default(IX_FileHdr);
        }
    }
}
