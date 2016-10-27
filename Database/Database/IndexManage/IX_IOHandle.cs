﻿using Database.IndexManage.BPlusTree;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.IndexManage.IndexValue;
using Database.Const;
using Database.FileManage;

namespace Database.IndexManage
{
    // wrapper the B+tree datastructure within the IO opeartion
    // TK:ConstProperty,TV:RID,ConstProperty
    public class IX_IOHandle<TK>
        where TK : IComparable<TK>
    {
        private RM_FileHandle rmp;

        private Func<string, TK> ConverStringToTK;

        private Func<TK> CreatNewTK;

        public Node<TK, RIDKey<TK>> SelectNode {set;get;}

        public void ForcePages()
        {
            rmp.ForcePages(ConstProperty.ALL_PAGES);
        }

        public IX_IOHandle(RM_FileHandle rmp,Func<string,TK> converStringToTK,Func<TK> creatNewTK)
        {
            this.rmp = rmp;
            this.ConverStringToTK = converStringToTK;
            this.CreatNewTK = creatNewTK;
        }

        /// <summary>
        /// 在内存中展开一棵子树
        /// </summary>
        /// <param name="height"></param>
        /// <param name="rid">当前结点的RID</param>
        /// <param name="parent">父节点</param>
        public void GetSubTreeImportToMemory(int height, RID rid, Node<TK, RIDKey<TK>> parent,bool useBehind, RID leafRid)
        {
            if (height == 0) return;
            
            var node = InsertImportToMemory(rid, parent);

            if (useBehind && node.Item1.CurrentRID.Rid.CompareTo(leafRid) == 0)
            {
                SelectNode = node.Item1;
            }

            foreach (var n in node.Item2)
            {
                GetSubTreeImportToMemory(height - 1, n, node.Item1, useBehind, leafRid);
            }
        }

        public Tuple<Node<TK, RIDKey<TK>>, List<RID>> InsertImportToMemoryRoot(RID rid)
        {
            RM_Record record = rmp.GetRec(rid);

            char[] data = record.GetData();

            // leaf or not
            NodeDisk<TK> nodeDisk = ConvertToNodeDisk(data);

            // set the node link to the parent
            var node = ConvertNodeDiskToNode(nodeDisk, rid);

            return node;
        }

        // root rid located in the first page
        // location of the orignal node may not set in right 
        public Tuple<Node<TK, RIDKey<TK>>, List<RID>> InsertImportToMemory(RID rid, Node<TK, RIDKey<TK>> parent)
        {
            var node = InsertImportToMemoryRoot(rid);
            node.Item1.Parent = parent;

            parent.ChildrenNodes.Add(node.Item1);
            return node;
        }

        // 添加单个node到硬盘
        public void InsertExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            var nodeDisk = ConvertNodeToNodeDisk(node);
            char[] data = ConvertToArray(nodeDisk);
            // set the nl to the disk
            rmp.InsertRec(data);
        }

        // 从硬盘删除单个node
        public void DeleteExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            rmp.DeleteRec(node.CurrentRID.Rid);
        }

        private Tuple<Node<TK, RIDKey<TK>>,List<RID>> ConvertNodeDiskToNode(NodeDisk<TK> nodeDisk, RID rid)
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
                    node.Property.Add(new RIDKey<TK>(nodeDisk.childRidList[i],nodeDisk.keyList[i]));
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
            return new Tuple<Node<TK, RIDKey<TK>>, List<RID>>(node,ridlist);
        }

        private NodeDisk<TK> ConvertNodeToNodeDisk(Node<TK, RIDKey<TK>> node)
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

        private int GetLength(NodeDisk<TK> nl)
        {
            int length = 0;

            length += 3 * ConstProperty.Int_Size+1;

            // TODO Set TK is int
            length += nl.capacity* ConstProperty.Int_Size;
            length += nl.capacity * ConstProperty.RM_Page_RID_SIZE+ ConstProperty.RM_Page_RID_SIZE;
            return length;
        }

        private NodeDisk<TK> ConvertToNodeDisk(char[] data)
        {
            string dataStr = new string(data);
            NodeDisk<TK> node = new NodeDisk<TK>();
            node.length = Convert.ToInt32(dataStr.Substring(0, ConstProperty.Int_Size));
            node.isLeaf = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size, 1));
            node.capacity = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
            node.height = Convert.ToInt32(dataStr.Substring(2*ConstProperty.Int_Size + 1, ConstProperty.Int_Size));

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
                    (3 + node.capacity+i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                int slotNum = Convert.ToInt32(dataStr.Substring(
                    (3 + node.capacity+i+1) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                node.childRidList[i] = new RID(pageNum,slotNum);
            }
            return node;
        }

        private char[] ConvertToArray(NodeDisk<TK> nl)
        {
            char[] data = new char[nl.length];
            // length
            FileManagerUtil.ReplaceTheNextFree(data,nl.length,0);
            // isLeaf
            data[ConstProperty.Int_Size] = Convert.ToChar(nl.isLeaf);
            // capacity
            FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, ConstProperty.Int_Size+1);
            // height
            FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, 2*ConstProperty.Int_Size + 1);

            // keyList
            // TODO Set TK is int
            for (int i = 0; i < nl.capacity; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, (3 + i) *ConstProperty.Int_Size + 1);
            }
            // childRidList
            for (int i = 0; i < nl.capacity; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, 
                    (3 + nl.capacity+i) * ConstProperty.Int_Size + 1,ConstProperty.RM_Page_RID_SIZE);
            }
            return data;
        } 
    }
}
