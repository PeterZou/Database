using Database.IndexManage.BPlusTree;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.IndexManage.IndexValue;
using Database.Const;
using Database.FileManage;
using Database.Util;

namespace Database.IndexManage
{
    // wrapper the B+tree datastructure within the IO opeartion
    // TK:ConstProperty,TV:RID,ConstProperty
    public class IX_IOHandle<TK>
        where TK : IComparable<TK>
    {
        #region init
        public RM_FileHandle rmp;

        private Func<string, TK> ConverStringToTK;

        private Func<TK> CreatNewTK;
        #endregion

        public IX_IOHandle(RM_FileHandle rmp, Func<string, TK> converStringToTK, Func<TK> creatNewTK)
        {
            this.rmp = rmp;
            this.ConverStringToTK = converStringToTK;
            this.CreatNewTK = creatNewTK;
        }

        public Node<TK, RIDKey<TK>> SelectNode {set;get;}

        public void ForcePages()
        {
            rmp.ForcePages(ConstProperty.ALL_PAGES);
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
            NodeDisk<TK> nodeDisk = IndexUtil<TK>.ConvertToNodeDisk(data, ConverStringToTK);

            // set the node link to the parent
            var node = IndexUtil<TK>.ConvertNodeDiskToNode(nodeDisk, rid, CreatNewTK);

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
            var nodeDisk = IndexUtil<TK>.ConvertNodeToNodeDisk(node);
            char[] data = IndexUtil<TK>.ConvertToArray(nodeDisk);
            // set the nl to the disk
            rmp.InsertRec(data);
        }

        // 从硬盘删除单个node
        public void DeleteExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            rmp.DeleteRec(node.CurrentRID.Rid);
        }

        
    }
}
