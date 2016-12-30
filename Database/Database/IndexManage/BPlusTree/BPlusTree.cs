// Copyright 2011 David Galles, University of San Francisco. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are
// permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of
// conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list
// of conditions and the following disclaimer in the documentation and/or other materials
// provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
// ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// The views and conclusions contained in the software and documentation are those of the
// authors and should not be interpreted as representing official policies, either expressed
// or implied, of the University of San Francisco

// Delete function is modified by above.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    /// <summary>
    /// key in the Logic is always not the same
    /// </summary>
    /// <typeparam name="TK"></typeparam>
    /// <typeparam name="TV"></typeparam>
    public class BPlusTree<TK,TV> 
        where TV : INode<TK>
        where TK : IComparable<TK>
    {
        public Node<TK, TV> SeachNode { set; get; }

        public int MaxDegree
        {
            get { return Degree-1; }
        }

        public int MinDegree
        {
            get { return (Degree-1) /2; }
        }

        public int Degree { set; get; }
        public Node<TK, TV> Root{ set; get; }

        private Node<TK, TV> tmpNode;

        public BPlusTree(int degree)
        {
            if (degree < 3) throw new Exception();
            this.Degree = degree;
        }

        public Node<TK, TV> SearchNode { get; set; }

        public void Search(TK key)
        {
            Search(Root, key);
        }

        public Node<TK, TV> SearchInTimes(int times,TK key,List<TV> list)
        {
            if (times < Root.Height) throw new Exception();
            SearchInTimes(times, key, Root,list);
            return SeachNode;
        }

        private void SearchInTimes(int times, TK key, Node<TK, TV> node, List<TV> list)
        {
            if (times != 0)
            {
                int index = GetIndex(key, node);
                SearchInTimes(times - 1, key, node.ChildrenNodes[index],list);
                list.Add(node.ChildrenNodes[index].CurrentRID);
                if (times == 1)
                {
                    SeachNode = node;
                }
            }
        }

        private void Search(Node<TK, TV> node, TK key)
        {
            if (node.IsLeaf == true)
            {
                foreach (var n in node.Values)
                {
                    if (n.CompareTo(key) == 0)
                    {
                        SearchNode = node;
                        return;
                    }
                }
                return;
            }
            else
            {
                var list = node.Values.Where(n => key.CompareTo(n) < 0);

                int index = -1;

                if (list != null && list.ToList().Count != 0)
                {
                    var v = list.First();
                    index = node.Values.IndexOf(v);
                }
                else
                {
                    index = node.Values.Count;
                }
                Search(node.ChildrenNodes[index], key);
            }
        }

        public void Insert(TV value)
        {
            tmpNode = null;
            // Root is null
            if (Root == null)
            {
                // leafNode
                Root = new Node<TK, TV>(true, null, value, 1);
            }
            else
            {
                Insert(value, Root);
            } 
        }

        public void Delete(TK key)
        {
            tmpNode = null;
            if (Root == null) return;

            if (Root.IsLeaf == true && Root.Values.Count == 1)
            {
                if (key.CompareTo(Root.Values[0]) == 0)
                {
                    Root = null;
                    return;
                }
            }

            DoDelete(key, Root);
        }

        /// <summary>
        /// Go through the tree include the nodes and leaves
        /// BFS
        /// </summary>
        public void TraverseForword(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            if (node == null) return;

            action(node);
            if (node.ChildrenNodes != null)
            {
                foreach (var n in node.ChildrenNodes)
                {
                    TraverseForword(n, action);
                }
            } 
        }

        public void TraverseBackword(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            if (node == null) return;

            if (node.ChildrenNodes != null)
            {
                foreach (var n in node.ChildrenNodes)
                {
                    TraverseBackword(n, action);
                }
            }
            action(node);
        }

        public void TraverseOutput(Node<TK, TV> node)
        {
            Console.WriteLine("----------------");
            if (node.Parent != null)
            {
                foreach (var v in node.Parent.Values)
                    Console.Write(v);
                Console.WriteLine();
            }
            Console.WriteLine("node height is " + node.Height);
            if (node.IsLeaf)
            {
                foreach (var p in node.Property)
                {
                    Console.WriteLine("leaf node property is " + p.ToString());
                }
            }
            else
            {
                foreach (var n in node.Values)
                {
                    Console.WriteLine("Branch node is " + n);
                }
            }
            
        }

        public Node<TK, TV> GetParentNode(Node<TK, TV> node)
        {
            if (node.Parent == null) return null;
            return node.Parent;
        }

        public List<Node<TK,TV>> GetChildrenNodes(Node<TK, TV> node)
        {
            if (node.IsLeaf) return null;
            return node.ChildrenNodes;
        }

        /// <summary>
        /// // 反向调用接口方法
        /// </summary>
        /// <param name="isRepairRoot">是否修正根节点</param>
        public void InsertRepair(Func<Node<TK, TV>,TV> nodeExportToDisk)
        {
            if(tmpNode != null)
                InsertRepair(tmpNode, nodeExportToDisk);
        }

        // 反向调用接口方法
        // StealFromLeft，StealFromRight，Merge都需要重新判断向上的子树
        public void RepairAfterDelete(Func<Node<TK, TV>, TV> nodeExportToDisk)
        {
            if (tmpNode != null)
                RepairAfterDelete(tmpNode, nodeExportToDisk);
        }

        /// <summary>
        /// 修正节点
        /// </summary>
        /// <param name="node"></param>
        /// <param name="isRepairRoot">是否修正根节点</param>
        public void InsertRepair(Node<TK, TV> node,
            Func<Node<TK, TV>,TV> funcnInsertToDisk)
        {
            if (node.Values.Count <= MaxDegree)
            {
                return;
            }
            else if (node.Parent == null)
            {
                Root = Split(node, funcnInsertToDisk);
                return;
            }
            else
            {
                var newNode = Split(node, funcnInsertToDisk);
                this.InsertRepair(newNode, 
                    funcnInsertToDisk);
            }
        }

        public void RepairAfterDelete(Node<TK, TV> node, Func<Node<TK, TV>, TV> nodeExportToDisk)
        {
            // less than degree
            if (node.Values.Count < MinDegree)
            {
                if (node.Parent == null)
                {
                    if (node.Values.Count == 0)
                    {
                        Root = node.ChildrenNodes[0];
                        Root.Parent = null;
                    }
                }
                else
                {
                    var parentNode = node.Parent;
                    int parentIndex = 0;
                    for (parentIndex = 0; parentNode.ChildrenNodes[parentIndex] != node; parentIndex++) ;

                    if (parentIndex > 0 && parentNode.ChildrenNodes[parentIndex - 1].Values.Count > MinDegree)
                    {
                        StealFromLeft(node, parentIndex, nodeExportToDisk);

                    }
                    else if (parentIndex < parentNode.Values.Count && parentNode.ChildrenNodes[parentIndex + 1].Values.Count > MinDegree)
                    {
                        StealFromRight(node, parentIndex, nodeExportToDisk);

                    }
                    else if (parentIndex == 0)
                    {
                        // Merge with right sibling
                        var nextNode = Merge(node, nodeExportToDisk);
                        RepairAfterDelete(nextNode.Parent, nodeExportToDisk);
                    }
                    else
                    {
                        // Merge with left sibling
                        var nextNode = Merge(parentNode.ChildrenNodes[parentIndex - 1], nodeExportToDisk);
                        RepairAfterDelete(nextNode.Parent, nodeExportToDisk);
                    }
                }
            }
        }

        private void Insert(TV value, Node<TK, TV> node)
        {
            int index = GetIndex(value, node);
            if (node.IsLeaf == true)
            {
                node.Values.Insert(index, value.Key);
                node.Property.Add(value);
                tmpNode = node;
            }
            else
            {
                Insert(value,node.ChildrenNodes[index]);
            }
        }

        // implete func
        public Node<TK, TV> Split(Node<TK, TV> node, 
            Func<Node<TK, TV>,TV> nodeExportToDisk)
        {
            var rightNode = node.SetNode(node.IsLeaf);
            var risingNode = node.Values[Degree / 2];

            if (node.Parent != null)
            {
                var currentNode = node.Parent;

                int index = currentNode.ChildrenNodes.IndexOf(node);

                currentNode.Values.Insert(index, risingNode);
                currentNode.ChildrenNodes.Insert(index+1, rightNode);
                rightNode.Parent = currentNode;
            }

            int rightSplit;

            if (node.IsLeaf)
            {
                rightSplit = Degree / 2;

                // only the leaf nodes have the property of record RIDs
                rightNode.Property.AddRange(
                    node.Property.GetRange(rightSplit, node.Values.Count - rightSplit));

                node.Property.RemoveRange(rightSplit, node.Values.Count - rightSplit);

                rightNode.Values.AddRange(
                    node.Values.GetRange(rightSplit, node.Values.Count - rightSplit));

                node.Values.RemoveRange(rightSplit, node.Values.Count - rightSplit);
            }
            else
            {
                rightSplit = Degree / 2 + 1;
                rightNode.ChildrenNodes.AddRange(
                    node.ChildrenNodes.GetRange(rightSplit, node.ChildrenNodes.Count - rightSplit));

                node.ChildrenNodes.RemoveRange(rightSplit, node.ChildrenNodes.Count - rightSplit);
                foreach (var r in rightNode.ChildrenNodes)
                {
                    r.Parent = rightNode;
                }

                rightNode.Values.AddRange(
                    node.Values.GetRange(rightSplit, node.Values.Count - rightSplit));

                node.Values.RemoveRange(rightSplit-1, node.Values.Count - rightSplit+1);
            }

            var leftNode = node;

            #region IO handle
            if (nodeExportToDisk != null)
            {
                var left = nodeExportToDisk(leftNode);
                leftNode.CurrentRID = left;
                var right = nodeExportToDisk(rightNode);
                rightNode.CurrentRID = right;
            }
            #endregion

            if (node.Parent != null)
            {
                return node.Parent;
            }
            else
            {
                // branch node
                Root = node.SetNode(false);
                Root.Parent = null;
                Root.Values.Add(risingNode);
                Root.ChildrenNodes.AddRange(new List<Node<TK, TV>>() { leftNode, rightNode });
                leftNode.Parent = Root;
                rightNode.Parent = Root;
                Root.IsLeaf = false;
                Root.Height++;

                #region IO handle
                if (nodeExportToDisk != null)
                {
                    var parent = nodeExportToDisk(Root);
                    Root.CurrentRID = parent;
                }
                #endregion

                return Root;
            }
        }

        private int GetIndex(TK key, Node<TK, TV> node)
        {
            var list = node.Values.Where(n => key.CompareTo(n) < 0);

            int index = -1;

            if (list != null && list.ToList().Count != 0)
            {
                var v = list.First();
                index = node.Values.IndexOf(v);
            }
            else
            {
                index = node.Values.Count;
            }

            return index;
        }

        private int GetIndex(TV value, Node<TK, TV> node)
        {
            return GetIndex(value.Key, node);
        }

        private void DoDelete(TK key, Node<TK, TV> node)
        {
            if (node == null) return;
            int i = 0;
            for (i = 0; i < node.Values.Count && key.CompareTo(node.Values[i]) > 0; i++) ;
            // 满节点
            if (i == node.Values.Count)
            {
                // 非子节点
                if (!node.IsLeaf)
                {
                    DoDelete(key, node.ChildrenNodes[i]);
                }
                else
                { }
            }
            else if (!node.IsLeaf && key.CompareTo(node.Values[i]) == 0)
            {
                this.DoDelete(key, node.ChildrenNodes[i + 1]);
            }
            else if (!node.IsLeaf)
            {
                this.DoDelete(key, node.ChildrenNodes[i]);
            }
            else if (node.IsLeaf && key.CompareTo(node.Values[i]) == 0)
            {
                node.Values.RemoveAt(i);

                // Bit of a hack -- if we remove the smallest element in a leaf, then find the *next* smallest element
                //  (somewhat tricky if the leaf is now empty!), go up our parent stack, and fix index keys
                if (i == 0 && node.Parent != null)
                {
                    TK nextSmallest = default(TK);
                    var parentNode = node.Parent;
                    int parentIndex;
                    for (parentIndex = 0; parentNode.ChildrenNodes[parentIndex] != node; parentIndex++) ;

                    //已经被删掉
                    if (node.Values.Count == 0)
                    {
                        if (parentIndex != parentNode.Values.Count)
                        {
                            // 借一个
                            nextSmallest = parentNode.ChildrenNodes[parentIndex + 1].Values[0];
                        }
                    }
                    else
                    {
                        nextSmallest = node.Values[0];
                    }

                    // 将nextSmallest一层一层的向上替换
                    while (parentNode != null)
                    {
                        if (parentIndex > 0 && key.CompareTo(parentNode.Values[parentIndex - 1]) == 0)
                        {
                            parentNode.Values[parentIndex - 1] = nextSmallest;
                        }
                        var grandParent = parentNode.Parent;
                        for (parentIndex = 0; grandParent != null && grandParent.ChildrenNodes[parentIndex] != parentNode; parentIndex++) ;
                        parentNode = grandParent;
                    }
                }

                tmpNode = node;
            }
        }

        private Node<TK, TV> Merge(Node<TK, TV> node, Func<Node<TK, TV>, TV> nodeExportToDisk)
        {
            var parentNode = node.Parent;
            var parentIndex = 0;
            for (parentIndex = 0; parentNode.ChildrenNodes[parentIndex] != node; parentIndex++) ;
            var rightSib = parentNode.ChildrenNodes[parentIndex + 1];
            var leftSib = parentNode.ChildrenNodes[parentIndex];

            if (!node.IsLeaf)
            {
                node.Values.Add(parentNode.Values[parentIndex]);
                var fromParentIndex = node.Values.Count;
                node.Values.AddRange(rightSib.Values);

                node.ChildrenNodes.AddRange(rightSib.ChildrenNodes);

                foreach (var c in node.ChildrenNodes)
                {
                    c.Parent = node;
                }
            }
            else
            {
                var fromParentIndex = node.Values.Count;
                node.Values.AddRange(rightSib.Values);
            }
            parentNode.ChildrenNodes.RemoveAt(parentIndex+1);
            parentNode.Values.RemoveAt(parentIndex);

            #region IO handle
            if (nodeExportToDisk != null)
            {
                var left = nodeExportToDisk(leftSib);
                leftSib.CurrentRID = left;
                var right = nodeExportToDisk(rightSib);
                rightSib.CurrentRID = right;
            }
            #endregion

            return node;
        }

        private void StealFromRight(Node<TK, TV> node, int parentIndex,Func<Node<TK, TV>, TV> nodeExportToDisk)
        {
            var parentNode = node.Parent;
            var rightSib = parentNode.ChildrenNodes[parentIndex + 1];

            if (node.IsLeaf)
            {
                node.Values.Add(rightSib.Values[0]);
                parentNode.Values[parentIndex] = rightSib.Values[1];
            }
            else
            {
                node.Values.Add(parentNode.Values[parentIndex]);
                parentNode.Values[parentIndex] = rightSib.Values[0];
            }

            if (!node.IsLeaf)
            {
                node.ChildrenNodes.Add(rightSib.ChildrenNodes[0]);
                node.ChildrenNodes.Last().Parent = node;

                rightSib.ChildrenNodes.RemoveAt(0);
            }
            rightSib.Values.RemoveAt(0);

            #region IO handle
            if (nodeExportToDisk != null)
            {
                var right = nodeExportToDisk(rightSib);
                rightSib.CurrentRID = right;
                var left = nodeExportToDisk(node);
                node.CurrentRID = left;
                var parent = nodeExportToDisk(parentNode);
                parentNode.CurrentRID = parent;
            }
            #endregion
        }

        private void StealFromLeft(Node<TK, TV> node, int parentIndex, Func<Node<TK, TV>, TV> nodeExportToDisk)
        {
            var parentNode = node.Parent;

            node.Values.Insert(0, default(TK));

            var leftSib = parentNode.ChildrenNodes[parentIndex - 1];

            if (node.IsLeaf)
            {
                node.Values[0] = leftSib.Values.Last();
            }
            else
            {
                node.Values[0] = parentNode.Values[parentIndex - 1];
            }
            parentNode.Values[parentIndex - 1] = leftSib.Values.Last();

            if (!node.IsLeaf)
            {
                node.ChildrenNodes.RemoveAt(0);
                node.ChildrenNodes.Insert(0, leftSib.ChildrenNodes.Last());

                int index = leftSib.ChildrenNodes.Count;
                leftSib.ChildrenNodes[index] = null;

                node.ChildrenNodes[0].Parent = node;
            }

            leftSib.Values.Remove(leftSib.Values.Last());

            #region IO handle
            if (nodeExportToDisk != null)
            {
                var left = nodeExportToDisk(leftSib);
                leftSib.CurrentRID = left;
                var right = nodeExportToDisk(node);
                node.CurrentRID = right;
                var parent = nodeExportToDisk(parentNode);
                parentNode.CurrentRID = parent;
            }
            #endregion
        }

        public Node<TK, TV> SearchProperLeafNode(TK key,List<Node<TK, TV>> topToLeafStoreList)
        {
            Node<TK, TV> node = null;
            SearchProperLeafNode(key, topToLeafStoreList,Root,ref node);
            return node;
        }

        private void SearchProperLeafNode(TK key, List<Node<TK, TV>> topToLeafStoreList, 
            Node<TK, TV> searchNode, ref Node<TK, TV> selectedNode)
        {
            if (searchNode.IsLeaf != true && searchNode.ChildrenNodes != null && searchNode.ChildrenNodes.Count != 0)
            {
                int index = GetIndex(key, searchNode);
                if (topToLeafStoreList != null)
                {
                    topToLeafStoreList.Add(searchNode);
                }
                SearchProperLeafNode(key, topToLeafStoreList, searchNode.ChildrenNodes[index], ref selectedNode);
            }
            else
            {
                selectedNode = searchNode;
            }
        }
    }
}
