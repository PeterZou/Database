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
        public int MaxDegree
        {
            get { return Degree-1; }
        }

        public int MinDegree
        {
            get { return Degree/2; }
        }

        public int Degree { set; get; }
        public Node<TK, TV> Root{ set; get; }

        public BPlusTree(int degree)
        {
            this.Degree = degree;
        }

        public TK SearchNode { get; set; }

        public void Search(TK key)
        {
            Search(Root, key);
        }

        private void Search(Node<TK, TV> node, TK key)
        {
            SearchNode = default(TK);
            if (node.IsLeaf == true)
            {
                foreach (var n in node.Values)
                {
                    if (n.CompareTo(key) == 0)
                    {
                        SearchNode = n;
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
            // Root is null
            if (Root == null)
            {
                // leafNode
                Root = new Node<TK, TV>(true, null, value);
            }
            else
            {
                Insert(value, Root);
            } 
        }

        public void Delete(TK key)
        {
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
        public void Traverse(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            if (node == null) return;

            action(node);
            if (node.ChildrenNodes != null)
            {
                foreach (var n in node.ChildrenNodes)
                {
                    Traverse(n, action);
                }
            } 
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

        private void Insert(TV value, Node<TK, TV> node)
        {
            int index = GetIndex(value, node);
            if (node.IsLeaf == true)
            {
                node.Values.Insert(index, value.Key);
                node.Property.Add(value);
                InsertRepair(node);
            }
            else
            {
                Insert(value,node.ChildrenNodes[index]);
            }
        }

        private void InsertRepair(Node<TK, TV> node)
        {
            if (node.Values.Count <= MaxDegree)
            {
                return;
            }
            else if (node.Parent == null)
            {
                Root = Split(node);
                return;
            }
            else
            {
                var newNode = Split(node);
                this.InsertRepair(newNode);
            }
        }

        private Node<TK, TV> Split(Node<TK, TV> node)
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
                return Root;
            }
        }

        private int GetIndex(TK key, Node<TK, TV> node)
        {
            var list = node.Values.Where(n => key.CompareTo(n) < 0);

            int index = -1;

            if (list != null && list.ToList().Count != 0)
            {
                var v = list.Last();
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

                RepairAfterDelete(node);
            }
        }

        private void RepairAfterDelete(Node<TK, TV> node)
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
                        StealFromLeft(node, parentIndex);

                    }
                    else if (parentIndex < parentNode.Values.Count && parentNode.ChildrenNodes[parentIndex + 1].Values.Count > MinDegree)
                    {
                        StealFromRight(node, parentIndex);

                    }
                    else if (parentIndex == 0)
                    {
                        // Merge with right sibling
                        var nextNode = Merge(node);
                        this.RepairAfterDelete(nextNode.Parent);
                    }
                    else
                    {
                        // Merge with left sibling
                        var nextNode = Merge(parentNode.ChildrenNodes[parentIndex - 1]);
                        RepairAfterDelete(nextNode.Parent);
                    }
                }
            }
        }

        private Node<TK, TV> Merge(Node<TK, TV> node)
        {
            var parentNode = node.Parent;
            var parentIndex = 0;
            for (parentIndex = 0; parentNode.ChildrenNodes[parentIndex] != node; parentIndex++) ;
            var rightSib = parentNode.ChildrenNodes[parentIndex + 1];

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

            return node;
        }

        private void StealFromRight(Node<TK, TV> node, int parentIndex)
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
                node.Values.Add(rightSib.Values[parentIndex]);
                parentNode.Values[parentIndex] = rightSib.Values[0];
            }

            if (!node.IsLeaf)
            {
                node.ChildrenNodes.Add(rightSib.ChildrenNodes[0]);
                node.ChildrenNodes.Last().Parent = node;

                rightSib.ChildrenNodes.RemoveAt(0);
            }
            rightSib.Values.RemoveAt(0);
        }

        private void StealFromLeft(Node<TK, TV> node, int parentIndex)
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
        }
    }
}
