using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
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
        public int Degree { set; get; }
        public Node<TK, TV> Root{ set; get; }

        public BPlusTree(int degree)
        {
            this.Degree = degree;
        }

        public TV SearchNode { get; set; }

        public void Search(TK key)
        {
            Search(Root, key);
        }

        private void Search(Node<TK, TV> node, TK key)
        {
            SearchNode = default(TV);
            if (node.IsLeaf == true)
            {
                foreach (var n in node.Values)
                {
                    if (n.Key.CompareTo(key) == 0)
                    {
                        SearchNode = n;
                        return;
                    }
                }
                return;
            }
            else
            {
                var list = node.Values.Where(n => key.CompareTo(n.Key) < 0);

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
                Root = new Node<TK, TV>(true, null, value);
            }
            else
            {
                Insert(value, Root);
            } 
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="key"></param>
        public void Delete(TK key)
        {
            if (Root.IsLeaf == true && Root.Values.Count == 1)
            {
                if (key.CompareTo(Root.Values[0].Key) == 0)
                {
                    Root = null;
                }
                return;
            }

            DeleteIntenal(key, Root);
        }

        private void DeleteIntenal(TK key, Node<TK, TV> node)
        {
            int i = node.Values.TakeWhile(entry => key.CompareTo(entry.Key) > 0).Count();

            // found key in node, so delete if from it
            if (i < node.Values.Count && key.CompareTo(node.Values[i].Key) == 0)
            {
                this.DeleteKeyFromNode(node, key, i);
                return;
            }

            // delete key from subtree
            if (!node.IsLeaf)
            {
                this.DeleteKeyFromSubtree(node, key, i);
            }
        }

        private void DeleteKeyFromSubtree(Node<TK, TV> parentNode, TK keyToDelete, int subtreeIndexInNode)
        {

        }

        private void DeleteKeyFromNode(Node<TK, TV> node, TK keyToDelete, int keyIndexInNode)
        {

        }

        /// <summary>
        /// Go through the tree include the nodes and leaves
        /// BFS
        /// </summary>
        public void Traverse(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
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
            foreach (var n in node.Values)
            {
                Console.WriteLine("key is " + n);
            }
        }

        private Node<TK, TV> Split(TV value,Node<TK,TV> node)
        {
            var output = node;
            if (node.Values.Count > Degree-1) throw new Exception();

            if (node.IsLeaf == true && node.Values.Count != Degree-1)
            {
                int index = GetIndex(value, node);

                node.Values.Insert(index, value);

                return node;
            }

            if (node.Values.Count == Degree-1)
            {
                int valuesCount = node.Values.Count;

                // split from the mid
                var leftNode = node.SetNode();
                var rightNode = node.SetNode();

                if (node.IsLeaf != true)
                {
                    var leftValues = node.Values.GetRange(0, Degree / 2);
                    leftNode.Values = leftValues;

                    var rightValues = node.Values.GetRange(Degree / 2 + 1, Degree / 2 - 1);
                    rightNode.Values = rightValues;

                    var leftNodes = node.ChildrenNodes.GetRange(0, Degree / 2 + 1);
                    leftNode.ChildrenNodes = leftNodes;

                    var rightNodes = node.ChildrenNodes.GetRange(Degree / 2 + 1, Degree / 2);
                    rightNode.ChildrenNodes = rightNodes;
                }
                else
                {
                    var leftValues = node.Values.GetRange(0, Degree / 2);
                    leftNode.Values = leftValues;

                    var rightValues = node.Values.GetRange(Degree / 2 , valuesCount- Degree / 2);
                    rightNode.Values = rightValues;
                }

                TV midNode = node.Values[(Degree-1) / 2];
                // create a new root node
                if (node.Parent == null)
                {
                    var newRoot = new Node<TK, TV>(false, null, midNode);
                    newRoot.ChildrenNodes = new List<Node<TK, TV>>();
                    newRoot.ChildrenNodes.Add(leftNode);
                    newRoot.ChildrenNodes.Add(rightNode);

                    leftNode.Parent = newRoot;
                    rightNode.Parent = newRoot;

                    Root = newRoot;
                    output = newRoot;
                }
                else
                {
                    var parent = node.Parent;
                    leftNode.Parent = parent;
                    rightNode.Parent = parent;

                    int index = GetIndex(midNode, parent);

                    parent.ChildrenNodes.RemoveAt(index);
                    parent.Values.Insert(index, midNode);
                    var nodeArray = new Node<TK, TV>[] { leftNode, rightNode };

                    parent.ChildrenNodes.InsertRange(index, nodeArray);

                    output = parent;
                }

                foreach (var v in leftNode.ChildrenNodes)
                {
                    v.Parent = leftNode;
                }

                foreach (var v in rightNode.ChildrenNodes)
                {
                    v.Parent = rightNode;
                }
            }

            return output;
        }

        private void Insert(TV value, Node<TK, TV> oldNode)
        {
            var node = Split(value,oldNode);

            int index = GetIndex(value, node);

            if (node.IsLeaf != true)
            {
                Insert(value, node.ChildrenNodes[index]);
            }
        }

        private int GetIndex(TV value, Node<TK, TV> node)
        {
            var list = node.Values.Where(n => value.Key.CompareTo(n.Key) < 0);

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
    }
}
