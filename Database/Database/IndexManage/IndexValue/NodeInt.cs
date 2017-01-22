﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.IndexManage.BPlusTree;

namespace Database.IndexManage.IndexValue
{
    public class NodeStringInt : INode<int>
    {
        private string nn = "TV";

        public NodeStringInt(int key) : base(key)
        {
        }

        public override string ToString()
        {
            return nn + "num is " + base.Key;
        }
    }
}
