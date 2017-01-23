﻿using Database.Interface;
using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    abstract public class OperationIterator : IOperationIterator
    { 
        protected bool bIterOpen;
        public List<DataAttrInfo> attrs;
        
        protected string indent;
        // ordering attributes
        public bool bSorted;
        public bool desc;
        public string sortRel;
        public string sortAttr;

        public virtual void Open()
        { }
        public virtual void GetNext(DataTuple dataTuple)
        { }
        public virtual void Close()
        { }
        public virtual void Eof()
        { }

        public OperationIterator()
        { }

        public OperationIterator(bool bIterOpen, string indent, bool bSorted, bool desc)
        {
            this.bIterOpen = bIterOpen;
            this.indent = indent;
            this.bSorted = bSorted;
            this.desc = desc;
        }

        public virtual DataTuple GetTuple()
        {
            DataTuple t = new DataTuple(attrs.Count, DataTupleLength());
            t.SetAttr(attrs);
            return t;
        }

        public virtual int DataTupleLength()
        {
            int l = 0;
            for (int i = 0; i < attrs.Count; i++)
                l += attrs[i].attrLength;
            return l;
        }
    }
}
