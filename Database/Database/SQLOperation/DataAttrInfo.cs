﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SQLOperation
{
    public class DataAttrInfo:IComparable<DataAttrInfo>
    {
        public int offset;                // Offset of attribute
        public Const.ConstProperty.AttrType attrType;              // Type of attribute
        public int attrLength;            // Length of attribute
        public int indexNo;               // Index number of attribute
        public char[] relName;    // Relation name
        public char[] attrName;   // Attribute name
        public Const.ConstProperty.AggFun func;                  // Aggr Function on attr

        public DataAttrInfo()
        {
            relName = new char[Const.ConstProperty.MAXSTRINGLEN];
            attrName = new char[Const.ConstProperty.MAXSTRINGLEN];
            offset = -1;
            func = Const.ConstProperty.AggFun.NO_F;
        }

        public DataAttrInfo(DataAttrInfo d)
        {
            relName = new char[Const.ConstProperty.MAXSTRINGLEN];
            relName.ToList().AddRange(d.relName);
            attrName = new char[Const.ConstProperty.MAXSTRINGLEN];
            attrName.ToList().AddRange(d.attrName);
            offset = d.offset;
            attrType = d.attrType;
            attrLength = d.attrLength;
            indexNo = d.indexNo;
            func = d.func;
        }

        public int CompareTo(DataAttrInfo other)
        {
            throw new NotImplementedException();
        }

        public static int size()
        {
            return 2* Const.ConstProperty.MAXSTRINGLEN + 5 * sizeof(int);
        }
    }
}