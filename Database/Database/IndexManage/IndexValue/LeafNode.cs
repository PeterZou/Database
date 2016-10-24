using Database.Const;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.IndexValue
{
    public class LeafNode<TK>
    {
        int capacity;

        List<TK> keyList;

        RID recordRID;
    }
}
